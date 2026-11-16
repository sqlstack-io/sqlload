use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SendError, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_pyarrow::{IntoPyArrow, Table};
use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyCFunction};
use pyo3::{Bound, PyAny, PyResult, Python};
use sqlload::http_source::main::run_http_job::run_http_job;
use sqlload::http_source::models::{HttpJob, HttpPage, PageTransform};
use sqlload::load_naming::main::loaded_name::loaded_name;
use sqlload::python_sources::models::TransformContext;
use sqlload::row_batches::main::columns_batch::columns_batch;
use sqlload::row_batches::main::flatten_batch::flatten_batch;
use sqlload::row_batches::main::json_rows::json_rows;
use sqlload::row_batches::main::normalise_batch::normalise_batch;
use sqlload::row_batches::main::projection_problem::projection_problem;
use sqlload::row_batches::models::{JsonLayout, JsonRead, Projection};

use crate::crossing::constants::PAGE_POLL;
use crate::crossing::errors::CrossError;
use crate::crossing::main::crossing_options::crossing_options;
use crate::crossing::main::table_batches::table_batches;
use crate::crossing::models::{
    Crossed, CrossingOptions, CrossingStats, Dispatch, PanicSite, ParseJob, Parsed, Polled,
    ReadContext, TableBatches,
};
use crate::host::constants::RUNTIME_MODULE;

/// Where one piece's batches go, and the read's counters.
struct Output {
    sender: SyncSender<Parsed>,
    stats: Arc<CrossingStats>,
    /// The objects to flatten and the connections an `sl.http` job may use.
    context: Arc<ReadContext>,
}

/// One page's rows on their way to its job's `transform`, with the call's `ctx` as JSON.
struct PageRows {
    rows: Vec<RecordBatch>,
    schema: SchemaRef,
    context: String,
}

/// A job's `transform` run over all its pages through one crossing: each page's rows go to `pages`, and what the call returns or yields crosses into `batches`, each page's output closed by a state marker; `problem` keeps why a call's return can't load.
struct PageCrossing {
    pages: SyncSender<PageRows>,
    batches: std::cell::RefCell<TableBatches>,
    problem: Arc<Mutex<Option<String>>>,
}

/// Start parser threads and one passing their batches to `sender` in yield order; each channel holds one batch and `max_queued + parse_threads - 2` pieces wait behind the one being passed on, so at most `max_queued + parse_threads` batches are in flight.
pub(crate) fn start_parsers(
    options: CrossingOptions,
    sender: SyncSender<Parsed>,
    stats: &Arc<CrossingStats>,
    context: ReadContext,
) -> Dispatch {
    let flatten = Arc::new(context.flatten.clone());
    let context = Arc::new(context);
    let workers = options.parse_threads.max(1);
    let waiting = (options.max_queued.max(1) + workers).saturating_sub(2);
    let (order_sender, order_receiver) = sync_channel(waiting);
    let (job_sender, job_receiver) = channel::<ParseJob>();
    let jobs = Arc::new(Mutex::new(job_receiver));
    for _ in 0..workers {
        let jobs = jobs.clone();
        let stats = stats.clone();
        let panic_at = options.panic_at;
        let context = context.clone();
        std::thread::spawn(move || work(&jobs, (&stats, &context), panic_at));
    }
    std::thread::spawn(move || {
        let fallback = sender.clone();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| collect(order_receiver, &sender))) {
            let _ = fallback.send(Parsed::Failed(panicked(&payload)));
        }
    });
    Dispatch {
        order: order_sender,
        jobs: job_sender,
        stats: stats.clone(),
        flatten,
    }
}

impl Dispatch {
    /// Queue `crossed` (with `ctx.state` as it crossed, for logged tables) for parsing in yield order, waiting while the queue is full; `false` once the consumer has gone. Call it without the GIL.
    pub(crate) fn send(&self, crossed: Crossed, state: Option<String>) -> bool {
        self.stats.budget.acquire(crossed.queued_bytes());
        let (piece_sender, piece_receiver) = sync_channel(0);
        match self.order.send(Some((piece_receiver, state))) {
            Ok(()) => match self.jobs.send(ParseJob {
                crossed,
                sender: piece_sender,
            }) {
                Ok(()) => true,
                Err(SendError(_)) => false,
            },
            Err(SendError(_)) => false,
        }
    }

    /// Mark the end of the generator, after its last piece.
    pub(crate) fn finish(&self) {
        let _ = self.order.send(None);
    }
}

/// The error for a crossing thread that panicked.
pub(crate) fn panicked(payload: &Box<dyn Any + Send>) -> CrossError {
    let reason = payload
        .downcast_ref::<&str>()
        .map(|reason| (*reason).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown cause".to_owned());
    CrossError::Data(format!("a crossing thread panicked: {reason}"))
}

/// Panic when the read was asked to panic at `site` (the crossing's test hook).
pub(crate) fn panic_if(panic_at: Option<PanicSite>, site: PanicSite) {
    if panic_at == Some(site) {
        panic!("injected {} panic", format!("{site:?}").to_lowercase());
    }
}

/// Parse jobs until the dispatcher stops; a panic fails the piece, not the thread.
fn work(
    jobs: &Mutex<Receiver<ParseJob>>,
    (stats, context): (&Arc<CrossingStats>, &Arc<ReadContext>),
    panic_at: Option<PanicSite>,
) {
    loop {
        let job = match jobs.lock() {
            Ok(jobs) => jobs.recv(),
            Err(_) => return,
        };
        let Ok(ParseJob { crossed, sender }) = job else {
            return;
        };
        let output = Output {
            sender,
            stats: stats.clone(),
            context: context.clone(),
        };
        let bytes = crossed.queued_bytes();
        let parsed = catch_unwind(AssertUnwindSafe(|| {
            panic_if(panic_at, PanicSite::Parser);
            output.parse_one(crossed)
        }));
        stats.budget.release(bytes);
        let last = match parsed {
            Ok(Ok(true)) => Parsed::Done,
            Ok(Ok(false)) => continue,
            Ok(Err(error)) => Parsed::Failed(error),
            Err(payload) => Parsed::Failed(panicked(&payload)),
        };
        let _ = output.sender.send(last);
    }
}

/// Pass each piece's batches on in yield order, then the `ctx.state` it crossed with, stopping after the first error, at the end marker, or when the consumer has gone.
fn collect(
    order: Receiver<Option<(Receiver<Parsed>, Option<String>)>>,
    sender: &SyncSender<Parsed>,
) {
    for piece in order {
        let Some((piece, state)) = piece else {
            let _ = sender.send(Parsed::Done);
            return;
        };
        loop {
            match piece.recv() {
                Ok(Parsed::Batch(batch)) => {
                    if let Err(SendError(_)) = sender.send(Parsed::Batch(batch)) {
                        return;
                    }
                }
                Ok(Parsed::Done) => {
                    if let Some(state) = state
                        && let Err(SendError(_)) = sender.send(Parsed::State(state))
                    {
                        return;
                    }
                    break;
                }
                Ok(Parsed::State(_)) => {}
                Ok(failed @ Parsed::Failed(_)) => {
                    let _ = sender.send(failed);
                    return;
                }
                Err(_) => {
                    let _ = sender.send(Parsed::Failed(stopped("a parser thread")));
                    return;
                }
            }
        }
    }
    let _ = sender.send(Parsed::Failed(stopped("the generator's reader")));
}

/// `projection` with `zone`, the table's `timezone`, for its parses without their own `tz`.
fn zoned(projection: Option<Projection>, zone: Option<chrono_tz::Tz>) -> Option<Projection> {
    projection.map(|projection| Projection { zone, ..projection })
}

fn stopped(thread: &str) -> CrossError {
    CrossError::Data(format!("{thread} stopped before finishing its data"))
}

impl Output {
    /// Parse one piece into its own channel; `Ok(false)` once the consumer has gone.
    fn parse_one(&self, crossed: Crossed) -> Result<bool, CrossError> {
        let mut open = true;
        let started = Instant::now();
        match crossed {
            Crossed::Columns(columns) => {
                let batch = columns_batch(columns, self.context.flatten.as_text)
                    .map_err(CrossError::Data)?;
                open = self.send(batch, started);
            }
            Crossed::Json { bytes, read } => {
                let read = JsonRead {
                    projection: zoned(read.projection, self.context.zone),
                    ..read
                };
                if let Some(problem) = read
                    .projection
                    .as_ref()
                    .and_then(|projection| projection_problem(projection, self.context.rules()))
                {
                    return Err(CrossError::Data(problem));
                }
                let mut started = started;
                json_rows(&bytes, &read, &mut |columns| {
                    let batch = flatten_batch(
                        &columns_batch(columns, self.context.flatten.as_text)?,
                        &self.context.flatten,
                    )?;
                    open = open && self.send(batch, started);
                    started = Instant::now();
                    match open {
                        true => Ok(()),
                        false => Err("the consumer stopped reading".to_owned()),
                    }
                })
                .or_else(|reason| match open {
                    true => Err(CrossError::Data(reason)),
                    false => Ok(()),
                })?;
            }
            Crossed::ArrowStream(reader) => {
                let mut started = started;
                for batch in reader {
                    let batch = batch.map_err(|error| CrossError::Data(error.to_string()))?;
                    let batch = self.arrow(&batch)?;
                    open = self.send(batch, started);
                    if !open {
                        break;
                    }
                    started = Instant::now();
                }
            }
            Crossed::ArrowBatch(batch) => {
                let batch = self.arrow(&batch)?;
                open = self.send(batch, started);
            }
            Crossed::Http(definition) => open = self.http(&definition)?,
            Crossed::Checkpoint => {}
            Crossed::Failed(error) => return Err(error),
        }
        Ok(open)
    }

    /// A yielded Arrow batch with the struct columns its table flattens flattened, then in the loader's types.
    fn arrow(&self, batch: &RecordBatch) -> Result<RecordBatch, CrossError> {
        let flattened = flatten_batch(batch, &self.context.flatten).map_err(CrossError::Data)?;
        let (naming, limit) = self.context.naming;
        let arrays: Vec<String> = flattened
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| {
                self.context
                    .arrays
                    .contains(&loaded_name(naming, name, limit))
            })
            .collect();
        normalise_batch(&flattened, &arrays).map_err(CrossError::Data)
    }

    /// Run an `sl.http` job, parsing each page into batches as it arrives, through the job's `transform` when it has one; `Ok(false)` once the consumer has gone, which cancels the requests in flight.
    fn http(&self, definition: &str) -> Result<bool, CrossError> {
        let transform = match serde_json::from_str::<HttpJob>(definition) {
            Ok(job) => job.transform().cloned(),
            Err(error) => {
                return Err(CrossError::Http(
                    format!("cannot read the sl.http job: {error}").into(),
                ));
            }
        };
        let crossing = match &transform {
            Some(transform) => Some(self.page_crossing(transform).map_err(CrossError::Python)?),
            None => None,
        };
        let mut open = true;
        let mut failed: Option<String> = None;
        run_http_job(
            definition,
            &self.context.connections,
            self.context.rules(),
            |page| {
                match self.page_rows(&page, transform.as_ref().zip(crossing.as_ref())) {
                    Ok(still) => open = still,
                    Err(reason) => failed = Some(format!("{}: {reason}", page.source)),
                }
                open && failed.is_none()
            },
        )
        .map_err(CrossError::Http)?;
        match (failed, crossing) {
            (Some(reason), _) => Err(CrossError::Data(reason)),
            (None, Some(crossing)) if open => self.finish_pages(crossing).map_err(CrossError::Data),
            (None, _) => Ok(open),
        }
    }

    /// Start the crossing `transform` runs in over a job's pages: its function called on each page's rows as they arrive (see [`PageRows`]), what it returns crossing as a table function's yields cross, with `sl.checkpoint(None)` after each page's output to mark where it ends.
    fn page_crossing(&self, transform: &PageTransform) -> PyResult<PageCrossing> {
        let (pages, received) = sync_channel::<PageRows>(0);
        let received = Mutex::new(received);
        let problem: Arc<Mutex<Option<String>>> = Arc::default();
        let noted = problem.clone();
        let function = transform.function.clone();
        let context = ReadContext {
            flatten: self.context.flatten.clone(),
            masked: self.context.masked.clone(),
            arrays: self.context.arrays.clone(),
            naming: self.context.naming,
            ..ReadContext::default()
        };
        let options = CrossingOptions {
            parse_threads: 1,
            ..crossing_options()
        };
        let batches = Python::attach(|py| -> PyResult<TableBatches> {
            let feed = PyCFunction::new_closure(py, None, None, move |args, _| {
                let py = args.py();
                let page = py.detach(|| match received.lock() {
                    Ok(receiver) => match receiver.recv() {
                        Ok(page) => Some(page),
                        Err(_) => None,
                    },
                    Err(_) => None,
                });
                let Some(page) = page else {
                    return Ok(None);
                };
                let table = Table::try_new(page.rows, page.schema)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?
                    .into_pyarrow(py)?;
                let (returned, problem): (Option<Bound<'_, PyAny>>, Option<String>) = py
                    .import(RUNTIME_MODULE)?
                    .call_method1("call_transform", (function.as_str(), table, page.context))?
                    .extract()?;
                match returned {
                    Some(returned) => PyResult::Ok(Some(returned.unbind())),
                    None => {
                        let problem =
                            problem.unwrap_or_else(|| "it returned nothing that loads".to_owned());
                        if let Ok(mut slot) = noted.lock() {
                            *slot = Some(problem.clone());
                        }
                        Err(PyValueError::new_err(problem))
                    }
                }
            })?;
            let items = py
                .import(RUNTIME_MODULE)?
                .call_method1("page_items", (feed,))?;
            let snapshot =
                PyCFunction::new_closure(py, None, None, |_, _| PyResult::Ok(None::<String>))?;
            Ok(table_batches(
                &items,
                Some(snapshot.as_any()),
                options,
                context,
            ))
        })?;
        Ok(PageCrossing {
            pages,
            batches: std::cell::RefCell::new(batches),
            problem,
        })
    }

    /// End `crossing` once its job has no more pages, passing on anything it still holds; `Ok(false)` once the consumer has gone.
    fn finish_pages(&self, crossing: PageCrossing) -> Result<bool, String> {
        let PageCrossing { pages, batches, .. } = crossing;
        drop(pages);
        let mut batches = batches.into_inner();
        loop {
            match batches.poll(PAGE_POLL) {
                Polled::Batch(batch) => {
                    if !self.send(batch, Instant::now()) {
                        return Ok(false);
                    }
                }
                Polled::Finished => return Ok(true),
                Polled::Failed(error) => return Err(error.to_string()),
                Polled::State(_) | Polled::Pending => {}
            }
        }
    }

    /// Parse `page` into batches and pass them on, or, given a `transform`, pass on what it returns or yields for the page's rows; `Ok(false)` once the consumer has gone.
    fn page_rows(
        &self,
        page: &HttpPage,
        transform: Option<(&PageTransform, &PageCrossing)>,
    ) -> Result<bool, String> {
        let read = JsonRead {
            layout: JsonLayout::Document,
            path: page.path.clone(),
            batch_rows: usize::MAX,
            projection: zoned(page.projection.clone(), self.context.zone),
        };
        let mut open = true;
        let mut held: Vec<RecordBatch> = Vec::new();
        let parsed = json_rows(&page.bytes, &read, &mut |columns| {
            let batch = columns_batch(columns, self.context.flatten.as_text)?;
            if transform.is_some() {
                held.push(batch);
                return Ok(());
            }
            let batch = flatten_batch(&batch, &self.context.flatten)?;
            open = open && self.send(batch, Instant::now());
            match open {
                true => Ok(()),
                false => Err("the consumer stopped reading".to_owned()),
            }
        });
        match (parsed, open, transform) {
            (Err(reason), true, _) => Err(reason),
            (_, false, _) | (Ok(()), true, None) => Ok(open),
            (Ok(()), true, Some((transform, crossing))) => self
                .transformed_page((transform, page), crossing, held)
                .map_err(|reason| format!("transform {} failed: {reason}", transform.name)),
        }
    }

    /// Call `transform` once on one page's `rows` through `crossing`, with the page's number and fan-out key as its `ctx`, and pass on what it returns or yields as it crosses, until the page's end marker; `Ok(false)` once the consumer has gone.
    fn transformed_page(
        &self,
        (transform, page): (&PageTransform, &HttpPage),
        crossing: &PageCrossing,
        rows: Vec<RecordBatch>,
    ) -> Result<bool, String> {
        let schema = rows
            .first()
            .map_or_else(|| Arc::new(Schema::empty()), RecordBatch::schema);
        let context = TransformContext {
            unit: "page",
            table: self.context.table.clone(),
            page: Some(page.number),
            key: (!page.key.is_empty()).then(|| page.key.clone()),
            ..TransformContext::default()
        };
        let context = serde_json::to_string(&context).map_err(|error| error.to_string())?;
        let sent = crossing.pages.send(PageRows {
            rows,
            schema,
            context,
        });
        if sent.is_err() {
            return Err(format!("{} stopped before this page", transform.name));
        }
        let mut batches = crossing.batches.borrow_mut();
        loop {
            match batches.poll(PAGE_POLL) {
                Polled::Batch(batch) => {
                    if !self.send(batch, Instant::now()) {
                        return Ok(false);
                    }
                }
                Polled::State(_) => return Ok(true),
                Polled::Failed(error) => {
                    let problem = match crossing.problem.lock() {
                        Ok(slot) => slot.clone(),
                        Err(_) => None,
                    };
                    return Err(problem.unwrap_or_else(|| error.to_string()));
                }
                Polled::Finished => {
                    return Err(format!("{} stopped before this page ended", transform.name));
                }
                Polled::Pending => {}
            }
        }
    }

    /// Count `batch`, parsed since `started`, and pass it on; `false` once the consumer has gone.
    fn send(&self, batch: RecordBatch, started: Instant) -> bool {
        CrossingStats::add(&self.stats.parse_nanos, started.elapsed());
        self.stats
            .rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.stats.batches.fetch_add(1, Ordering::Relaxed);
        match self.sender.send(Parsed::Batch(batch)) {
            Ok(()) => true,
            Err(SendError(_)) => false,
        }
    }
}
