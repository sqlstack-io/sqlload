use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SendError, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_array::RecordBatch;
use sqlload::row_batches::main::columns_batch::columns_batch;
use sqlload::row_batches::main::json_rows::json_rows;
use sqlload::row_batches::main::normalise_batch::normalise_batch;

use crate::crossing::errors::CrossError;
use crate::crossing::models::{
    Crossed, CrossingOptions, CrossingStats, Dispatch, PanicSite, ParseJob, Parsed,
};

/// Where one piece's batches go, and the read's counters.
struct Output {
    sender: SyncSender<Parsed>,
    stats: Arc<CrossingStats>,
}

/// Start parser threads and one passing their batches to `sender` in yield order; each channel holds one batch and `max_queued + parse_threads - 2` pieces wait behind the one being passed on, so at most `max_queued + parse_threads` batches are in flight.
pub(crate) fn start_parsers(
    options: CrossingOptions,
    sender: SyncSender<Parsed>,
    stats: &Arc<CrossingStats>,
) -> Dispatch {
    let workers = options.parse_threads.max(1);
    let waiting = (options.max_queued.max(1) + workers).saturating_sub(2);
    let (order_sender, order_receiver) = sync_channel(waiting);
    let (job_sender, job_receiver) = channel::<ParseJob>();
    let jobs = Arc::new(Mutex::new(job_receiver));
    for _ in 0..workers {
        let jobs = jobs.clone();
        let stats = stats.clone();
        let panic_at = options.panic_at;
        std::thread::spawn(move || work(&jobs, &stats, panic_at));
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
fn work(jobs: &Mutex<Receiver<ParseJob>>, stats: &Arc<CrossingStats>, panic_at: Option<PanicSite>) {
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
                let batch = columns_batch(columns).map_err(CrossError::Data)?;
                open = self.send(batch, started);
            }
            Crossed::Json { bytes, read } => {
                let mut started = started;
                json_rows(&bytes, &read, &mut |columns| {
                    let batch = columns_batch(columns)?;
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
                    let batch = normalise_batch(&batch).map_err(CrossError::Data)?;
                    open = self.send(batch, started);
                    if !open {
                        break;
                    }
                    started = Instant::now();
                }
            }
            Crossed::ArrowBatch(batch) => {
                let batch = normalise_batch(&batch).map_err(CrossError::Data)?;
                open = self.send(batch, started);
            }
            Crossed::Failed(error) => return Err(error),
        }
        Ok(open)
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
