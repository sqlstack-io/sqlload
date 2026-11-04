use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, SendError, SyncSender, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow_array::RecordBatch;
use sqlload::row_batches::main::columns_batch::columns_batch;
use sqlload::row_batches::main::json_rows::json_rows;
use sqlload::row_batches::main::normalise_batch::normalise_batch;

use crate::crossing::errors::CrossError;
use crate::crossing::models::{Crossed, CrossingStats};

type Parsed = Result<RecordBatch, CrossError>;

/// Where one piece's batches go, and the read's counters.
struct Output {
    sender: SyncSender<Parsed>,
    stats: Arc<CrossingStats>,
}

/// Parse crossed pieces into Arrow batches on `workers` threads without the GIL, passing batches on in yield order; the first error ends the read.
pub(crate) fn parse(
    receiver: Receiver<Crossed>,
    sender: SyncSender<Parsed>,
    stats: Arc<CrossingStats>,
    workers: usize,
) {
    let queued = workers.max(1) * 2;
    let (order_sender, order_receiver) = sync_channel::<Receiver<Parsed>>(queued);
    let (job_sender, job_receiver) = channel::<(Crossed, SyncSender<Parsed>)>();
    let jobs = Arc::new(Mutex::new(job_receiver));
    for _ in 0..workers.max(1) {
        let jobs = jobs.clone();
        let stats = stats.clone();
        std::thread::spawn(move || work(&jobs, &stats));
    }
    std::thread::spawn(move || collect(order_receiver, sender));
    for crossed in receiver {
        let (piece_sender, piece_receiver) = sync_channel(queued);
        let queued = match order_sender.send(piece_receiver) {
            Ok(()) => job_sender.send((crossed, piece_sender)).map_err(|_| ()),
            Err(SendError(_)) => Err(()),
        };
        if let Err(()) = queued {
            return;
        }
    }
}

/// Parse jobs until the dispatcher stops.
fn work(jobs: &Mutex<Receiver<(Crossed, SyncSender<Parsed>)>>, stats: &Arc<CrossingStats>) {
    loop {
        let job = match jobs.lock() {
            Ok(jobs) => jobs.recv(),
            Err(_) => return,
        };
        let Ok((crossed, sender)) = job else {
            return;
        };
        let output = Output {
            sender,
            stats: stats.clone(),
        };
        match crossed {
            Crossed::Failed(error) => {
                let _ = output.sender.send(Err(error));
            }
            crossed => {
                if let Err(reason) = output.parse_one(crossed) {
                    let _ = output.sender.send(Err(CrossError::Data(reason)));
                }
            }
        }
    }
}

/// Pass each piece's batches on in yield order, stopping after the first error or when the consumer has gone.
fn collect(order: Receiver<Receiver<Parsed>>, sender: SyncSender<Parsed>) {
    for piece in order {
        for parsed in piece {
            match parsed {
                Ok(batch) => {
                    if let Err(SendError(_)) = sender.send(Ok(batch)) {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                    return;
                }
            }
        }
    }
}

impl Output {
    /// Parse one piece into its own channel; `Ok(false)` once the consumer has gone.
    fn parse_one(&self, crossed: Crossed) -> Result<bool, String> {
        let mut open = true;
        let started = Instant::now();
        match crossed {
            Crossed::Columns(columns) => {
                let batch = columns_batch(columns)?;
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
                    true => Err(reason),
                    false => Ok(()),
                })?;
            }
            Crossed::ArrowStream(reader) => {
                let mut started = started;
                for batch in reader {
                    let batch = normalise_batch(&batch.map_err(|error| error.to_string())?)?;
                    open = self.send(batch, started);
                    if !open {
                        break;
                    }
                    started = Instant::now();
                }
            }
            Crossed::ArrowBatch(batch) => {
                let batch = normalise_batch(&batch)?;
                open = self.send(batch, started);
            }
            Crossed::Failed(error) => return Err(error.to_string()),
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
        match self.sender.send(Ok(batch)) {
            Ok(()) => true,
            Err(SendError(_)) => false,
        }
    }
}
