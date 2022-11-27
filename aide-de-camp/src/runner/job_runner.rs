use super::{event_store::EventStore, job_event::JobEvent, job_router::RunnerRouter};
use crate::{
    core::queue::{Queue, QueueError},
    prelude::ShutdownOptions,
};
use anyhow::Context;
use futures::{future::join_all, stream::FuturesUnordered};
use rand::seq::SliceRandom;
use std::{future::Future, sync::Arc};
use tap::TapFallible;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
pub const JITTER_INTERVAL_MS: [i64; 10] = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34];

/// A bridge between job processors and the queue.
///
/// ## Implementation
///
/// This runner implemented very naively:
///
/// - First, it creates a semaphore with permits count equal to desited concurrency
/// - Then, in a loop, for every avaiable permit:
///     - Poll queue with given interval + random jitter
///     - Process incoming job
///     - Give back the permit
///
/// Future Implementation might work differently, but public interface should stay the same.
///
/// ## Examples
/// See `aide-de-camp-sqlite` for examples.
pub struct JobRunner<Q>
where
    Q: Queue,
{
    queue: Arc<Q>,
    processor: Arc<RunnerRouter>,
    semaphore: Arc<Semaphore>,
    event_tx: tokio::sync::broadcast::Sender<JobEvent>,
}

impl<Q> JobRunner<Q>
where
    Q: Queue + 'static,
{
    /// Create a new JobRunner with desired concurrency from queue and router.
    pub fn new(queue: Q, processor: RunnerRouter, concurrency: usize) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(32);
        Self {
            queue: Arc::new(queue),
            processor: Arc::new(processor),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            event_tx,
        }
    }
}

impl<Q> JobRunner<Q>
where
    Q: Queue + 'static,
{
    pub async fn run(&mut self, interval: chrono::Duration) -> Result<(), QueueError> {
        self.run_with_shutdown(
            interval,
            Box::pin(async move {
                std::future::pending::<()>().await;
            }),
            ShutdownOptions::default(),
        )
        .await
    }
    pub async fn run_with_shutdown<F: Future<Output = ()> + Unpin>(
        &mut self,
        interval: chrono::Duration,
        mut shutdown: F,
        options: ShutdownOptions,
    ) -> Result<(), QueueError> {
        let workers = FuturesUnordered::new();
        let cancellation_token = CancellationToken::new();

        loop {
            let semaphore = self.semaphore.clone();
            tokio::select! {
                permit = semaphore.acquire_owned() => {
                    let permit = permit.context("Semaphore closed while running")?;
                    let queue = self.queue.clone();
                    let processor = self.processor.clone();
                    let event_tx = self.event_tx.clone();
                    let cancellation_token = cancellation_token.child_token();

                    let handle = tokio::spawn(async move {
                        let _permit = permit;
                        let queue = queue;
                        let processor = processor;
                        let interval = interval + get_random_jitter();
                        processor.listen(queue, interval, event_tx, cancellation_token, options.job_timeout).await;
                    });
                    workers.push(handle);
                }
                _ = &mut shutdown => {
                    let worker_timeout = options.worker_timeout.to_std()
                        .unwrap_or_else(|e| {
                            tracing::warn!("Error parsing worker timeout, using default: {e:?}");
                            std::time::Duration::default()
                        });
                    cancellation_token.cancel();
                    match tokio::time::timeout(worker_timeout, join_all(workers)).await {
                        Ok(worker_results) => {
                            for result in worker_results {
                                result.tap_err(|e| tracing::error!("Worker panicked: {e:?}")).ok();
                            }
                        }
                        Err(_) => tracing::warn!("Worker failed to shut down within the grace period")
                    }
                    return Ok(());
                }
            }
        }
    }

    pub fn event_store(&self) -> EventStore<JobEvent> {
        EventStore::new(self.event_tx.clone())
    }
}

fn get_random_jitter() -> chrono::Duration {
    JITTER_INTERVAL_MS
        .choose(&mut rand::thread_rng())
        .map(|ms| chrono::Duration::milliseconds(*ms))
        .unwrap_or_else(|| chrono::Duration::milliseconds(5)) // Always takes a happy path technically
}
