use super::job_event::JobEvent;

#[derive(Clone)]
pub struct EventStore {
    event_tx: tokio::sync::broadcast::Sender<JobEvent>,
}

impl EventStore {
    pub(crate) fn new(event_tx: tokio::sync::broadcast::Sender<JobEvent>) -> Self {
        Self { event_tx }
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<JobEvent> {
        self.event_tx.subscribe()
    }
}
