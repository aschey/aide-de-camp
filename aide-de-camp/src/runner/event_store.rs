#[derive(Clone)]
pub struct EventStore<T> {
    event_tx: tokio::sync::broadcast::Sender<T>,
}

impl<T> EventStore<T> {
    pub(crate) fn new(event_tx: tokio::sync::broadcast::Sender<T>) -> Self {
        Self { event_tx }
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<T> {
        self.event_tx.subscribe()
    }
}
