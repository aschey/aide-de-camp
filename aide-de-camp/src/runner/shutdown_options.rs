use chrono::Duration;

pub struct ShutdownOptions {
    pub job_timeout: Duration,
    pub worker_timeout: Duration,
}

impl Default for ShutdownOptions {
    fn default() -> Self {
        Self {
            job_timeout: Duration::seconds(2),
            worker_timeout: Duration::seconds(3),
        }
    }
}
