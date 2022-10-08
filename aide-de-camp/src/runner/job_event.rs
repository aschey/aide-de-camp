use std::sync::Arc;

use crate::core::job_processor::JobError;

use super::job_info::JobInfo;

#[derive(Clone, Debug)]
pub enum JobEvent {
    Succeeded(JobInfo),
    DeadQueue(JobInfo, Arc<JobError>),
    Failed(JobInfo, Arc<JobError>),
}
