use std::time::Duration;

use crate::prelude::Xid;

#[derive(Clone, Debug)]
pub struct JobInfo {
    pub id: Xid,
    pub duration: Duration,
    pub retries: u32,
}
