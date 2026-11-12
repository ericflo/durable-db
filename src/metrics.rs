//! Fixed-cardinality operational metrics. Labels never contain caller data.
use std::{
    fmt::Write,
    future::Future,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Operation {
    HttpInfo,
    HttpCreate,
    HttpRead,
    HttpCommit,
    HttpGc,
    HttpAccess,
    HttpToken,
    HttpOther,
    AuthorityRead,
    AuthorityReplace,
    ReadBarrier,
    Publication,
    ObjectGet,
    ObjectPut,
    ObjectList,
    PhysicalDelete,
    StorageMaintenance,
    DatabaseMaintenance,
    NativeListBuckets,
    NativeUpdateBucket,
    NativeListVersions,
    NativeDeleteVersion,
    NativeAuthorize,
    NativeOther,
}
const OPERATIONS: [&str; 24] = [
    "http_info",
    "http_create",
    "http_read",
    "http_commit",
    "http_gc",
    "http_access",
    "http_token",
    "http_other",
    "authority_read",
    "authority_replace",
    "read_barrier",
    "publication",
    "object_get",
    "object_put",
    "object_list",
    "physical_delete",
    "storage_maintenance",
    "database_maintenance",
    "native_list_buckets",
    "native_update_bucket",
    "native_list_versions",
    "native_delete_version",
    "native_authorize",
    "native_other",
];
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Outcome {
    Ok,
    Conflict,
    Rejected,
    Unauthorized,
    Invalid,
    Unavailable,
    Error,
    Cancelled,
}
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Event {
    ReadGroups,
    ReadGroupMembers,
    ObservationQueueRejected,
    PublicationRebase,
    CacheHit,
    CacheMiss,
    DataCandidates,
    DataDeleteAck,
    ManifestCandidates,
    ManifestDeleteAck,
    IndexCandidates,
    IndexDeleteAck,
    ProposalSubmitted,
    PublicationGroups,
    PublicationGroupMembers,
    ProposalCandidates,
    ProposalRejected,
    ProposalDeleteAck,
    UploadChargedBytes,
    UploadChargedAttempts,
    UploadGrantBytes,
    UploadGrantAttempts,
    UploadBudgetExhausted,
    ResponseValidationUnavailable,
}
const EVENTS: [&str; 24] = [
    "read_groups",
    "read_group_members",
    "observation_queue_rejected",
    "publication_rebase",
    "cache_hit",
    "cache_miss",
    "data_candidates",
    "data_delete_ack",
    "manifest_candidates",
    "manifest_delete_ack",
    "index_candidates",
    "index_delete_ack",
    "proposal_submitted",
    "publication_groups",
    "publication_group_members",
    "proposal_candidates",
    "proposal_rejected",
    "proposal_delete_ack",
    "upload_charged_bytes",
    "upload_charged_attempts",
    "upload_grant_bytes",
    "upload_grant_attempts",
    "upload_budget_exhausted",
    "response_validation_unavailable",
];
const OUTCOMES: [&str; 8] = [
    "ok",
    "conflict",
    "rejected",
    "unauthorized",
    "invalid",
    "unavailable",
    "error",
    "cancelled",
];
const BOUNDS: [u64; 12] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000,
    15_000_000, 30_000_000,
];

#[derive(Default)]
struct Series {
    outcomes: [AtomicU64; OUTCOMES.len()],
    buckets: [AtomicU64; BOUNDS.len()],
    count: AtomicU64,
    micros: AtomicU64,
    inflight: AtomicU64,
}
pub struct Registry {
    series: [Series; OPERATIONS.len()],
    events: [AtomicU64; EVENTS.len()],
}
impl Default for Registry {
    fn default() -> Self {
        Self {
            series: std::array::from_fn(|_| Series::default()),
            events: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

pub fn http_outcome(status: u16) -> Outcome {
    match status {
        200..=299 => Outcome::Ok,
        401 | 403 => Outcome::Unauthorized,
        409 => Outcome::Conflict,
        429 => Outcome::Rejected,
        400..=499 => Outcome::Invalid,
        503 => Outcome::Unavailable,
        _ => Outcome::Error,
    }
}

impl Registry {
    pub fn event(&self, event: Event, count: usize) {
        self.events[event as usize].fetch_add(count as u64, Ordering::Relaxed);
    }
    pub fn start(&self, operation: Operation) -> Timer<'_> {
        let series = &self.series[operation as usize];
        series.inflight.fetch_add(1, Ordering::Relaxed);
        Timer {
            series,
            started: Instant::now(),
            outcome: Outcome::Cancelled,
        }
    }
    pub fn render(&self) -> String {
        let mut out = String::from(
            "# HELP durable_db_operations_total Completed or cancelled attempts by fixed operation and outcome.\n# TYPE durable_db_operations_total counter\n# HELP durable_db_operation_duration_seconds Attempt latency including errors and cancellation.\n# TYPE durable_db_operation_duration_seconds histogram\n# HELP durable_db_operations_inflight Attempts currently awaiting completion.\n# TYPE durable_db_operations_inflight gauge\n",
        );
        for (operation, series) in OPERATIONS.iter().zip(&self.series) {
            for (outcome, count) in OUTCOMES.iter().zip(&series.outcomes) {
                writeln!(out, "durable_db_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {}", count.load(Ordering::Relaxed)).unwrap();
            }
            for (bound, count) in BOUNDS.iter().zip(&series.buckets) {
                writeln!(out, "durable_db_operation_duration_seconds_bucket{{operation=\"{operation}\",le=\"{}\"}} {}", *bound as f64 / 1_000_000.0, count.load(Ordering::Relaxed)).unwrap();
            }
            let count = series.count.load(Ordering::Relaxed);
            writeln!(out, "durable_db_operation_duration_seconds_bucket{{operation=\"{operation}\",le=\"+Inf\"}} {count}").unwrap();
            writeln!(
                out,
                "durable_db_operation_duration_seconds_count{{operation=\"{operation}\"}} {count}"
            )
            .unwrap();
            writeln!(
                out,
                "durable_db_operation_duration_seconds_sum{{operation=\"{operation}\"}} {}",
                series.micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
            )
            .unwrap();
            writeln!(
                out,
                "durable_db_operations_inflight{{operation=\"{operation}\"}} {}",
                series.inflight.load(Ordering::Relaxed)
            )
            .unwrap();
        }
        out.push_str("# HELP durable_db_events_total Fixed-cardinality protocol, cache, and collection events.\n# TYPE durable_db_events_total counter\n");
        for (event, count) in EVENTS.iter().zip(&self.events) {
            writeln!(
                out,
                "durable_db_events_total{{event=\"{event}\"}} {}",
                count.load(Ordering::Relaxed)
            )
            .unwrap();
        }
        out
    }
}
pub struct Timer<'a> {
    series: &'a Series,
    started: Instant,
    outcome: Outcome,
}
impl Timer<'_> {
    pub fn finish(mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let elapsed = self.started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        for (bound, bucket) in BOUNDS.iter().zip(&self.series.buckets) {
            if elapsed <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.series.micros.fetch_add(elapsed, Ordering::Relaxed);
        self.series.count.fetch_add(1, Ordering::Relaxed);
        self.series.outcomes[self.outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.series.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}
pub async fn measure<T, E>(
    operation: Operation,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let timer = REGISTRY.start(operation);
    let result = future.await;
    timer.finish(if result.is_ok() {
        Outcome::Ok
    } else {
        Outcome::Error
    });
    result
}
pub async fn compare<T, E>(
    operation: Operation,
    future: impl Future<Output = Result<Option<T>, E>>,
) -> Result<Option<T>, E> {
    let timer = REGISTRY.start(operation);
    let result = future.await;
    timer.finish(match &result {
        Ok(Some(_)) => Outcome::Ok,
        Ok(None) => Outcome::Conflict,
        Err(_) => Outcome::Error,
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_and_completion_release_inflight_and_use_fixed_labels() {
        let registry = Registry::default();
        let cancelled = registry.start(Operation::ObjectGet);
        let completed = registry.start(Operation::ObjectGet);
        assert!(
            registry
                .render()
                .contains("durable_db_operations_inflight{operation=\"object_get\"} 2\n")
        );
        completed.finish(Outcome::Ok);
        drop(cancelled);
        let metrics = registry.render();
        assert!(metrics.contains("durable_db_operations_inflight{operation=\"object_get\"} 0\n"));
        assert!(
            metrics.contains(
                "durable_db_operations_total{operation=\"object_get\",outcome=\"ok\"} 1\n"
            )
        );
        assert!(metrics.contains(
            "durable_db_operations_total{operation=\"object_get\",outcome=\"cancelled\"} 1\n"
        ));
        assert!(metrics.contains(
            "durable_db_operation_duration_seconds_bucket{operation=\"object_get\",le=\"+Inf\"} 2\n"
        ));
        assert!(
            metrics.contains(
                "durable_db_operation_duration_seconds_count{operation=\"object_get\"} 2\n"
            )
        );
        assert!(metrics.len() < 64 * 1024);
    }
}
