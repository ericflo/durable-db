# Deployment and operations

The service is experimental. Native cloud serving is integrated; full Kubernetes fault recovery and performance remain under qualification. The [verification results](verification-results.md) record passing phases, failed deadlines and exact configurations.

The [release](https://github.com/ericflo/durable-db/releases) provides a static Linux x86_64 binary. While the repository is private, `gh` must be authenticated with repository access. Install without root or a Rust toolchain:

```sh
curl -fsSL https://ericflo.github.io/durable-db-site/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
durable-db serve --data ./data
```

For laptop use, run `durable-db serve --data ./data`. This selects native serving with automatic checkpoints and collection. The process exclusively locks its store directory, including while stopped or suspended; no physical ownership clock is required. Stop it before opening the same directory elsewhere. Local mode must not be shared across pods, placed on a network filesystem, or cloned from live process memory. Older directories require [export and restore](recovery.md#local-format-upgrades).

For S3, every pod receives the same bucket, prefix, and administrator signing key file. Supply AWS credentials through the provider's environment or workload identity. For a single process, run `durable-db serve --bucket BUCKET --admin-key-file /run/secrets/admin.key --upload-budget-bytes 1073741824 --upload-budget-attempts 100000`. This listens on loopback. Multiple processes require `--discovery-service`; wildcard listeners also require `--advertise-ip`. The Kubernetes base supplies both. A startup probe rejects providers that fail conditional object writes. B2 uses the same publication implementation through its native revision binding; see [B2 configuration](b2.md).

Use `/health/live` for liveness and `/health/ready` for readiness. On SIGTERM the server closes admission, drops readiness and signals graceful HTTP shutdown without waiting for provider I/O. HTTP acceptance and idle connections close while admitted requests and routing withdrawal drain concurrently. Withdrawal retains its five-second bound; process exit waits for both drains. The cluster retires every local resident before starting storage I/O; up to four conditional withdrawals run concurrently. In-flight requests may retain their backend until completion. A confirmed withdrawal records the departing stable member: its replacement skips the 15-second challenge wait, while another member waits before fallback. A replacement that still observes the address uses the ordinary challenge path, including after a crash or failed hint. The successor still revokes old permits and recovers journals. Shutdown hooks are never a correctness dependency. Completed cloud writes survive pod removal because their authority is object storage; clients retry ambiguous writes with identical request bodies and IDs. Local readiness checks its worker and recovered ownership. Cloud readiness exposes routing and administration once this process appears in valid discovery membership, even while partitions recover or upload credit is exhausted. Unavailable partitions return retryable errors; healthy partitions remain reachable. Data responses still require recovered ownership and fresh permit validation. Readiness is not a guarantee of continuous provider reachability.

Set pod CPU/memory limits and concurrency/cache limits together. Memory cache bytes are a payload budget, not a process RSS ceiling. Bound ingress body size, authenticate at the service, terminate TLS at trusted ingress, and retain provider credentials only on the service side. See [security](security.md) for the remaining cluster-wide abuse protection work.

Provider HTTP requests do not follow redirects. Configure the final regional S3 endpoint directly. The shared S3 connector preserves the SDK's environment-configured timeouts, connection pooling, HTTP version, randomized DNS, proxy/exclusions/proxy CA, content type, and certificate validation settings. It uses system trust roots; `disable_system_certificates` fails startup because this CLI has no replacement-root configuration. SDK-only programmatic custom resolvers, root certificates, and default headers are not exposed. AWS credential providers and server-side encryption configuration remain handled by the SDK. Immutable uploads have one attempt per invocation; reads retain bounded SDK retries.

Each native owner performs bounded scheduled maintenance. Collection uses a durable publication fence; it does not depend on pod leases or reliable termination hooks. Failed deletion is retried in later sweeps. Both providers delete obsolete physical file versions. S3 also collects superseded versions of its directory control object. External retention must not prevent reclamation or delete live files.

## Kubernetes manifests

`deploy/kubernetes/` is a Kustomize base with a StatefulSet, client Service, headless discovery Service, HPA (1–8 replicas), and disruption budget. It uses a read-only root filesystem, unprivileged UID, bounded disposable disk cache, startup/readiness/liveness probes, and graceful drain. CPU-based autoscaling requires Metrics Server; provider latency can limit throughput even when CPU is low. Calibrate scaling against real workload measurements.

Each pod advertises its stable name through `DURABLE_DB_MEMBER_ID`; placement
hashes this identity, while routing uses its current IP. [StatefulSet pod identities](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-identity)
survive replacement. Scale operations run in parallel; rolling updates replace one
pod at a time. There are no PVCs: the bucket remains authoritative and `emptyDir`
caches remain disposable. Replacement still needs fenced recovery, and temporary
membership loss can still move partitions.

Outside Kubernetes, set `--member-id` to a stable unique name per logical process.
The existing `/health/live` response advertises `x-durable-db-member`; IDs are
bounded to 253 ASCII characters. Missing headers fall back to address identity for
older peers. Duplicate IDs exclude all ambiguous destinations while retaining
unrelated members. These unauthenticated discovery hints belong on a trusted peer
network and never grant permission to read or write data. Mixed-version placement
is transitional and has not been qualified for continuous availability.

Existing Deployment installations need a controller migration: scale the old
Deployment to zero and wait for its pods to terminate, remove that Deployment,
then apply the StatefulSet overlay. Preserve the namespace, Services, Secrets and
bucket. This procedure has a cold-recovery interruption; applying the new base
alone does not delete an existing Deployment.

Build/pull the runtime image, then set its immutable digest in an overlay. The base's `durable-db:local` is a deliberate image placeholder, not a published registry claim. Woodpecker publishes each successful commit's image in the configured private registry.

Create namespace `durable-db`, a `durable-db-signing` Secret containing `admin.key`, and a `durable-db-storage` Secret containing either `DURABLE_DB_B2_BUCKET`, `B2_APPLICATION_KEY_ID`, `B2_APPLICATION_KEY`, or `DURABLE_DB_BUCKET` and the S3 provider settings. Include `DURABLE_DB_UPLOAD_BUDGET_BYTES` and `DURABLE_DB_UPLOAD_BUDGET_ATTEMPTS` for initial shared ceilings. Existing counters/ceilings are preserved on reopen; refill them through the administrator API. Keep secrets out of Git. When changing provider type in an existing Secret, replace its complete `data` map and remove the old provider fields; applying new `stringData` alone may leave old environment variables behind. Startup refuses simultaneous B2 and S3 selection. Never mount a shared local store directory across pods. Apply the configured overlay with `kubectl apply -k`.

The headless service publishes not-ready addresses so discovery does not depend on acquisition completing first. Discovery admits candidates only after an HTTP 200 from `/health/live`, and excludes an established peer after two missed observations even if DNS still lists it. Probes run independently of DNS refresh, with sixteen requests at most and a one-second request deadline. Three-second rounds do not overlap; large peer lists can take longer. Permit fencing, not this failure suspicion, authorizes takeover. Allow peer HTTP traffic on the advertised port. All pods must use the same peer port, IP family and fixed layout. Defaults are 16 partitions and a 128 MiB aggregate resident payload allowance (`--partitions`, `--resident-memory-mib`). The partition count does not cap database count and does not change when pods scale. Too-small per-partition shares fail startup. Frame/window/checkpoint thresholds remain per partition; the cloud receipt window is 128 commits. Journal bytes retain a 256 MiB aggregate component ceiling. Cache, transient checkpoint/HTTP/provider buffers and allocator overhead are separate from resident payload accounting; qualify peak RSS against pod limits.

Before taking over from an excluded incumbent, the candidate publishes or joins a
storage-path challenge and waits 15 seconds for an answer. The incumbent answers
through the bucket even when peer HTTP is blocked. Pending waits release acquisition
slots; the three-second scheduler revisits them while other partitions recover.
A response defers takeover; a timeout only schedules the existing fenced recovery. Slow storage or pauses
can still cause false suspicions. This adds failure-detection latency and does
not make every partition reachable through every isolated pod. Upgrade all
servers before relying on this behavior; older binaries do not answer challenges.

A preference change keeps the current healthy backend serving until actual
revocation; leaving membership still removes it. While yielding to another
preferred process, the incumbent stops answering challenges so asymmetric peer
reachability cannot prevent that successor from taking over. The existing
resident budget covers this overlap. Once a claim is confirmed, recovery also
continues while the process remains a member, even if preference changes before
activation finishes. A later takeover still fences it. Candidates without a
confirmed claim continue to yield immediately. No extra settling timer is used.

Graceful shutdown leaves the departing stable member's fingerprint in the existing
owner-bound hint. A replacement of that member skips the challenge wait; another
member waits the same 15 seconds before falling back. This avoids a temporary
claim when the original member returns promptly. Scale-down still permits takeover,
and a late return can still interrupt recovery. Failed shutdown hints retain the
ordinary recovery path. The hint grants no authority: every successor must claim,
revoke the old permit and recover. Older binaries preserve this field but do not
honor the member preference; mixed-version convergence remains unqualified.

`scripts/qualify-multinode.py` is the maintained native deployment/fault campaign.
Use `--scenario rollout` for two consecutive replacements after 1→3→6 scaling,
followed by zero-compute cold recovery; `recovery` retains the broader fault campaign.
All scenarios use the same writers, budget checks and independent history verification.
The harness uses a stable client Service through scaling and replacement and requires two
new verified readbacks from every writer before each fault. Fixed writer names
map to partitions 2, 12, 14 and 2, preserving shared-partition contention across
runs. The provider-egress fault allows peer forwarding and verifies a ten-second
workload blackout after thirty seconds of settling; a new-connection probe alone
cannot establish that existing connections were blocked.

The host-pause phase stops every database process on the preferred host of an
active writer, retains its DNS records and requires recovery without adding pods.
The all-process pause then requires a sole new process to recover all writers.
Both verify the stopped identities before resume. The same-UID control sidecar,
shared PID namespace and disabled liveness restart apply only to the disposable
fixture. This simulates process loss on a host, not physical power loss or a
network partition. [Current measurements and historical failures](verification-results.md)
remain separate from the declared recovery targets.

For the focused network experiment, add `--scenario peer-isolation` to the same
qualification command. It starts three processes on three hosts, sends writers
through stable services on both sides of a one-versus-two split, and exercises
one-way then two-way peer isolation. HTTPS/DNS and workload ingress remain open.
A pinned test helper uses `NET_ADMIN` as root inside each disposable pod’s private
network namespace, dropping only traffic to peer addresses on TCP port 7654.
Host networking is refused. Long-lived observers check fresh and retained peer
connections; every process
must also complete a fresh provider-backed administrative read. Each split is
held for thirty seconds after its network checks pass. Per-writer progress during
the split is recorded separately from recovery after healing. Both scenarios
share cold replacement, full history verification and namespace cleanup. The
harness records bounded claim-attempt samples during each hold independently of
writer progress. [Current results and missed targets](verification-results.md)
retain exact binaries, configurations and timings. These short runs qualify their
bounded fault/recovery histories, not permanent-partition availability or a
production SLO. Rolling and scale-down still need broader qualification.
The initial NetworkPolicy attempt
failed injection validation because established connections remained usable;
[that behavior is implementation-defined](https://kubernetes.io/docs/concepts/services-networking/network-policies/#networkpolicy-s-impact-on-existing-connections).


Use a registry address reachable by the **nodes' container runtime**, not just by pods. For example, a build may push through a cluster DNS service while the nodes pull the same registry through a configured NodePort alias. The qualification run used this arrangement; it did not modify cluster-wide registry or metrics settings.


## Request budgets

`--concurrency` bounds admitted tenant HTTP requests. Two additional slots are
reserved for authenticated budget, access-policy, token and metrics operations; these requests
retain their per-principal rate/concurrency limits. Budget for at most
`concurrency + 2` admitted HTTP requests per process. Tenant traffic cannot use
the control reserve, even with a control-like path or database name.

v0.19.0 exposes two per-principal, per-pod overload settings:

| Flag | Environment variable | Default | Accepted range |
| --- | --- | --- | --- |
| `--request-units-per-second` | `DURABLE_DB_REQUEST_UNITS_PER_SECOND` | 20 | 1–1,000,000 |
| `--request-burst-units` | `DURABLE_DB_REQUEST_BURST_UNITS` | 40 | 5–1,000,000 |

A commit attempt costs five units; other authenticated requests cost one. A new principal starts with the burst allowance, which refills continuously up to that ceiling. With defaults, an otherwise idle principal can attempt eight commits immediately and sustain at most four per second. Mixed requests share the same budget. Raising concurrency alone does not raise this ceiling.

All tokens for one database share its budget. All administrator credentials share one administrator budget across databases; metrics-only credentials have their own budget. Applications should use database-scoped credentials. Rate or concurrency rejection returns HTTP 429 before the handler runs; clients should back off. Invalid configuration fails before storage initialization.

Choose these settings alongside concurrency, memory and measured provider capacity, and keep them consistent across the pod template. They are policy limits, not measured database throughput. Scaling and process restarts create additional initial allowances; these transient limits do not replace durable quotas or cluster-wide spending controls. See [security](security.md).

## Signing-key changes

v0.19.0 reads either the original hex key file or a bounded JSON keyring through the same `--admin-key-file` setting. Follow the [expansion, promotion, and retirement procedure](design-signing-rotation.md). Key files are loaded at startup: update the Secret and complete the corresponding rolling restart, including the template used by new autoscaled pods. A still-serving process outside the current control-plane view must be fenced from clients or included in the completion check. Keep the old key trusted during an availability rollback; a compromised or deliberately retired key must not be reintroduced by restoring an old Secret.

For isolated qualification on an existing cluster, `scripts/qualify-multinode.py` creates a fresh namespace with explicit CPU/memory/pod quotas, private fixture Secrets, an authenticated control channel, and only namespace-scoped faults. It never drains or reboots nodes. Supply a dedicated disposable provider configuration and an image reference supported by the nodes' container runtime, pinned by digest; a registry address usable inside pods may not be a valid host pull address. The script deletes its own namespace on exit; its caller must remove the disposable provider bucket and key. `scripts/check-autoscaling-history.py` checks its resulting append history.

The qualifier optionally holds six replicas at its four-writer workload for `--steady-seconds 3600` before disruption. This pilot is bounded to one hour and 100,000 write intents, with unchanged 1 GiB/100,000-attempt upload ceilings. `--observe-memory` samples RSS, working set, cumulative CPU, pod identity, and restart counters from the kubelet statistics API; it requires the operator's existing node-proxy read permission and retains only the new namespace's API-container counters. An unplanned restart/replacement fails that steady memory phase instead of hiding growth behind a reset. Lightweight control polling excludes the append history; full histories are retrieved for final or failed-run evidence. This pilot does not substitute for the required 72-hour declared-load campaign.

Inspect and replenish `/v1/storage/upload-budget` through the normal authenticated client Service, including when upload credit is exhausted. Refilling does not require a recovered partition. If discovery or Service routing itself is unavailable, use `kubectl port-forward pod/<pod> 7654:7654` for administrative access.
