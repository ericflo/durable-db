# NeuralColumn integration plan

This is the sole service-specific integration document. The database protocol, code, public website, and general documentation remain product-neutral. Do not publish this document to the documentation website.

The intended host is the NeuralColumn API, with one or more database IDs associated with a site's stable ID. The API already owns user/site identity and capability links. It should provision databases as an administrator, enforce site membership and application rules, and issue short-lived database-scoped capabilities or proxy requests. Site visitors must never receive the database signing key or object-provider credentials.

Each site may provision multiple databases. The bucket directory must support a large site population without per-site buckets or tiny metadata slot limits. Formats 3 through 5 use a persistent B+tree; database cardinality and aggregate publication throughput must be qualified separately. Provisioning quotas should be explicit service policy, not an incidental storage format ceiling.

**Shared platform rollout remains blocked.** The narrowed 1.0 supports one administrative trust domain per deployment, not mutually untrusted sites sharing a database service. Native recovery has replaced the old publication engine and passes bounded B2 failure campaigns; those results do not establish tenant cost/progress isolation or capacity for a large active site population. Complete the deferred isolation, gateway budgets, automatic backups and workload qualification before platform onboarding. Dedicated deployments may be evaluated under the [1.0 scope](production-readiness.md).

Native HTTP serving now integrates transactions, the native index, authorization, durable checkpoints, cold recovery and one explicitly retained checkpoint. Local CLI serving uses this path under a lifetime directory lock. The cloud CLI now uses the native controller with DNS membership, election, routing, fixed partition mapping and bounded resident state. Native B2/MinIO HTTP fixtures now exercise clock-independent permit revocation with a live predecessor; older Harness and autoscaling results qualify the publication engine. Platform rollout must repeat those workloads against native serving with per-site protections. Existing Kubernetes hosts remain required; no special clock profile is selected.

## Acceptance scenario

1. Create a site and persist its database provisioning request ID/configuration before calling protected PUT creation. Retry the identical request after uncertainty; retain the returned incarnation and site mapping in the service control plane, then mint a scoped token. A matching database name alone does not establish provisioning ownership.
2. Use the general KV API for site settings and application records; test denied cross-site reads/writes and a quota-exhausting client.
3. Start a Pi Harness with storage under a dedicated prefix and submit an idempotent request.
4. Interrupt a tool/model turn, replace API pods, and explicitly transfer the Session owner after the harness host restarts.
5. Reopen and resume with no cache state. Validate the transcript, task checkpoints, documents, and request deduplication.
6. Scale API compute 1→3→1 while writers/readers across independent sites continue; kill a pod during durable batch publication, ownership transfer and collection. Resolve uncertain writes without duplicates. Measure each site's latency and progress so aggregate throughput cannot hide starvation of one site.
7. Revoke access, rotate keys, enforce provider spend/rate budgets, export a tenant, and delete it with a defined retention policy.

## Decisions still required

- The existing platform uses B2. B2 is a required authority, using native bucket revision publication rather than S3 conditional file overwrites. The common B2/S3 protocol passes real-B2 integration and a short CPU-driven kind HPA/fault/cold-recovery campaign. Complete independent-node, abuse, latency, and soak qualification before platform rollout. See [the B2 design](b2.md).
- Pi Session execution has one owner. HTTP ingress routing, database partition ownership and Harness execution are separate responsibilities. Scheduling/fencing a running Harness and external tool side effects needs a host protocol in addition to database transactions.
- Browser identity, CORS, individual-user row authorization, and billing belong to the gateway. Per-database scoped credentials alone do not provide row-level access control.
- The indexed Pi adapter passes upstream conformance with scoped credentials and has direct record/index/document storage. B2 conformance and cache-free host restart with explicit takeover pass; eight actual Harness recovery/cancellation cases now pass on three B2 API processes, including competing owners and complete API replacement. Complete sustained-load and host/external-effect fencing qualification before production onboarding; the bounded journal remains a reference fixture.
- Initialize namespace physical ceilings and per-database combined admission/reserve ceilings at provisioning; persist replenishment IDs/revisions and monitor both. Native byte grants separately prepay durable maintenance; every checkpoint retry and recovery claims new allowance, without restoring spent admission rights. Repeated faults can exhaust a tenant reserve and pause a shared checkpoint until refill. These conservative units are not provider billing. Administrative metadata, distributed request/egress budgets and stronger progress isolation still need attribution and qualification. The namespace ceiling remains the physical backstop.
- Production readiness requires strict physical/spend controls, deployment qualification of the implemented overlapping signing-key rollout, tenant lifecycle/export, upgrade strategy, and measured performance under the target workload.

No running NeuralColumn service, bucket, deployment, or production credentials are modified by this repository's initial development work.

Logical recovery tooling exports one revision and restores into a fresh protected database. Keep the destination private until full verification succeeds, then issue a new incarnation-scoped credential and change site routing. The native Rust operator API can retain one exact checkpoint through collection and release it by identity. The operator API now exports any database from that exact retained catalog/value cut into the existing logical archive and restore path. Coordinate slot release after archiving. Administrative scheduling, bounded retained backups and independent restore after source loss remain necessary before treating this as the platform backup service. Multiple in-database restore points and PITR may follow after 1.0.
