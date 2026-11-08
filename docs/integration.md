# NeuralColumn integration plan

This is the sole service-specific integration document. The database protocol, code, public website, and general documentation remain product-neutral. Do not publish this document to the documentation website.

The intended host is the NeuralColumn API, with one or more database IDs associated with a site's stable ID. The API already owns user/site identity and capability links. It should provision databases as an administrator, enforce site membership and application rules, and issue short-lived database-scoped capabilities or proxy requests. Site visitors must never receive the database signing key or object-provider credentials.

Each site may provision multiple databases. The bucket directory must support a large site population without per-site buckets or tiny metadata slot limits. Format 3 uses a persistent B+tree; database cardinality and aggregate publication throughput must be qualified separately. Provisioning quotas should be explicit service policy, not an incidental storage format ceiling.

## Acceptance scenario

1. Create a site, provision a database, and persist the mapping in the service control plane.
2. Use the general KV API for site settings and application records; test denied cross-site reads/writes and a quota-exhausting client.
3. Start a Pi Harness with storage under a dedicated prefix and submit an idempotent request.
4. Interrupt a tool/model turn, replace API pods, and explicitly transfer the Session owner after the harness host restarts.
5. Reopen and resume with no cache state. Validate the transcript, task checkpoints, documents, and request deduplication.
6. Scale API compute 1→3→1 while writers/readers continue; kill a pod during manifest publication and during collection. Resolve uncertain writes without duplicates.
7. Revoke access, rotate keys, enforce provider spend/rate budgets, export a tenant, and delete it with a defined retention policy.

## Decisions still required

- The existing platform uses B2. B2 is a required authority, using native bucket revision publication rather than S3 conditional file overwrites. The common B2/S3 protocol has passed its initial real-B2 test; complete scaling, abuse, and soak qualification before platform rollout. See [the B2 design](b2.md).
- Pi Session execution has one owner even though database API pods are interchangeable. Scheduling/fencing a running Harness and external tool side effects needs a host protocol in addition to database transactions.
- Browser identity, CORS, individual-user row authorization, and billing belong to the gateway. Per-database scoped credentials alone do not provide row-level access control.
- The reference Pi adapter validates semantics but needs replacement with the indexed adapter before production onboarding.
- Production readiness requires strict global physical/spend controls, key rotation/revocation, tenant lifecycle/export, upgrade strategy, and measured performance under the target workload.

No running NeuralColumn service, bucket, deployment, or production credentials are modified by this repository's initial development work.
