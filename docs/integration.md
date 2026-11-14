# NeuralColumn integration plan

This is the only service-specific document in the repository. It is not published to the website. Nothing here has been applied: no NeuralColumn service, bucket, cluster or secret is touched by this repository.

## What NeuralColumn gets

A database service for sites: each site can have one or more databases, used through the general key-value API or as pi-durable storage. Site visitors and site code never see a durable-db token, the signing key or a bucket credential; they talk to the NeuralColumn API, which authorizes the request and relays it.

```
browser or site code ── HTTPS ──▶ api.neuralcolumn.com ── HTTP, in-cluster ──▶ durable-db Service ──▶ B2 bucket
                                  (auth, CORS, site rules)                       (2+ pods, autoscaled)
```

## Deploying durable-db in Epsilon

Following NeuralColumn's own deployment rules (`DEPLOYMENT.md` there): the application repository owns source, Dockerfile and pipeline; `ericflo/epsilon` owns every Kubernetes object.

1. **Image.** This repository's Woodpecker pipeline already publishes `registry.clouderic.svc.cluster.local:5000/durable-db:main-<timestamp>-<sha12>` on every push to `main`, with provenance. The repository must be admitted to Woodpecker through Epsilon's reconciler (`platform/ci/reconcile.py`), as other repositories are.
2. **Bucket.** A new private B2 bucket used by nothing else, for example `neuralcolumn-db`, and an application key restricted to it with `listBuckets`, `listFiles`, `readFiles`, `writeFiles`, `deleteFiles`. No lifecycle rules. Do not reuse the `neuralcolumn` bucket: durable-db deletes objects under its prefix by listing, and a shared bucket puts site files one misconfigured prefix away from that.
3. **Manifests.** Add `apps/durable-db/` to Epsilon with the contents of [`deploy/kubernetes`](../deploy/kubernetes) in its own namespace, plus:
   - `Secret/durable-db-keys` (SOPS): `admin.key`, from `openssl rand -hex 32`.
   - `Secret/durable-db-storage` (SOPS): `DURABLE_DB_BUCKET`, `DURABLE_DB_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
   - An `ImageRepository`, `ImagePolicy` (newest `main-*` tag by timestamp) and `ImageUpdateAutomation`, as for NeuralColumn.
   - A `NetworkPolicy` that admits traffic to port 7654 only from the NeuralColumn server's pods and from durable-db's own pods. There is no Ingress: the service is not exposed outside the cluster.
   - A scrape of `/metrics`.
4. **NeuralColumn's side.** The NeuralColumn server needs the same `admin.key` (a second SOPS secret in its namespace) to mint tokens, and the Service URL `http://durable-db.durable-db.svc.cluster.local:7654`.

Rolling updates of durable-db do not take databases offline: each pod hands its databases over before it exits. This is unlike the NeuralColumn server, which is a single replica.

## What the NeuralColumn server does

**Provisioning.** When a site asks for a database, the server records the request in the site's own state first, then calls `PUT /v1/databases/<name>` with the site's limits, using an administrator token it mints locally. The call is idempotent, so it is safe to repeat after a crash. Name databases `<siteID>-<label>`: site IDs are unique and never reused, which matters because a deleted database's name cannot be used again.

**Minting tokens** needs no call to durable-db. A token is `ddb_<payload>.<signature>` where `payload` is base64url (no padding) of the JSON `{"db":"*","access":"admin","exp":0,"generation":0}` and `signature` is base64url of HMAC-SHA256 over the payload text with the 32-byte key decoded from the hex in `admin.key`. In Go this is about fifteen lines with the standard library.

**Relaying.** Expose, under the existing site API, something like `POST /v1/sites/{id}/databases/{label}/read` and `/commit`. For each request the server authenticates the caller as it does today, checks that the site owns the database, applies any per-site rule (read-only visitors, for example), and forwards the body unchanged to durable-db with a token. Responses and status codes pass through; `503` responses carry `Retry-After`.

**Limits per site**, set at creation and changeable with `PATCH`:

| Setting | Suggested start | Effect |
| --- | --- | --- |
| `max_bytes` | 256 MiB | Writes that add data beyond it get `507`. |
| `max_requests_per_second` | 50 | Requests beyond it get `429`. |
| databases per site | 4 | Enforced by NeuralColumn, not durable-db. |

**Erasing a site** deletes its databases with `DELETE /v1/databases/<name>`.

## pi-durable

A site that runs a pi Harness uses the storage adapter from `@durable-db/client/pi`, pointed at the relay URL with the site's NeuralColumn credential. One pi Session is the single owner of its storage; the adapter relies on that, as every pi-durable storage does. Scheduling which host runs a Session, and fencing side effects outside the database, are the host's responsibility and are not solved by durable-db.

## Before the first site uses it

- [ ] Bucket and restricted key created; key works only on that bucket.
- [ ] `scripts/test-kubernetes.sh` passes against the new bucket from a workstation.
- [ ] Manifests applied by Flux; two pods ready; `/readyz` green.
- [ ] From a pod in the NeuralColumn namespace: create a database, write, read, delete.
- [ ] From any other namespace: the connection is refused by the NetworkPolicy.
- [ ] `durable-db bench --workload ledger` run as a Job against the Service for ten minutes, with one pod deleted during the run: verified.
- [ ] Metrics are scraped; an alert exists on `durable_db_storage_failures_total` rising.
- [ ] The relay endpoints refuse a request for another site's database.

## Rollback

Scale the Deployment to zero. Databases stay in the bucket untouched and are served again when a pod starts. Removing the relay endpoints from NeuralColumn removes the feature without touching data.

## Decisions for the owner

- **One bucket key or two.** The plan uses one key for all pods. A read-only second key is not useful: every pod must write.
- **Backups.** B2 replication of the database bucket to a second bucket gives disaster recovery. durable-db itself keeps no history.
- **Browser access.** The plan relays every request through NeuralColumn. Short-lived database tokens handed to browsers would save a hop but require CORS and public exposure of durable-db; not recommended.
- **Billing.** durable-db enforces limits; it does not meter usage. `GET /v1/databases/<name>` reports `bytes` and `revision` for whatever accounting NeuralColumn wants.
