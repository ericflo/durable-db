# Security and resource isolation

## Trust boundaries

Database users can be buggy or hostile. They can send arbitrary requests, race transactions, abandon responses, and exhaust their allowed quotas. They cannot access object-store credentials, the signing key, server files, or another tenant's token. Compromised servers and malicious storage providers are outside the current threat model.

Only administrators create databases, set initial quotas, mint scoped tokens, or request explicit maintenance. Signed HMAC-SHA256 capability tokens bind one database, read/write permission, and expiration. Signature checks use constant-time verification. Authorization occurs before data access. Key names are data, never filesystem paths. Database identifiers are validated independently.

Tokens are bearer capabilities. Keep admin credentials on the server, use HTTPS at ingress, issue short-lived application tokens, and avoid embedding long-lived credentials in static assets. CORS is denied by default; a trusted application gateway supplies browser origin and user authorization policy. Token expiry uses wall-clock time; synchronize node clocks. The storage safety protocol does not use clocks.

## Resource controls

Logical bytes and key quotas live in the same manifest CAS as the write. Competing pods cannot separately approve growth beyond the quota. Oversized values, operations, manifests, results, and page rewrites are rejected. Global concurrent requests, rate-limiter entries, and immutable caches are bounded. No request loads the entire database.

Cache budgets account for encoded data plus conservative entry/key overhead. They are not a hard process-RSS promise: decoding, HTTP buffers, TLS, allocator overhead, and concurrent requests consume additional memory. Kubernetes memory limits and sensible concurrency settings provide the outer bound. OOM or SIGKILL may interrupt availability but cannot acknowledge an uncommitted manifest.

Rate limits are per database **per pod**, not a strict global billing limit. More pods increase the aggregate allowance. Successful logical quotas do not cap the temporary physical cost of aborted uploads. Collection eventually reclaims those uploads when the provider and collectors remain available. A hostile public deployment therefore also needs gateway-level tenant budgets and object-provider billing alerts. Cluster-wide physical reservations and spend admission remain a release gate; do not sell the current quota as a hard storage-bill cap.

## Operations

Use a private dedicated bucket and least-privilege permissions to one prefix. Enforce TLS, encryption at rest, and conditional writes with bucket policy where supported. Configure version-expiration lifecycle if the bucket is versioned. Never expose object credentials to database clients. The server emits control/error categories, not request keys, values, credentials, or provider error strings.

Rotate the shared signing key by restarting all pods with the replacement; all existing tokens become invalid. Per-token revocation, overlapping key rotation, audit-event export, and tenant lifecycle deletion are planned. The initial release has not had an independent security review.
