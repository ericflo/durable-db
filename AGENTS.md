# Working on durable-db

durable-db is a small, auditable, transactional ordered database whose source of truth is an object-storage bucket. Apache-2.0. Commit directly to main; no pull requests unless the owner changes that policy. Keep the repository private until explicitly instructed otherwise. The documentation website may be public.

## The design, in rules

- **Storage is PUT, GET, LIST and DELETE with read-after-write consistency. Nothing else.** Never use conditional writes (`If-Match`, `If-None-Match`): Backblaze B2 rejects them, and B2 and S3 must run the same algorithm. One operation may have a second wire form when measurement justifies it (B2's native upload); the algorithm above it must not change.
- **The bucket is authoritative.** Memory and disk caches are bounded and disposable. A pod-local lock, clock, cache, Kubernetes lease or termination hook is never a correctness dependency. Membership only routes.
- **Every object is immutable and written by one epoch.** An acknowledgement, a linearizable read, a refusal and a deletion each wait for a claim check that began after the bytes they depend on were stored.
- **A server never deletes its own claim while one of its manifest uploads may still arrive.**
- Transaction scope is one database. Tenant isolation, resource limits, unknown commit outcomes, recovery and collection are part of the protocol, not later polish.
- Do not log secrets, tokens, keys, values or database names in metrics.

`docs/architecture.md` explains the protocol; `spec/DurableDb.tla` specifies it; `src/db.rs` implements it.

## How to work

Iteration speed is a product requirement. Prefer the smallest working change, run the focused check, commit, continue. The whole suite (`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`) runs in under a minute; keep it that way.

Protocol changes start in `spec/`, then the implementation, then adversarial tests. Every protocol rule has a negative control in the model that must be refuted. Never describe bounded model checking as a proof of the Rust implementation or of every deployment. Record assumptions, bounds, results and remaining obligations in `docs/verification.md`.

Keep the core small. Remove superseded code, transient documents and speculative scaffolding in the same change that replaces them. Do not keep parallel implementations, evidence archives or dated run logs in the repository. Report what was deleted as well as what was added.

Every performance number in the README, the docs or the website must be reproducible with the command given in `docs/performance.md`, and must say where it was measured. Report misses and limits as plainly as results.

Cap heavy jobs. TLC, load tests and anything else that can grow must run with explicit memory, CPU and time limits (`scripts/check-model.sh` does this). An uncapped model check once exhausted the owner's machine.

## Tooling

Use [kache](https://github.com/kunobi-ninja/kache) for all Rust builds, with the pinned toolchain; keep the kache client and engine versions together. CI, releases and website publication run on the owner's Woodpecker. Never add GitHub Actions workflows or enable Actions builds: they cost the owner credits. Documentation is Markdown in `docs/`; the website is generated from it by `site/` and published to GitHub Pages. Keep service-specific integration planning out of the repository, in the ignored `.dev/integration/`.

Ship verified milestones as releases; state their scope and limits honestly, and keep the installer and docs current.
