# Working on durable-db

Build a small, auditable transactional ordered database. The replacement value index is native Rust owned by durable-db; the SlateDB evaluation has been retired. Apache-2.0. Commit directly to main; no pull requests unless the owner changes that policy. Keep the repository private until explicitly instructed otherwise. The documentation website may be public. Ship verified experimental milestones without waiting for production readiness; label the remaining gates and keep the installer and docs current.

Iteration speed is a product requirement. Prefer the smallest working change, focused local checks in seconds, then commit and continue. Keep the core and proofs small. Reuse existing protocol models instead of duplicating them; keep bounded checks minimal while retaining their negative controls. Remove superseded code, transient documents, speculative scaffolding and redundant pages in the same change. Do not maintain parallel implementations. Report what was deleted as well as what was added.

Protocol changes start in `spec/`, followed by implementation and adversarial tests. Never describe bounded model checking as a proof of the Rust implementation or of every possible deployment. Record assumptions, checked bounds, results, and remaining obligations in `docs/verification.md`.

Object storage is authoritative in cluster mode. Local and memory caches are bounded and disposable. A pod-local lock, clock, cache, Kubernetes lease, or termination hook must never be a cluster correctness dependency. Never silently downgrade conditional object writes to unconditional writes. Transaction scope is one database.

Tenant isolation, resource limits, ambiguous commit outcomes, recovery, and concurrent garbage collection are part of the protocol, not later polish. Do not log secrets, tokens, keys, or values. Keep service-specific integration planning only in `docs/integration.md`.

Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and `scripts/check-model.sh` for relevant protocol changes. Include failure-injection and real HTTP tests. Keep advertised features and benchmark claims tied to checked-in evidence. Repository documentation uses Markdown; the website lives in `site/` and is published with GitHub Pages.

Use https://github.com/kunobi-ninja/kache for all Rust builds. CI, release automation, and website publication use the owner's Woodpecker CI. Never add GitHub Actions workflows or enable Actions builds; they cost the owner credits. Consult sibling projects' `.woodpecker.yaml` for deployment conventions. Keep the pinned kache client and engine together.
