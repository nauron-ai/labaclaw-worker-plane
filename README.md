# labaclaw-worker-plane

`labaclaw-worker-plane` is the dedicated execution plane for `claw`, the host-side LabaClaw orchestrator.

It exists as a separate public repo so that:

- orchestrator UX and host runtime stay in `labaclaw`
- Kubernetes worker execution stays isolated
- deploy and versioning of the worker-plane are independent
- no secrets need to live in the repo

## Scope

This repo owns:

- `agent-factory` as the Redpanda-facing launcher/controller
- `agent-runner` as the per-agent runtime entrypoint
- shared worker-plane contracts
- Helm chart for deploying the factory on Kubernetes

This repo does not own:

- the `claw` daemon
- user-facing conversation UX
- PostgreSQL session store on the host
- infra secrets

## Runtime model

Production model:

- `claw` publishes `SpawnAgentRequested` and `TaskAssigned`
- `agent-factory` consumes commands and materializes `1 Deployment per agent`
- each worker Deployment runs `agent-runner`
- artifacts move through RustFS-compatible object storage
- lifecycle and progress flow through Redpanda topics
- `agent-runner` fetches `AgentSpec` and bootstrap/task payloads from RustFS, then publishes heartbeat, questions, progress, and final result events back to the orchestrator

Dev/test model:

- local Docker fallback can still exist in `labaclaw`
- this repo focuses on the production Kubernetes path

## Deploy

The intended GitOps source path is:

- `deploy/helm/labaclaw-worker-plane`

Recommended Argo CD wiring:

- base namespace/secrets stay in infra repo
- this repo stays public and secret-free
- image tag changes roll out through Helm values updates

Expected runtime secret keys:

- `RUSTFS_ACCESS_KEY` or `LABACLAW_RUSTFS_ACCESS_KEY`
- `RUSTFS_SECRET_KEY` or `LABACLAW_RUSTFS_SECRET_KEY`

## Packaging

Production packaging uses a single image that contains both entrypoints:

- `/usr/local/bin/agent-factory`
- `/usr/local/bin/agent-runner`

Build and publish a versioned `linux/amd64` image to GHCR with:

```bash
scripts/build-and-push-ghcr.sh v0.1.9-0006
```

The Helm chart runs `agent-factory` explicitly, and spawned worker Deployments run
`agent-runner` explicitly.
