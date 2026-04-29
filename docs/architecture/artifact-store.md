---
summary: "SparseKernel content-addressed artifact storage"
read_when:
  - Changing artifact persistence, retention, or access control
  - Handling large tool, browser, download, trace, or transcript outputs
title: "Artifact Store"
sidebarTitle: "Artifact Store"
---

SparseKernel stores large blobs outside SQLite. SQLite records metadata, access, retention policy, and audit events.

Default path shape:

```bash
artifacts/sha256/ab/cd/<sha256>
```

Artifacts include screenshots, downloads, PDFs, browser traces, videos, large HTML dumps, workspace snapshots, exported transcripts, and oversized tool outputs.

Retention policies:

- `ephemeral`
- `session`
- `durable`
- `debug`

Artifact reads and writes are capability-mediated where the caller is not the trusted runtime. Access grants are recorded in `artifact_access`.

The local artifact store accepts bytes, streams, or file paths. File imports stream into a private temporary blob while computing sha256, then move into the content-addressed path, so downloads and snapshots do not need to be loaded fully into memory. The v0 `sparsekerneld` API exposes artifact create/read/metadata endpoints over local JSON. For small compatibility payloads, binary content can still be transported as base64. For large local payloads, use `/artifacts/import-file` with a file staged under the daemon-owned staging directory and `/artifacts/export-file` to copy content into the daemon-owned export directory without moving bytes through JSON. Node clients can use `@openclaw/sparsekernel-client/node-artifacts` to resolve the daemon-compatible staging directory, copy local files into it, and copy exported files to a caller destination without touching the base64 compatibility path.

Browser broker adapters must route screenshots and downloads through this API. The TypeScript CDP broker uses staged local imports when the kernel client supports `/artifacts/import-file`, then falls back to base64 artifact create only for compatibility clients. Agents should receive artifact ids and metadata, not raw browser download paths or large binary payloads.
