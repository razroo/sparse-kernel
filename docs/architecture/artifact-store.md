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

The v0 `sparsekerneld` API exposes artifact create/read/metadata endpoints over local JSON. Binary content is transported as base64 in the API; the daemon writes the bytes to the content-addressed store and records only metadata, permissions, and audit events in SQLite.
