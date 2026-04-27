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
