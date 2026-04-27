# Coding Harness Pool

This example is a placeholder for a SparseKernel workload where many coding agents share a small pool of expensive harnesses, sandboxes, and test runners.

Expected resource profile:

- high logical agent count;
- low active harness count;
- scarce browser contexts;
- scarce heavy sandboxes;
- test jobs throttled through leases.
