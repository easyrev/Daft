Draft only — not posted. Measurements were performed by Codex and have not yet been manually rechecked by the contributor.

I reproduced the remaining request duplication on upstream main `d3cd974150ba93dbbe8efef8370b129538fcee4b` using a source-built dev extension and the native runner, with a local HTTP range server and the repository's SeaweedFS S3-compatible service. Each baseline case was run in three fresh processes and all rows/columns were checked against a PyArrow-generated oracle.

The two cases are distinct:

- With default suffix ranges, a remote reader makes one HEAD for its file length while fetching the footer via suffix GET.
- With `IOConfig(disable_suffix_range=True)`, the same initialization makes two successful HEADs: one inside footer loading and one from the parallel size lookup. A direct eager reader produced 1 vs. 2 HEADs, with the same four GETs and 5,189 response-body bytes. A two-file S3-compatible glob scan produced 2 vs. 4 execution HEADs, plus a separate discovery LIST that already contained both physical file sizes.

Logging-only instrumentation confirmed that the executed native ScanSources retain those physical sizes, but the reader does not receive them. Simply forwarding `ScanSource.size_bytes` is unsafe for split tasks, where that field becomes the compressed size of the selected row groups. Schema inference and execution also reread the footer; I would leave that broader metadata reuse work separate.

Would a small reader-local change that shares the size result only when suffix ranges are disabled, preserving the default parallel footer/HEAD flow and the unknown-size fallback, be an acceptable first scope for this issue?

These measurements establish request behavior on local HTTP/S3-compatible services, not AWS latency or an end-to-end speedup.
