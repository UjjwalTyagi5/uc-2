"""
run_pipeline.py
---------------
Attachment processing pipeline — root entry point.

Usage
-----
    python run_pipeline.py                # process all pending PRs
    python run_pipeline.py --limit 1000   # process at most 1000 PRs per run

For high-volume runs (e.g. 5 lakh PRs), always use --limit so a single
run stays within a manageable time window and a crash doesn't lose all
progress.  The pipeline is fully resumable — restarting picks up where
it left off.

Logs
----
    Console : INFO and above
    File    : logs/pipeline_YYYY-MM-DD.log  (DEBUG+, rotated daily, 30 days)
"""
from pipeline.__main__ import main

if __name__ == "__main__":
    main()
