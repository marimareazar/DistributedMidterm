Task 3 Step 3.2: fixed and FastCDC-style content-defined chunking

Copy shared/, tests/, and benchmarks/ into your EXISTING DistributedMidterm
project. Merge folders: do not delete earlier files. This bundle does not
replace compose.yaml, requirements.txt, metadata_service/, or storage_node/.

From activated .venv at the project root:
    python -m pytest tests/test_chunking.py -v
    python -m benchmarks.compare_chunking

Save screenshots of the passing tests and comparison output.
Output file: reports/chunking_comparison.json

Note: chunking is currently a standalone, tested module. Integrating it with
PostgreSQL metadata and the gateway is the next step, not done in this step.
