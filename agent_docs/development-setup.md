# Development Setup

All prototype work happens inside `prototype/`. The venv and Docker services are required.

```bash
cd prototype

# Start PostgreSQL (Iceberg SQL catalog), Redis (distributed locks), MinIO (S3-compatible storage)
docker compose up -d

# Create and activate virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

## Running Benchmarks

Benchmarks are standalone scripts in `prototype/benchmarks/`, not part of pytest:

```bash
python benchmarks/bench_write.py
python benchmarks/bench_read.py
python benchmarks/bench_memory.py write 20   # RSS delta, 20-point community
memray run -o /tmp/out.bin benchmarks/bench_memory.py write 20 && memray flamegraph /tmp/out.bin
```
