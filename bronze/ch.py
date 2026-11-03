"""Run SQL against ClickHouse, two interchangeable backends.

  default            the standalone `clickhouse` binary (`clickhouse local`),
                     data kept in ./.chdata. A file lock serialises calls,
                     because the binary allows one process per data folder
                     and Airflow runs tasks in parallel.
  CLICKHOUSE_URL set a real server over HTTP (e.g. http://localhost:8123,
                     or the Docker service). Optional CLICKHOUSE_USER /
                     CLICKHOUSE_PASSWORD.

Same function signatures either way, so the builders, verifiers and Airflow
tasks do not care which backend is in use.
"""
import fcntl
import json
import os
import subprocess
import urllib.request
from contextlib import contextmanager
from pathlib import Path

CHDATA = Path(__file__).resolve().parent.parent / ".chdata"
LOCKFILE = CHDATA.parent / ".chdata.lock"


@contextmanager
def _lock():
    with open(LOCKFILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _http(sql):
    req = urllib.request.Request(os.environ["CLICKHOUSE_URL"], data=sql.encode(), method="POST")
    req.add_header("X-ClickHouse-User", os.environ.get("CLICKHOUSE_USER", "default"))
    req.add_header("X-ClickHouse-Key", os.environ.get("CLICKHOUSE_PASSWORD", ""))
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()[:800]) from None


def _local(binary, args):
    with _lock():
        p = subprocess.run([binary, "local", "--path", str(CHDATA)] + args, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or "clickhouse failed")
    return p.stdout


def _split(sql_text):
    return [s.strip() for s in sql_text.split(";\n") if s.strip()]


def execute(binary, sql):
    """Run one statement, no result."""
    if os.environ.get("CLICKHOUSE_URL"):
        _http(sql)
    else:
        _local(binary, ["--query", sql])


def run_file(binary, sql_file):
    """Run every statement in a .sql file, in order."""
    text = Path(sql_file).read_text()
    if os.environ.get("CLICKHOUSE_URL"):
        for stmt in _split(text):
            _http(stmt)
    else:
        _local(binary, ["--multiquery", "--queries-file", str(sql_file)])


def query(binary, sql):
    """Run one query, return a list of dict rows."""
    if os.environ.get("CLICKHOUSE_URL"):
        out = _http(sql.rstrip().rstrip(";") + " FORMAT JSONEachRow")
    else:
        out = _local(binary, ["--format", "JSONEachRow", "--query", sql])
    return [json.loads(line) for line in out.splitlines() if line.strip()]
