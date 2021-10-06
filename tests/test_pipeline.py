import pytest

import psycopg
from psycopg import pq

pytestmark = pytest.mark.libpq(">= 14")


def test_pipeline_status(conn):
    with conn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        p.sync()
        r = conn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = conn.pgconn.get_result()
        assert r is None
    assert p.status == pq.PipelineStatus.OFF


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    with conn.cursor(name="pipeline") as cur, conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")
