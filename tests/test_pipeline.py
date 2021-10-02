import pytest

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
