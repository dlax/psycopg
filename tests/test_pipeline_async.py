import pytest

import psycopg
from psycopg import pq

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


async def test_pipeline_status(aconn):
    async with aconn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        await p.sync()
        r = aconn.pgconn.get_result()
        assert r.status == pq.ExecStatus.PIPELINE_SYNC
        r = aconn.pgconn.get_result()
        assert r is None
    assert p.status == pq.PipelineStatus.OFF


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()
