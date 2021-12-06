import pytest

import psycopg
from psycopg import pq
from psycopg.errors import UndefinedColumn, UndefinedTable

pytestmark = [
    pytest.mark.libpq(">= 14"),
    pytest.mark.asyncio,
]


async def test_pipeline_status(aconn):
    async with aconn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        await p.sync()

        # PQpipelineSync
        assert len(p.result_queue) == 1

    assert p.status == pq.PipelineStatus.OFF


async def test_cursor_stream(aconn):
    async with aconn.pipeline(), aconn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            await cur.stream("select 1").__anext__()


async def test_server_cursor(aconn):
    async with aconn.cursor(name="pipeline") as cur, aconn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            await cur.execute("select 1")


async def test_cannot_insert_multiple_commands(aconn):
    with pytest.raises(psycopg.errors.SyntaxError) as cm:
        async with aconn.pipeline():
            await aconn.execute("select 1; select 2")
    assert cm.value.sqlstate == "42601"


async def test_pipeline_processed_at_exit(aconn):
    async with aconn.cursor() as cur:
        async with aconn.pipeline() as pipeline:
            await cur.execute("select 1")
            await pipeline.sync()

            # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
            assert len(pipeline.result_queue) == 3

        assert len(pipeline.result_queue) == 0
        assert await cur.fetchone() == (1,)


async def test_pipeline(aconn):
    async with aconn.pipeline() as pipeline:
        c1 = aconn.cursor()
        c2 = aconn.cursor()
        await c1.execute("select 1")
        await c2.execute("select 2")
        await pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(2), PQpipelineSync
        assert len(pipeline.result_queue) == 4

        (r1,) = await c1.fetchone()
        assert r1 == 1
        assert len(pipeline.result_queue) == 0

    (r2,) = await c2.fetchone()
    assert r2 == 2


async def test_autocommit(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline, aconn.cursor() as c:
        await c.execute("select 1")
        await pipeline.sync()

        # PQsendQuery, PQpipelineSync
        assert len(pipeline.result_queue) == 2

        (r,) = await c.fetchone()
        assert r == 1


async def test_pipeline_aborted(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select 1")
        await pipeline.sync()
        with pytest.raises(UndefinedTable):
            await (
                await aconn.execute("select * from doesnotexist")
            ).fetchone()
        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            await (await aconn.execute("select 'aborted'")).fetchone()
        await pipeline.sync()
        c2 = await aconn.execute("select 2")
        await pipeline.sync()

    (r,) = await c1.fetchone()
    assert r == 1

    (r,) = await c2.fetchone()
    assert r == 2


async def test_pipeline_commit_aborted(aconn):
    with pytest.raises((UndefinedColumn, psycopg.OperationalError)):
        async with aconn.pipeline():
            await aconn.execute("select error")
            await aconn.execute("create table voila ()")
            await aconn.commit()


async def test_prepared(aconn):
    await aconn.set_autocommit(True)
    async with aconn.pipeline() as pipeline:
        c1 = await aconn.execute("select %s::int", [10], prepare=True)
        c2 = await aconn.execute("select count(*) from pg_prepared_statements")
        await pipeline.sync()

        (r,) = await c1.fetchone()
        assert r == 10

        (r,) = await c2.fetchone()
        assert r == 1


async def test_auto_prepare(aconn):
    aconn.prepared_threshold = 5
    async with aconn.pipeline():
        cursors = [
            await aconn.execute("select count(*) from pg_prepared_statements")
            for i in range(10)
        ]

        assert len(aconn._prepared._names) == 1

    res = [(await c.fetchone())[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


async def test_transaction(aconn):
    async with aconn.pipeline():
        async with aconn.transaction():
            cur = await aconn.execute("select 'tx'")

        (r,) = await cur.fetchone()
        assert r == "tx"

        async with aconn.transaction():
            cur = await aconn.execute("select 'rb'")
            raise psycopg.Rollback()

        (r,) = await cur.fetchone()
        assert r == "rb"


async def test_transaction_nested(aconn):
    async with aconn.pipeline():
        async with aconn.transaction():
            outer = await aconn.execute("select 'outer'")
            with pytest.raises(ZeroDivisionError):
                async with aconn.transaction():
                    inner = await aconn.execute("select 'inner'")
                    1 / 0

        (r,) = await outer.fetchone()
        assert r == "outer"
        (r,) = await inner.fetchone()
        assert r == "inner"
