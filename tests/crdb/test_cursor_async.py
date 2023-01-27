import json
import asyncio
import math
from typing import Any
from asyncio.queues import Queue

import anyio
import pytest
from psycopg import pq, errors as e
from psycopg.rows import namedtuple_row
from psycopg._compat import create_task

from .test_cursor import testfeed

testfeed  # fixture

pytestmark = [pytest.mark.crdb, pytest.mark.anyio]


@pytest.mark.slow
@pytest.mark.parametrize("fmt_out", pq.Format)
async def test_changefeed(aconn_cls, dsn, aconn, testfeed, fmt_out, use_anyio):
    await aconn.set_autocommit(True)

    async def worker(enqueue):
        try:
            async with await aconn_cls.connect(dsn, autocommit=True) as conn:
                cur = conn.cursor(binary=fmt_out, row_factory=namedtuple_row)
                streamit = cur.stream(
                    f"experimental changefeed for {testfeed}"
                ).__aiter__()
                try:
                    async for row in streamit:
                        enqueue(row)
                except e.QueryCanceled:
                    assert conn.info.transaction_status == conn.TransactionStatus.IDLE
                    enqueue(None)
                finally:
                    await streamit.aclose()
        except Exception as ex:
            enqueue(ex)

    if not use_anyio:
        q: "Queue[Any]" = Queue()  # infinite queue
        t = create_task(worker(q.put_nowait))
        cur = aconn.cursor()

        await cur.execute(
            f"insert into {testfeed} (data) values ('hello') returning id"
        )
        (key,) = await cur.fetchone()
        row = await asyncio.wait_for(q.get(), 1.0)
        assert row.table == testfeed
        assert json.loads(row.key) == [key]
        assert json.loads(row.value)["after"] == {"id": key, "data": "hello"}

        await cur.execute(f"delete from {testfeed} where id = %s", [key])
        row = await asyncio.wait_for(q.get(), 1.0)
        assert row.table == testfeed
        assert json.loads(row.key) == [key]
        assert json.loads(row.value)["after"] is None

        await cur.execute(
            "select query_id from [show statements] where query !~ 'show'"
        )
        (qid,) = await cur.fetchone()
        await cur.execute("cancel query %s", [qid])
        assert cur.statusmessage == "CANCEL QUERIES 1"

        # We often find the record with {"after": null} at least another time
        # in the queue. Let's tolerate an extra one.
        for i in range(2):
            row = await asyncio.wait_for(q.get(), 1.0)
            if row is None:
                break
            assert json.loads(row.value)["after"] is None, json
        else:
            pytest.fail("keep on receiving messages")

        await asyncio.gather(t)
    else:
        send_stream, receive_stream = anyio.create_memory_object_stream(
            max_buffer_size=math.inf  # unbounded buffer
        )
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker, send_stream.send_nowait)
            cur = aconn.cursor()
            async with receive_stream:
                await cur.execute(
                    f"insert into {testfeed} (data) values ('hello') returning id"
                )
                (key,) = await cur.fetchone()
                with anyio.fail_after(1.0):
                    row = await receive_stream.receive()
                assert row.table == testfeed
                assert json.loads(row.key) == [key]
                assert json.loads(row.value)["after"] == {"id": key, "data": "hello"}

                await cur.execute(f"delete from {testfeed} where id = %s", [key])
                with anyio.fail_after(1.0):
                    row = await receive_stream.receive()
                assert row.table == testfeed
                assert json.loads(row.key) == [key]
                assert json.loads(row.value)["after"] is None

                await cur.execute(
                    "select query_id from [show statements] where query !~ 'show'"
                )
                (qid,) = await cur.fetchone()
                await cur.execute("cancel query %s", [qid])
                assert cur.statusmessage == "CANCEL QUERIES 1"

                # We often find the record with {"after": null} at least another time
                # in the queue. Let's tolerate an extra one.
                for i in range(2):
                    with anyio.fail_after(1.0):
                        row = await receive_stream.receive()
                    if row is None:
                        break
                    assert json.loads(row.value)["after"] is None, json
                else:
                    pytest.fail("keep on receiving messages")
