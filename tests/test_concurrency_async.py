import sys
import time
import signal
import asyncio
import subprocess as sp
from asyncio.queues import Queue
from typing import List, Tuple

import anyio
import pytest

import psycopg
from psycopg import errors as e
from psycopg._compat import create_task


@pytest.mark.slow
async def test_commit_concurrency(aconn, use_anyio):
    # Check the condition reported in psycopg2#103
    # Because of bad status check, we commit even when a commit is already on
    # its way. We can detect this condition by the warnings.
    stop = False

    async def committer(sleepfn):
        nonlocal stop
        while not stop:
            await aconn.commit()
            await sleepfn(0)  # Allow the other worker to work

    async def runner():
        nonlocal stop
        cur = aconn.cursor()
        for i in range(1000):
            await cur.execute("select %s;", (i,))
            await aconn.commit()

        # Stop the committer thread
        stop = True

    if not use_anyio:
        notices = Queue()  # type: ignore[var-annotated]
        aconn.add_notice_handler(lambda diag: notices.put_nowait(diag.message_primary))
        await asyncio.gather(committer(asyncio.sleep), runner())
        assert notices.empty(), "%d notices raised" % notices.qsize()
    else:
        send_notices, receive_notices = anyio.create_memory_object_stream()
        aconn.add_notice_handler(
            lambda diag: send_notices.send_nowait(diag.message_primary)
        )
        async with anyio.create_task_group() as tg:
            async with send_notices:
                tg.start_soon(committer, anyio.sleep)
                tg.start_soon(runner)
        stats = receive_notices.statistics()
        assert stats.current_buffer_used == 0, (
            "%d notices raised" % stats.current_buffer_used
        )


@pytest.mark.slow
async def test_concurrent_execution(aconn_cls, dsn, use_anyio):
    async def worker():
        cnn = await aconn_cls.connect(dsn)
        cur = cnn.cursor()
        await cur.execute("select pg_sleep(0.5)")
        await cur.close()
        await cnn.close()

    t0 = time.time()
    if not use_anyio:
        workers = [worker(), worker()]
        await asyncio.gather(*workers)
    else:
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker)
            tg.start_soon(worker)
    assert time.time() - t0 < 0.8, "something broken in concurrency"


@pytest.mark.slow
@pytest.mark.timing
@pytest.mark.crdb_skip("notify")
async def test_notifies(aconn_cls, aconn, dsn, use_anyio):
    nconn = await aconn_cls.connect(dsn, autocommit=True)
    npid = nconn.pgconn.backend_pid

    async def notifier(sleepfn):
        cur = nconn.cursor()
        await sleepfn(0.25)
        await cur.execute("notify foo, '1'")
        await sleepfn(0.25)
        await cur.execute("notify foo, '2'")
        await nconn.close()

    async def receiver():
        await aconn.set_autocommit(True)
        cur = aconn.cursor()
        await cur.execute("listen foo")
        gen = aconn.notifies()
        async for n in gen:
            ns.append((n, time.time()))
            if len(ns) >= 2:
                await gen.aclose()

    ns: List[Tuple[psycopg.Notify, float]] = []
    t0 = time.time()
    if not use_anyio:
        workers = [notifier(asyncio.sleep), receiver()]
        await asyncio.gather(*workers)
    else:
        async with anyio.create_task_group() as tg:
            tg.start_soon(notifier, anyio.sleep)
            tg.start_soon(receiver)
    assert len(ns) == 2

    n, t1 = ns[0]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "1"
    assert t1 - t0 == pytest.approx(0.25, abs=0.05)

    n, t1 = ns[1]
    assert n.pid == npid
    assert n.channel == "foo"
    assert n.payload == "2"
    assert t1 - t0 == pytest.approx(0.5, abs=0.05)


async def canceller(aconn, errors, sleepfn):
    try:
        await sleepfn(0.5)
        aconn.cancel()
    except Exception as exc:
        errors.append(exc)


@pytest.mark.slow
@pytest.mark.crdb_skip("cancel")
async def test_cancel(aconn, use_anyio):
    async def worker():
        cur = aconn.cursor()
        with pytest.raises(e.QueryCanceled):
            await cur.execute("select pg_sleep(2)")

    errors: List[Exception] = []
    t0 = time.time()
    if not use_anyio:
        workers = [worker(), canceller(aconn, errors, asyncio.sleep)]
        await asyncio.gather(*workers)
    else:
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker)
            tg.start_soon(canceller, aconn, errors, anyio.sleep)

    t1 = time.time()
    assert not errors
    assert 0.0 < t1 - t0 < 1.0

    # still working
    await aconn.rollback()
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)


@pytest.mark.slow
@pytest.mark.crdb_skip("cancel")
async def test_cancel_stream(aconn, use_anyio):
    async def worker():
        cur = aconn.cursor()
        with pytest.raises(e.QueryCanceled):
            async for row in cur.stream("select pg_sleep(2)"):
                pass

    errors: List[Exception] = []
    t0 = time.time()
    if not use_anyio:
        workers = [worker(), canceller(aconn, errors, asyncio.sleep)]
        await asyncio.gather(*workers)
    else:
        async with anyio.create_task_group() as tg:
            tg.start_soon(worker)
            tg.start_soon(canceller, aconn, errors, anyio.sleep)

    t1 = time.time()
    assert not errors
    assert 0.0 < t1 - t0 < 1.0

    # still working
    await aconn.rollback()
    cur = aconn.cursor()
    await cur.execute("select 1")
    assert await cur.fetchone() == (1,)


@pytest.mark.slow
@pytest.mark.crdb_skip("pg_terminate_backend")
async def test_identify_closure(aconn_cls, dsn, use_anyio):
    async def closer(sleepfn):
        await sleepfn(0.2)
        await conn2.execute(
            "select pg_terminate_backend(%s)", [aconn.pgconn.backend_pid]
        )

    aconn = await aconn_cls.connect(dsn)
    conn2 = await aconn_cls.connect(dsn)
    try:
        if not use_anyio:
            t = create_task(closer(asyncio.sleep))
            t0 = time.time()
            try:
                with pytest.raises(psycopg.OperationalError):
                    await aconn.execute("select pg_sleep(1.0)")
                t1 = time.time()
                assert 0.2 < t1 - t0 < 0.4
            finally:
                await asyncio.gather(t)
        else:
            async with anyio.create_task_group() as tg:
                tg.start_soon(closer, anyio.sleep)
                t0 = time.time()
                with pytest.raises(psycopg.OperationalError):
                    await aconn.execute("select pg_sleep(1.0)")
                t1 = time.time()
                assert 0.2 < t1 - t0 < 0.4
    finally:
        await aconn.close()
        await conn2.close()


@pytest.mark.slow
@pytest.mark.subprocess
@pytest.mark.skipif(
    sys.platform == "win32", reason="don't know how to Ctrl-C on Windows"
)
@pytest.mark.crdb_skip("cancel")
def test_ctrl_c(dsn, use_anyio):
    if not use_anyio:
        script = f"""\
import signal
import asyncio
import psycopg

async def main():
    ctrl_c = False
    loop = asyncio.get_event_loop()
    async with await psycopg.AsyncConnection.connect({dsn!r}) as conn:
        loop.add_signal_handler(signal.SIGINT, conn.cancel)
        cur = conn.cursor()
        try:
            await cur.execute("select pg_sleep(2)")
        except psycopg.errors.QueryCanceled:
            ctrl_c = True

        assert ctrl_c, "ctrl-c not received"
        assert (
            conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
        ), f"transaction status: {{conn.info.transaction_status!r}}"

        await conn.rollback()
        assert (
            conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        ), f"transaction status: {{conn.info.transaction_status!r}}"

        await cur.execute("select 1")
        assert (await cur.fetchone()) == (1,)

asyncio.run(main())
"""
    else:
        script = f"""\
import signal
import anyio
import psycopg

async def signal_handler(conn, scope):
    with anyio.open_signal_receiver(signal.SIGINT) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                conn.cancel()
            return

async def main():
    ctrl_c = False
    async with await psycopg.AnyIOConnection.connect({dsn!r}) as conn:
        async with anyio.create_task_group() as tg:
            tg.start_soon(signal_handler, conn, tg.cancel_scope)

            cur = conn.cursor()
            try:
                await cur.execute("select pg_sleep(2)")
            except psycopg.errors.QueryCanceled:
                ctrl_c = True

            assert ctrl_c, "ctrl-c not received"
            assert (
                conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR
            ), f"transaction status: {{conn.info.transaction_status!r}}"

            await conn.rollback()
            assert (
                conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            ), f"transaction status: {{conn.info.transaction_status!r}}"

        await cur.execute("select 1")
        assert (await cur.fetchone()) == (1,)

anyio.run(main, backend="trio")
"""
    if sys.platform == "win32":
        creationflags = sp.CREATE_NEW_PROCESS_GROUP
        sig = signal.CTRL_C_EVENT
    else:
        creationflags = 0
        sig = signal.SIGINT

    proc = sp.Popen([sys.executable, "-s", "-c", script], creationflags=creationflags)
    with pytest.raises(sp.TimeoutExpired):
        outs, errs = proc.communicate(timeout=1)

    proc.send_signal(sig)
    proc.communicate()
    assert proc.returncode == 0
