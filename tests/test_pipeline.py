import pytest

import psycopg
from psycopg import pq
from psycopg.errors import UndefinedColumn, UndefinedTable

pytestmark = pytest.mark.libpq(">= 14")


def test_pipeline_status(conn):
    with conn.pipeline() as p:
        assert p.status == pq.PipelineStatus.ON
        p.sync()

        # PQpipelineSync
        assert len(p.result_queue) == 1

    assert p.status == pq.PipelineStatus.OFF


def test_cursor_stream(conn):
    with conn.pipeline(), conn.cursor() as cur:
        with pytest.raises(psycopg.ProgrammingError):
            cur.stream("select 1").__next__()


def test_server_cursor(conn):
    with conn.cursor(name="pipeline") as cur, conn.pipeline():
        with pytest.raises(psycopg.NotSupportedError):
            cur.execute("select 1")


def test_cannot_insert_multiple_commands(conn):
    with pytest.raises(psycopg.errors.SyntaxError) as cm:
        with conn.pipeline():
            conn.execute("select 1; select 2")
    assert cm.value.sqlstate == "42601"


def test_pipeline_processed_at_exit(conn):
    with conn.cursor() as cur:
        with conn.pipeline() as pipeline:
            cur.execute("select 1")
            pipeline.sync()

            # PQsendQuery[BEGIN], PQsendQuery, PQpipelineSync
            assert len(pipeline.result_queue) == 3

        assert len(pipeline.result_queue) == 0
        assert cur.fetchone() == (1,)


@pytest.mark.xfail
def test_pipeline_errors_processed_at_exit(conn):
    """Results are fetched upon pipeline exit, even without an explicit
    fetch() call. Here we check that an error is raised. Also check that we
    can issue more query after pipeline exit, thus checking that pipeline
    really exits in case of error.
    """
    conn.autocommit = True
    with pytest.raises(UndefinedTable):
        with conn.pipeline():
            conn.execute("select * from nosuchtable")
            conn.execute("create table voila ()")
    cur = conn.execute(
        "select count(*) from pg_tables where name = %s", ("voila",)
    )
    (count,) = cur.fetchone()
    assert count == 0


def test_pipeline(conn):
    with conn.pipeline() as pipeline:
        c1 = conn.cursor()
        c2 = conn.cursor()
        c1.execute("select 1")
        c2.execute("select 2")
        pipeline.sync()

        # PQsendQuery[BEGIN], PQsendQuery(2), PQpipelineSync
        assert len(pipeline.result_queue) == 4

        (r1,) = c1.fetchone()
        assert r1 == 1
        assert len(pipeline.result_queue) == 0

    (r2,) = c2.fetchone()
    assert r2 == 2


def test_autocommit(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline, conn.cursor() as c:
        c.execute("select 1")
        pipeline.sync()

        # PQsendQuery, PQpipelineSync
        assert len(pipeline.result_queue) == 2

        (r,) = c.fetchone()
        assert r == 1


def test_pipeline_aborted(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        c1 = conn.execute("select 1")
        pipeline.sync()
        with pytest.raises(UndefinedTable):
            conn.execute("select * from doesnotexist").fetchone()
        with pytest.raises(psycopg.OperationalError, match="pipeline aborted"):
            conn.execute("select 'aborted'").fetchone()
        pipeline.sync()
        c2 = conn.execute("select 2")
        pipeline.sync()

    (r,) = c1.fetchone()
    assert r == 1

    (r,) = c2.fetchone()
    assert r == 2


def test_pipeline_commit_aborted(conn):
    with pytest.raises((UndefinedColumn, psycopg.OperationalError)):
        with conn.pipeline():
            conn.execute("select error")
            conn.execute("create table voila ()")
            conn.commit()


def test_prepared(conn):
    conn.autocommit = True
    with conn.pipeline() as pipeline:
        c1 = conn.execute("select %s::int", [10], prepare=True)
        c2 = conn.execute("select count(*) from pg_prepared_statements")
        pipeline.sync()

        (r,) = c1.fetchone()
        assert r == 10

        (r,) = c2.fetchone()
        assert r == 1


def test_auto_prepare(conn):
    conn.autocommit = True
    conn.prepared_threshold = 5
    with conn.pipeline():
        cursors = [
            conn.execute("select count(*) from pg_prepared_statements")
            for i in range(10)
        ]

        assert len(conn._prepared._names) == 1

    res = [c.fetchone()[0] for c in cursors]
    assert res == [0] * 5 + [1] * 5


def test_transaction(conn):
    with conn.pipeline():
        with conn.transaction():
            cur = conn.execute("select 'tx'")

        (r,) = cur.fetchone()
        assert r == "tx"

        with conn.transaction():
            cur = conn.execute("select 'rb'")
            raise psycopg.Rollback()

        (r,) = cur.fetchone()
        assert r == "rb"


def test_transaction_nested(conn):
    with conn.pipeline():
        with conn.transaction():
            outer = conn.execute("select 'outer'")
            with pytest.raises(ZeroDivisionError):
                with conn.transaction():
                    inner = conn.execute("select 'inner'")
                    1 / 0

        (r,) = outer.fetchone()
        assert r == "outer"
        (r,) = inner.fetchone()
        assert r == "inner"
