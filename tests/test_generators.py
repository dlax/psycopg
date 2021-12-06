from collections import deque
from functools import partial
from typing import List

import pytest

from psycopg import waiting
from psycopg.pq.abc import PGresult
from psycopg import pq


@pytest.fixture
@pytest.mark.libpq(">= 14")
def pipeline(pgconn):
    nb, pgconn.nonblocking = pgconn.nonblocking, True
    pgconn.enter_pipeline_mode()
    yield
    pgconn.exit_pipeline_mode()
    pgconn.nonblocking = nb


def test_pipeline(pgconn, pipeline, generators):
    commands = deque(
        [
            partial(pgconn.send_query, b"select 1"),
            partial(pgconn.send_query, b"select error"),
            partial(pgconn.send_query, b"select 3"),
            pgconn.pipeline_sync,
            partial(pgconn.send_query, b"select 2"),
        ]
    )
    ncmds = len(commands)
    gen = generators.pipeline_communicate(pgconn, commands)
    allresults: List[PGresult] = []
    while len(allresults) != ncmds:
        results = waiting.wait(gen, pgconn.socket)
        if results:
            allresults.extend(results)
    (rto1,), (rfe,), (rpa,), (rs,), (rto2,) = results
    assert rto1.status == pq.ExecStatus.TUPLES_OK
    assert rfe.status == pq.ExecStatus.FATAL_ERROR
    assert rpa.status == pq.ExecStatus.PIPELINE_ABORTED
    assert rs.status == pq.ExecStatus.PIPELINE_SYNC
    assert rto2.status == pq.ExecStatus.TUPLES_OK
