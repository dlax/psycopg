"""Pipeline mode demo

This reproduces libpq_pipeline::pipelined_insert PostgreSQL test at
src/test/modules/libpq_pipeline/libpq_pipeline.c::test_pipelined_insert().

We do not fetch results explicitly (using cursor.fetch*()), this is
handled by execute() calls when pgconn socket is read-ready, which
happens when the output buffer is full.
"""
import argparse
import asyncio
import logging
from collections import OrderedDict, deque
from typing import Any, Deque, Iterable, Optional, Sequence

from psycopg import AsyncConnection, Connection
from psycopg import pq, waiting
from psycopg import errors as e
from psycopg.abc import Command
from psycopg.generators import pipeline_communicate
from psycopg.pq._enums import Format


class LoggingPGconn:
    """Wrapper for PGconn that logs fetched results."""

    def __init__(self, pgconn: pq.abc.PGconn, logger: logging.Logger):
        self._pgconn = pgconn
        self._logger = logger

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pgconn, name)

    def send_query(self, command: bytes) -> None:
        self._pgconn.send_query(command)
        self._logger.info("sent %s", command.decode())

    def send_query_params(
        self,
        command: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_types: Optional[Sequence[int]] = None,
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        self._pgconn.send_query_params(
            command, param_values, param_types, param_formats, result_format
        )
        self._logger.info("sent %s", command.decode())

    def send_query_prepared(
        self,
        name: bytes,
        param_values: Optional[Sequence[Optional[bytes]]],
        param_formats: Optional[Sequence[int]] = None,
        result_format: int = Format.TEXT,
    ) -> None:
        self._pgconn.send_query_prepared(
            name, param_values, param_formats, result_format
        )
        self._logger.info("sent prepared '%s'", name.decode())

    def send_prepare(
        self,
        name: bytes,
        command: bytes,
        param_types: Optional[Sequence[int]] = None,
    ) -> None:
        self._pgconn.send_prepare(name, command, param_types)
        self._logger.info(
            "prepare %s as '%s'", command.decode(), name.decode()
        )

    def get_result(self) -> Optional[pq.abc.PGresult]:
        r = self._pgconn.get_result()
        if r is not None:
            self._logger.info("got %s result", pq.ExecStatus(r.status).name)
        return r


class DemoPipeline:
    """Handle for pipeline demo using 'pq' API."""

    _queries = OrderedDict(
        {
            "begin": "BEGIN TRANSACTION",
            "drop table": "DROP TABLE IF EXISTS pq_pipeline_demo",
            "create table": (
                "CREATE UNLOGGED TABLE pq_pipeline_demo("
                " id serial primary key,"
                " itemno integer,"
                " int8filler int8"
                ")"
            ),
            "prepare": (
                "INSERT INTO pq_pipeline_demo(itemno, int8filler)"
                " VALUES ($1, $2)"
            ),
        }
    )

    def __init__(
        self, pgconn: pq.abc.PGconn, logger: logging.Logger, rows_to_send: int
    ) -> None:
        self.pgconn = pgconn
        self.logger = logger
        self.queue: Deque[str] = deque()
        self.rows_to_send = rows_to_send
        self.committed = False
        self.synced = False

    def __enter__(self) -> "DemoPipeline":
        logger = self.logger
        pgconn = self.pgconn

        logger.debug("enter pipeline")
        pgconn.enter_pipeline_mode()

        for qname, query in self._queries.items():
            self.queue.append(qname)
            if qname == "prepare":
                insert_name = qname.encode()
                pgconn.send_prepare(insert_name, query.encode())
                logger.info(
                    "prepared '%s' as '%s'", query, insert_name.decode()
                )
            else:
                pgconn.send_query(query.encode())
                logger.info("sent %s", query)

        pgconn.nonblocking = 1
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.logger.debug("exit pipeline")
        self.pgconn.exit_pipeline_mode()

    def __iter__(self) -> "DemoPipeline":
        return self

    def __next__(self) -> Command:
        pgconn = self.pgconn
        logger = self.logger
        queue = self.queue
        if self.rows_to_send:

            def send_query_prepared(rown: int = self.rows_to_send) -> None:
                params = [
                    f"{rown}".encode(),
                    f"{1 << 62}".encode(),
                ]
                pgconn.send_query_prepared(b"prepare", params)
                logger.info(f"sent row {rown}")

            self.rows_to_send -= 1
            queue.append(f"insert {self.rows_to_send}")
            return send_query_prepared
        elif not self.committed:

            def commit() -> None:
                pgconn.send_query(b"COMMIT")
                logger.info("sent COMMIT")

            self.committed = True
            queue.append("commit")
            return commit
        elif not self.synced:

            def sync() -> None:
                pgconn.pipeline_sync()
                logger.info("pipeline sync sent")

            self.synced = True
            queue.append("pipeline sync")
            return sync
        else:
            raise StopIteration()

    def process_results(
        self, fetched: Iterable[Iterable[pq.abc.PGresult]]
    ) -> None:
        for results in fetched:
            self.queue.popleft()
            for r in results:
                if r.status in (
                    pq.ExecStatus.FATAL_ERROR,
                    pq.ExecStatus.PIPELINE_ABORTED,
                ):
                    raise e.error_from_result(r)


def pipeline_demo_pq(rows_to_send: int, logger: logging.Logger) -> None:
    pgconn = Connection.connect().pgconn
    return _pipeline_demo_pq(pgconn, rows_to_send, logger)


async def pipeline_demo_pq_async(
    rows_to_send: int, logger: logging.Logger
) -> None:
    pgconn = (await AsyncConnection.connect()).pgconn
    return _pipeline_demo_pq(pgconn, rows_to_send, logger)


def _pipeline_demo_pq(
    pgconn: pq.abc.PGconn, rows_to_send: int, logger: logging.Logger
) -> None:
    handler = DemoPipeline(pgconn, logger, rows_to_send)

    socket = pgconn.socket
    wait = waiting.wait

    with handler:
        commands = deque(handler)
        while handler.queue:
            gen = pipeline_communicate(
                LoggingPGconn(pgconn, logger),  # type: ignore[arg-type]
                commands,
            )
            fetched = wait(gen, socket)
            assert not commands, commands
            handler.process_results(fetched)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        dest="nrows",
        metavar="ROWS",
        default=10_000,
        type=int,
        help="number of rows to insert",
    )
    parser.add_argument(
        "--async", dest="async_", action="store_true", help="use async API"
    )
    parser.add_argument("-l", "--log", help="log file (stderr by default)")
    args = parser.parse_args()
    logger = logging.getLogger("psycopg")
    logger.setLevel(logging.DEBUG)
    pipeline_logger = logging.getLogger("pipeline")
    pipeline_logger.setLevel(logging.DEBUG)
    if args.log:
        logger.addHandler(logging.FileHandler(args.log))
        pipeline_logger.addHandler(logging.FileHandler(args.log))
    else:
        logger.addHandler(logging.StreamHandler())
        pipeline_logger.addHandler(logging.StreamHandler())
    if args.async_:
        asyncio.run(pipeline_demo_pq_async(args.nrows, pipeline_logger))
    else:
        pipeline_demo_pq(args.nrows, pipeline_logger)


if __name__ == "__main__":
    main()
