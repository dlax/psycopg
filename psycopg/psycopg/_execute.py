"""
psycopg query execution logic.
"""

# Copyright (C) 2023 The Psycopg Team


from dataclasses import dataclass, field
from functools import partial
from typing import Optional, TYPE_CHECKING

from . import pq
from . import errors as e
from .abc import PQGen
from .generators import execute
from ._encodings import pgconn_encoding
from ._pipeline import BasePipeline

if TYPE_CHECKING:
    from .pq.abc import PGconn, PGresult


TEXT = pq.Format.TEXT

OK = pq.ConnStatus.OK
BAD = pq.ConnStatus.BAD

COMMAND_OK = pq.ExecStatus.COMMAND_OK
TUPLES_OK = pq.ExecStatus.TUPLES_OK
FATAL_ERROR = pq.ExecStatus.FATAL_ERROR

IDLE = pq.TransactionStatus.IDLE


@dataclass
class Executor:

    pgconn: "PGconn"

    pipeline: Optional[BasePipeline] = field(default=None, init=False)
    # Number of transaction blocks currently entered
    num_transactions: int = field(default=0, init=False)

    def exec_command(
        self, command: bytes, result_format: pq.Format = TEXT
    ) -> PQGen[Optional["PGresult"]]:
        """
        Generator to send a command and receive the result to the backend.

        Only used to implement internal commands such as "commit", with eventual
        arguments bound client-side.
        """
        if self.pipeline:
            cmd = partial(
                self.pgconn.send_query_params,
                command,
                None,
                result_format=result_format,
            )
            self.pipeline.command_queue.append(cmd)
            self.pipeline.result_queue.append(None)
            return None

        self.pgconn.send_query_params(command, None, result_format=result_format)

        result = (yield from execute(self.pgconn))[-1]
        if result.status != COMMAND_OK and result.status != TUPLES_OK:
            if result.status == FATAL_ERROR:
                raise e.error_from_result(result, encoding=pgconn_encoding(self.pgconn))
            else:
                raise e.InterfaceError(
                    f"unexpected result {pq.ExecStatus(result.status).name}"
                    f" from command {command.decode()!r}"
                )
        return result

    def check_intrans_gen(self, attribute: str) -> PQGen[None]:
        # Raise an exception if we are in a transaction
        status = self.pgconn.transaction_status
        if status == IDLE and self.pipeline:
            yield from self.pipeline._sync_gen()
            status = self.pgconn.transaction_status
        if status != IDLE:
            if self.num_transactions:
                raise e.ProgrammingError(
                    f"can't change {attribute!r} now: "
                    "connection.transaction() context in progress"
                )
            else:
                raise e.ProgrammingError(
                    f"can't change {attribute!r} now: "
                    "connection in transaction status "
                    f"{pq.TransactionStatus(status).name}"
                )
