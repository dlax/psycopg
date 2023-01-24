"""
psycopg Future objects holding query execution results
"""

# Copyright (C) 2023 The Psycopg Team

from concurrent import futures
from typing import Any, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .abc import PrepareKey
    from .cursor import BaseCursor
    from .pq.abc import PGresult


class Future(futures.Future[List["PGresult"]]):
    def __init__(
        self, obj: Optional[Tuple["BaseCursor[Any, Any]", Optional["PrepareKey"]]]
    ) -> None:
        self.obj = obj
        super().__init__()

    def set_result(self, results: List["PGresult"]) -> None:
        super().set_result(results)
        if self.obj is not None:
            cursor = self.obj[0]
            cursor._set_results(results)


def create_future(
    obj: Optional[Tuple["BaseCursor[Any, Any]", Optional["PrepareKey"]]] = None
) -> Future:
    return Future(obj)
