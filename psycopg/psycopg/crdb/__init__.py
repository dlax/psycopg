"""
CockroachDB support package.
"""

# Copyright (C) 2022 The Psycopg Team

from . import _types
from .connection import CrdbConnection, AsyncCrdbConnection, CrdbConnectionInfo

try:
    from .connection import AnyIOCrdbConnection  # noqa: F401
except ImportError:
    _anyio = False
else:
    _anyio = True

adapters = _types.adapters  # exposed by the package
connect = CrdbConnection.connect

_types.register_crdb_adapters(adapters)

__all__ = [
    "AsyncCrdbConnection",
    "CrdbConnection",
    "CrdbConnectionInfo",
]
if _anyio:
    __all__.append("AnyIOCrdbConnection")
