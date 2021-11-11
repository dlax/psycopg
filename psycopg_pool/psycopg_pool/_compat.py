"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

import sys


if sys.version_info >= (3, 7):
    from contextlib import asynccontextmanager
else:

    def asynccontextmanager(func):
        def helper(*args, **kwds):
            raise NotImplementedError(
                "async pool not implemented on Python 3.6"
            )

        return helper


if sys.version_info >= (3, 9):
    from collections import Counter, deque as Deque
else:
    from typing import Counter, Deque

__all__ = [
    "Counter",
    "Deque",
    "asynccontextmanager",
]
