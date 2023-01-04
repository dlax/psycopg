import pytest

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types import TypeInfo
from psycopg.types.composite import CompositeInfo
from psycopg.types.enum import EnumInfo
from psycopg.types.multirange import MultirangeInfo
from psycopg.types.range import RangeInfo


@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
def test_fetch(conn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        conn.execute("select 1")

    assert conn.info.transaction_status == status
    info = TypeInfo.fetch(conn, name)
    assert conn.info.transaction_status == status

    assert info.name == "text"
    # TODO: add the schema?
    # assert info.schema == "pg_catalog"

    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid
    assert info.regtype == "text"


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["text", sql.Identifier("text")])
@pytest.mark.parametrize("status", ["IDLE", "INTRANS"])
async def test_fetch_async(aconn, name, status):
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        await aconn.execute("select 1")

    assert aconn.info.transaction_status == status
    info = await TypeInfo.fetch(aconn, name)
    assert aconn.info.transaction_status == status

    assert info.name == "text"
    # assert info.schema == "pg_catalog"
    assert info.oid == psycopg.adapters.types["text"].oid
    assert info.array_oid == psycopg.adapters.types["text"].array_oid


_name = pytest.mark.parametrize("name", ["nosuch", sql.Identifier("nosuch")])
_status = pytest.mark.parametrize("status", ["IDLE", "INTRANS"])


def _fetch_not_found(conn, name, status, monkeypatch, has_to_regtype, info_cls):
    exit_orig = psycopg.Transaction.__exit__

    if has_to_regtype:

        def exit(self, exc_type, exc_val, exc_tb):
            assert exc_val is None
            return exit_orig(self, exc_type, exc_val, exc_tb)

        monkeypatch.setattr(psycopg.Transaction, "__exit__", exit)
    else:
        monkeypatch.setattr(TypeInfo, "_has_to_regtype_function", lambda _: False)
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        conn.execute("select 1")

    assert conn.info.transaction_status == status
    info = info_cls.fetch(conn, name)
    assert conn.info.transaction_status == status
    assert info is None


@_name
@_status
@pytest.mark.crdb_skip("to_regtype")
def test_fetch_not_found_TypeInfo_to_regtype(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, True, TypeInfo)


@_name
@_status
def test_fetch_not_found_TypeInfo_cast(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, False, TypeInfo)


@_name
@_status
@pytest.mark.crdb_skip("range")
def test_fetch_not_found_RangeInfo(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, True, RangeInfo)


@_name
@_status
@pytest.mark.crdb_skip("range")
@pytest.mark.pg(">= 14")
def test_fetch_not_found_MultirangeInfo(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, True, MultirangeInfo)


@_name
@_status
@pytest.mark.crdb_skip("composite")
def test_fetch_not_found_CompositeInfo(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, True, CompositeInfo)


@_name
@_status
@pytest.mark.crdb_skip("to_regtype")
def test_fetch_not_found_EnumInfo_to_regtype(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, True, EnumInfo)


@_name
@_status
def test_fetch_not_found_EnumInfo_cast(conn, name, status, monkeypatch):
    _fetch_not_found(conn, name, status, monkeypatch, False, EnumInfo)


async def _fetch_not_found_async(
    aconn, name, status, monkeypatch, has_to_regtype, info_cls
):
    exit_orig = psycopg.AsyncTransaction.__aexit__

    if has_to_regtype:

        async def aexit(self, exc_type, exc_val, exc_tb):
            assert exc_val is None
            return await exit_orig(self, exc_type, exc_val, exc_tb)

        monkeypatch.setattr(psycopg.AsyncTransaction, "__aexit__", aexit)
    else:
        monkeypatch.setattr(TypeInfo, "_has_to_regtype_function", lambda _: False)
    status = getattr(TransactionStatus, status)
    if status == TransactionStatus.INTRANS:
        await aconn.execute("select 1")

    assert aconn.info.transaction_status == status
    info = await info_cls.fetch(aconn, name)
    assert aconn.info.transaction_status == status

    assert info is None


@pytest.mark.asyncio
@_name
@_status
@pytest.mark.crdb_skip("to_regtype")
async def test_fetch_not_found_TypeInfo_to_regtype_async(
    aconn, name, status, monkeypatch
):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, True, TypeInfo)


@pytest.mark.asyncio
@_name
@_status
async def test_fetch_not_found_TypeInfo_cast_async(aconn, name, status, monkeypatch):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, False, TypeInfo)


@pytest.mark.asyncio
@_name
@_status
@pytest.mark.crdb_skip("range")
async def test_fetch_not_found_RangeInfo_async(aconn, name, status, monkeypatch):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, True, RangeInfo)


@pytest.mark.asyncio
@_name
@_status
@pytest.mark.crdb_skip("range")
@pytest.mark.pg(">= 14")
async def test_fetch_not_found_MultirangeInfo_async(aconn, name, status, monkeypatch):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, True, MultirangeInfo)


@pytest.mark.asyncio
@_name
@_status
@pytest.mark.crdb_skip("composite")
async def test_fetch_not_found_CompositeInfo_async(aconn, name, status, monkeypatch):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, True, CompositeInfo)


@pytest.mark.asyncio
@_name
@_status
@pytest.mark.crdb_skip("to_regtype")
async def test_fetch_not_found_EnumInfo_to_regtype_async(
    aconn, name, status, monkeypatch
):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, True, EnumInfo)


@pytest.mark.asyncio
@_name
@_status
async def test_fetch_not_found_EnumInfo_cast_async(aconn, name, status, monkeypatch):
    await _fetch_not_found_async(aconn, name, status, monkeypatch, False, EnumInfo)


@pytest.mark.crdb_skip("composite")
@pytest.mark.parametrize(
    "name", ["testschema.testtype", sql.Identifier("testschema", "testtype")]
)
def test_fetch_by_schema_qualified_string(conn, name):
    conn.execute("create schema if not exists testschema")
    conn.execute("create type testschema.testtype as (foo text)")

    info = TypeInfo.fetch(conn, name)
    assert info.name == "testtype"
    # assert info.schema == "testschema"
    cur = conn.execute(
        """
        select oid, typarray from pg_type
        where oid = 'testschema.testtype'::regtype
        """
    )
    assert cur.fetchone() == (info.oid, info.array_oid)


@pytest.mark.parametrize(
    "name",
    [
        "text",
        # TODO: support these?
        # "pg_catalog.text",
        # sql.Identifier("text"),
        # sql.Identifier("pg_catalog", "text"),
    ],
)
def test_registry_by_builtin_name(conn, name):
    info = psycopg.adapters.types[name]
    assert info.name == "text"
    assert info.oid == 25


def test_registry_empty():
    r = psycopg.types.TypesRegistry()
    assert r.get("text") is None
    with pytest.raises(KeyError):
        r["text"]


@pytest.mark.parametrize("oid, aoid", [(1, 2), (1, 0), (0, 2), (0, 0)])
def test_registry_invalid_oid(oid, aoid):
    r = psycopg.types.TypesRegistry()
    ti = psycopg.types.TypeInfo("test", oid, aoid)
    r.add(ti)
    assert r["test"] is ti
    if oid:
        assert r[oid] is ti
    if aoid:
        assert r[aoid] is ti
    with pytest.raises(KeyError):
        r[0]


def test_registry_copy():
    r = psycopg.types.TypesRegistry(psycopg.postgres.types)
    assert r.get("text") is r["text"] is r[25]
    assert r["text"].oid == 25


def test_registry_isolated():
    orig = psycopg.postgres.types
    tinfo = orig["text"]
    r = psycopg.types.TypesRegistry(orig)
    tdummy = psycopg.types.TypeInfo("dummy", tinfo.oid, tinfo.array_oid)
    r.add(tdummy)
    assert r[25] is r["dummy"] is tdummy
    assert orig[25] is r["text"] is tinfo
