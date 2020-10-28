import pytest

from psycopg3 import DatabaseError, sql
from psycopg3.adapt import Format

eur = "\u20ac"


#
# tests with text
#


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_1char(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    for i in range(1, 256):
        cur.execute(f"select {ph} = chr(%s::int)", (chr(i), i))
        assert cur.fetchone()[0] is True, chr(i)


def test_quote_1char(conn):
    cur = conn.cursor()
    query = sql.SQL("select {ch} = chr(%s::int)")
    for i in range(1, 256):
        if chr(i) == "%":
            continue
        cur.execute(query.format(ch=sql.Literal(chr(i))), (i,))
        assert cur.fetchone()[0] is True, chr(i)


# the only way to make this pass is to reduce %% -> % every time
# not only when there are query arguments
# see https://github.com/psycopg/psycopg2/issues/825
@pytest.mark.xfail
def test_quote_percent(conn):
    cur = conn.cursor()
    cur.execute(sql.SQL("select {ch}").format(ch=sql.Literal("%")))
    assert cur.fetchone()[0] == "%"

    cur.execute(
        sql.SQL("select {ch} = chr(%s::int)").format(ch=sql.Literal("%")),
        (ord("%"),),
    )
    assert cur.fetchone()[0] is True


@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_1char(conn, typename, fmt_out):
    cur = conn.cursor(format=fmt_out)
    for i in range(1, 256):
        cur.execute(f"select chr(%s::int)::{typename}", (i,))
        res = cur.fetchone()[0]
        assert res == chr(i)

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
def test_dump_enc(conn, fmt_in, encoding):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.client_encoding = encoding
    (res,) = cur.execute(f"select {ph}::bytea", (eur,)).fetchone()
    assert res == eur.encode("utf8")


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_ascii(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.client_encoding = "sql_ascii"
    (res,) = cur.execute(f"select ascii({ph})", (eur,)).fetchone()
    assert res == ord(eur)


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_badenc(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"

    conn.client_encoding = "latin1"
    with pytest.raises(UnicodeEncodeError):
        cur.execute(f"select {ph}::bytea", (eur,))


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("encoding", ["utf8", "latin9"])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_enc(conn, typename, encoding, fmt_out):
    cur = conn.cursor(format=fmt_out)

    conn.client_encoding = encoding
    (res,) = cur.execute(
        f"select chr(%s::int)::{typename}", (ord(eur),)
    ).fetchone()
    assert res == eur


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_load_badenc(conn, typename, fmt_out):
    cur = conn.cursor(format=fmt_out)

    conn.client_encoding = "latin1"
    with pytest.raises(DatabaseError):
        cur.execute(f"select chr(%s::int)::{typename}", (ord(eur),))


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar"])
def test_load_ascii(conn, typename, fmt_out):
    cur = conn.cursor(format=fmt_out)

    conn.client_encoding = "sql_ascii"
    (res,) = cur.execute(
        f"select chr(%s::int)::{typename}", (ord(eur),)
    ).fetchone()
    assert res == eur.encode("utf8")


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("typename", ["name", "bpchar"])
def test_load_ascii_encanyway(conn, typename, fmt_out):
    cur = conn.cursor(format=fmt_out)

    conn.client_encoding = "sql_ascii"
    (res,) = cur.execute(f"select 'aa'::{typename}").fetchone()
    assert res == "aa"


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("typename", ["text", "varchar", "name", "bpchar"])
def test_text_array(conn, typename, fmt_in, fmt_out):
    cur = conn.cursor(format=fmt_out)
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    a = list(map(chr, range(1, 256))) + [eur]

    (res,) = cur.execute(f"select {ph}::{typename}[]", (a,)).fetchone()
    assert res == a


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_text_array_ascii(conn, fmt_in, fmt_out):
    conn.client_encoding = "sql_ascii"
    cur = conn.cursor(format=fmt_out)
    a = list(map(chr, range(1, 256))) + [eur]
    exp = [s.encode("utf8") for s in a]
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    (res,) = cur.execute(f"select {ph}::text[]", (a,)).fetchone()
    assert res == exp


#
# tests with bytea
#


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
def test_dump_1byte(conn, fmt_in):
    cur = conn.cursor()
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    for i in range(0, 256):
        cur.execute(f"select {ph} = %s::bytea", (bytes([i]), fr"\x{i:02x}"))
        assert cur.fetchone()[0] is True, i


def test_quote_1byte(conn):
    cur = conn.cursor()
    query = sql.SQL("select {ch} = %s::bytea")
    for i in range(0, 256):
        cur.execute(query.format(ch=sql.Literal(bytes([i]))), (fr"\x{i:02x}",))
        assert cur.fetchone()[0] is True, i


@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_load_1byte(conn, fmt_out):
    cur = conn.cursor(format=fmt_out)
    for i in range(0, 256):
        cur.execute("select %s::bytea", (fr"\x{i:02x}",))
        assert cur.fetchone()[0] == bytes([i])

    assert cur.pgresult.fformat(0) == fmt_out


@pytest.mark.parametrize("fmt_in", [Format.TEXT, Format.BINARY])
@pytest.mark.parametrize("fmt_out", [Format.TEXT, Format.BINARY])
def test_bytea_array(conn, fmt_in, fmt_out):
    cur = conn.cursor(format=fmt_out)
    a = [bytes(range(0, 256))]
    ph = "%s" if fmt_in == Format.TEXT else "%b"
    (res,) = cur.execute(f"select {ph}::bytea[]", (a,)).fetchone()
    assert res == a
