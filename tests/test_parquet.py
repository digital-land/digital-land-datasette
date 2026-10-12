import asyncio
import sqlite3
import time
from datasette.app import Datasette
from .create_db import create_dbs
import pytest
import duckdb
from datasette_parquet.winging_it import ProxyConnection, fixup_params
from datasette_parquet import exceptions

@pytest.fixture(scope="session")
def datasette():
    create_dbs('./fixtures')
    metadata = {
        'plugins': {
            'datasette-parquet': {
                'trove': {
                    'directory': './fixtures'
                },
                'duckdb': {
                    'file': './fixtures/fixtures.duckdb'
                },
                'limited': {
                    'directory': './fixtures',
                    'settings': {'memory_limit': '64MiB', 'threads': 1},
                    'setup_sql': ['CREATE TABLE setup_marker AS SELECT 42 AS answer']
                }

            }
        }
    }

    return Datasette(
        [],
        memory=True,
        metadata=metadata,
    )

@pytest.mark.asyncio
async def test_plugin_is_installed(datasette):
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-parquet" in installed_plugins

@pytest.mark.asyncio
async def test_file_mode(datasette):
    response = await datasette.client.get('/duckdb')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_directory_mode(datasette):
    response = await datasette.client.get('/trove')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_json_works(datasette):
    response = await datasette.client.get("/trove/fixtures.json?_size=max&_labels=on&_shape=objects")
    assert response.status_code == 200
    assert response.json()['rows'] == [{'date': '2023-01-01', 'ts': '2023-01-02T03:04:05' }]

@pytest.mark.asyncio
async def test_extraneous_parameters(datasette):
    response = await datasette.client.get("/trove?sql=select+%2A+from+fixtures&_hide_sql=1")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_sql_json(datasette):
    response = await datasette.client.get("/trove.json?sql=select+%2A+from+fixtures&_hide_sql=1")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_duckdb_table(datasette):
    response = await datasette.client.get("/duckdb/fixtures")
    assert response.status_code == 200

def test_fetchone():
    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)
    fetched = conn.execute('SELECT 1 AS col').fetchone()
    assert fetched['col'] == 1


@pytest.mark.asyncio
def test_catch_double_quote_usage_for_literal(datasette):

    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)

    # try reading the parquet file in trove/userdata1.parquet
    explodey_string_with_double_quotes = 'SELECT * from "./trove/userdata1.parquet" WHERE first_name = "Amanda"'

    with pytest.raises(exceptions.DoubleQuoteForLiteraValue):
        result = conn.execute(explodey_string_with_double_quotes).fetchall()

def test_fixup_params_binds_dollar_placeholders_by_name():
    # newer sqlglot rewrites Datasette's :n to $n
    sql, params = fixup_params('SELECT sql FROM sqlite_master WHERE name = $n AND type = $t', {'n': 'fixtures', 't': 'view'})
    assert sql == 'SELECT sql FROM sqlite_master WHERE name = $n AND type = $t'
    assert params == {'n': 'fixtures', 't': 'view'}


def test_fixup_params_converts_colon_placeholders():
    sql, params = fixup_params('SELECT * FROM t WHERE a = :a', {'a': 1})
    assert sql == 'SELECT * FROM t WHERE a = $a'
    assert params == {'a': 1}


def test_fixup_params_drops_unused_parameters():
    sql, params = fixup_params('SELECT * FROM t WHERE a = $a', {'a': 1, '_hide_sql': '1'})
    assert params == {'a': 1}


def test_fixup_params_does_not_match_name_prefixes():
    # $n must not be treated as a use of $name, or vice versa
    sql, params = fixup_params('SELECT * FROM t WHERE x = $name', {'n': 1, 'name': 2})
    assert params == {'name': 2}


def test_fixup_params_without_parameters():
    assert fixup_params('SELECT 1', None) == ('SELECT 1', [])
    assert fixup_params('SELECT 1', {}) == ('SELECT 1', [])


@pytest.mark.asyncio
async def test_each_call_gets_its_own_duckdb_connection(datasette):
    db = datasette.get_database('duckdb')
    seen = []
    await db.execute_fn(lambda conn: seen.append(conn.conn))
    await db.execute_fn(lambda conn: seen.append(conn.conn))
    assert seen[0] is not seen[1]
    assert db.conn.conn not in seen


@pytest.mark.asyncio
async def test_concurrent_requests_all_succeed(datasette):
    # a shared DuckDB connection used from several threads at once fails intermittently
    paths = ["/duckdb", "/duckdb/fixtures", "/trove/fixtures.json"] * 30
    responses = await asyncio.gather(*[datasette.client.get(p) for p in paths])
    assert [r.status_code for r in responses] == [200] * len(paths)


def test_time_limit_interrupts_a_long_duckdb_query():
    conn = ProxyConnection(duckdb.connect(), per_query=True)
    deadline = time.perf_counter() + 0.2
    conn.set_progress_handler(lambda: time.perf_counter() >= deadline, 1000)
    started = time.perf_counter()

    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        conn.cursor().execute("SELECT sum(i) FROM range(1000000000000) t(i)")

    conn.close()
    assert time.perf_counter() - started < 5


def test_queries_without_a_time_limit_are_not_interrupted():
    conn = ProxyConnection(duckdb.connect(), per_query=True)
    conn.set_progress_handler(lambda: False, 1000)
    rows = conn.cursor().execute("SELECT sum(i) AS total FROM range(1000000) t(i)").fetchall()
    conn.set_progress_handler(None, 1000)
    conn.close()
    assert rows[0][0] == 499999500000


@pytest.mark.asyncio
async def test_datasette_reports_a_long_query_as_too_long(datasette):
    # Datasette's default sql_time_limit_ms is 1000
    started = time.perf_counter()
    response = await datasette.client.get(
        "/duckdb.json?sql=select+sum(i)+from+range(1000000000000)+t(i)"
    )
    assert response.status_code == 400
    assert response.json()["title"] == "SQL Interrupted"
    assert time.perf_counter() - started < 10


@pytest.mark.asyncio
async def test_settings_apply_to_every_query_connection(datasette):
    db = datasette.get_database('limited')
    memory_limit, threads = await db.execute_fn(
        lambda conn: conn.execute("SELECT current_setting('memory_limit'), current_setting('threads')").fetchone()
    )
    assert memory_limit == '64.0 MiB'
    assert threads == 1


@pytest.mark.asyncio
async def test_memory_limit_stops_a_query_that_needs_more(datasette):
    db = datasette.get_database('limited')
    with pytest.raises(duckdb.OutOfMemoryException):
        await db.execute_fn(
            lambda conn: conn.execute("SELECT list(i) FROM range(200000000) t(i)").fetchall()
        )


@pytest.mark.asyncio
async def test_select_cannot_read_local_files(datasette):
    for name in ('duckdb', 'trove'):
        db = datasette.get_database(name)
        with pytest.raises(duckdb.PermissionException):
            await db.execute_fn(
                lambda conn: conn.execute("SELECT content FROM read_text('tests/test_parquet.py')").fetchall()
            )


@pytest.mark.asyncio
async def test_select_cannot_read_urls(datasette):
    db = datasette.get_database('duckdb')
    with pytest.raises(duckdb.PermissionException):
        await db.execute_fn(
            lambda conn: conn.execute("SELECT content FROM read_text('https://example.com/')").fetchall()
        )


@pytest.mark.asyncio
async def test_sql_cannot_change_the_configuration(datasette):
    db = datasette.get_database('duckdb')
    with pytest.raises(duckdb.InvalidInputException, match="configuration has been locked"):
        await db.execute_fn(lambda conn: conn.execute("SET enable_external_access = true"))


@pytest.mark.asyncio
async def test_setup_sql_runs_before_lockdown(datasette):
    response = await datasette.client.get("/limited.json?sql=select+answer+from+setup_marker&_shape=array")
    assert response.status_code == 200
    assert response.json() == [{'answer': 42}]
