import asyncio
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
