import contextlib
import sqlite3
import threading
import time
import re
import typing

from duckdb import BinderException, InterruptException

from .rewrite import rewrite, NO_OP_SQL
from . import exceptions


# a regular expression to find the literal wrapped by the double quoted string
# GOOD for DuckDB, works fine:  
# `WHERE column = 'some value'` 
# 
# BAD for DuckDB, triggers duckdb.BinderException:
# `WHERE column = "some value"` 
DOUBLE_QUOTES_AROUND_LITERALS_PATTERN = r'(?<![A-Za-z0-9_])"(?:[^"]*(?:"[^"]*)*[^"]*)"(?![A-Za-z0-9_])' # noqa

def find_matching_double_quote_usage(ex: BinderException) -> typing.List[str]:
    """
    Accepts a BinderException from DuckDB, and returns a list of
    matching strings in double quotes
    """
    # should return 'Binder Error: Referenced column "LITERAL_IN_DOUBLE_QUOTES" '
    referenced_column_message = ex.args[0].split(' not found in FROM clause')[0]
    pattern = DOUBLE_QUOTES_AROUND_LITERALS_PATTERN

    # find the offending values wrapped in double quotes
    matches = re.findall(pattern, referenced_column_message)
    return matches


# A collection of classes to provide a facade that mimics the sqlite3 DB-API
# interface.
class Row:
    def __init__(self, columns, tpl):
        self.columns = columns
        self.tpl = tpl

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.tpl[key]
        else:
            return self.tpl[self.columns[key]]

@contextlib.contextmanager
def interrupted_as_sqlite_error():
    # Datasette only recognises a query that hit its time limit by sqlite3's
    # "interrupted" error, so translate DuckDB's equivalent into that
    try:
        yield
    except InterruptException as ex:
        raise sqlite3.OperationalError("interrupted") from ex

def fixup_params(sql, parameters):
    # Sometimes we skip queries that DuckDB can't handle, eg DATE(...) facet queries.
    # If the old query had parameters, sending them with the new query will
    # cause an assertion to fail. So return an empty list of parameters.
    if sql == NO_OP_SQL:
        return sql, []

    if isinstance(parameters, (tuple, list)):
        return sql, parameters

    # Datasette uses named :param placeholders. Newer sqlglot rewrites them to
    # DuckDB's $param form while older versions leave them as :param, so accept
    # both and bind by name. On the custom SQL page Datasette passes every query
    # string parameter, and DuckDB rejects unused ones, so keep only those in the SQL.
    used = {}
    for k, v in (parameters or {}).items():
        placeholder = re.compile(r'[:$]{}\b'.format(re.escape(k)))
        if placeholder.search(sql):
            sql = placeholder.sub('$' + k, sql)
            used[k] = v

    return sql, used or []


class ProxyCursor:
    def __init__(self, conn, existing_cursor=None):
        self.conn = conn

        if existing_cursor:
            self.cursor = existing_cursor
        else:
            self.cursor = self.conn.cursor()

    def execute(self, sql, parameters=None):
        #print('# params={} sql={}'.format(parameters, sql))
        sql = rewrite(sql)
        sql, parameters = fixup_params(sql, parameters)

        #print('## params={} sql={}'.format(parameters, sql))
        t = time.time()
        try:
            with interrupted_as_sqlite_error():
                rv = self.cursor.execute(sql, parameters)

        except BinderException as ex:
            matches = find_matching_double_quote_usage(ex)
            if matches:
                raise exceptions.DoubleQuoteForLiteraValue(matches)
            else:
                # continue raising the original BinderException
                raise

        #print('took {}'.format(time.time() - t))
        return rv

    def fetchone(self):
        tpl = self.cursor.fetchone()

        if not tpl:
            return tpl

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return Row(columns, tpl)

    def fetchmany(self, size=1):
        with interrupted_as_sqlite_error():
            tpls = self.cursor.fetchmany(size)

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return [Row(columns, tpl) for tpl in tpls]

    def fetchall(self):
        with interrupted_as_sqlite_error():
            tpls = self.cursor.fetchall()

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return [Row(columns, tpl) for tpl in tpls]

    def __iter__(self):
        return self

    def __next__(self):
        rv = self.cursor.fetchone()

        if rv == None:
            raise StopIteration

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i
        rv = Row(columns, rv)
        return rv

    def __getattr__(self, name):
        return getattr(self.cursor, name)

class ProxyConnection:
    def __init__(self, conn, per_query=False):
        self.conn = conn
        self.per_query = per_query
        self._stop_watching = None

    def for_query(self):
        # DuckDB's cursor() opens a new connection to the same database, which shares
        # its views and loaded extensions
        return ProxyConnection(self.conn.cursor(), per_query=True)

    def close(self):
        self.set_progress_handler(None, 0)
        self.conn.close()

    def __enter__(self):
        pass

    def __exit__(self, exc_type,exc_value, exc_traceback):
        pass

    def execute(self, sql, parameters=None):
        #print('! params={} sql={}'.format(parameters, sql))
        sql = rewrite(sql)
        #print('! rewritten sql={}'.format(sql))
        sql, parameters = fixup_params(sql, parameters)
        #print('!! params={} sql={}'.format(parameters, sql))
        try:
            rv = self.conn.execute(sql, parameters)
        except BinderException as ex:
            matches = find_matching_double_quote_usage(ex)
            if matches:
                raise exceptions.DoubleQuoteForLiteraValue(matches)
            else:
                # continue raising the original BinderException
                raise
            
        return ProxyCursor(self.conn, rv)

    def fetchall(self):
        raise Exception('TODO: ProxyConnection.fetchall is not implemented')

    def set_progress_handler(self, handler, n):
        # Datasette enforces its SQL time limit through this sqlite3 hook: handler()
        # returns true once the deadline has passed, and Datasette calls this again with
        # None when the query is done. DuckDB has no such hook, so poll the handler from
        # a watcher thread and interrupt this connection's query when it fires.
        if self._stop_watching is not None:
            self._stop_watching.set()
            self._stop_watching = None
        if handler is None:
            return

        stop = threading.Event()
        self._stop_watching = stop

        def watch():
            while not stop.wait(0.05):
                if handler():
                    self.conn.interrupt()
                    return

        threading.Thread(target=watch, daemon=True).start()

    def cursor(self):
        if self.per_query:
            # keep everything Datasette runs for this call on the call's own connection
            return ProxyCursor(self.conn, self.conn)
        return ProxyCursor(self.conn)


