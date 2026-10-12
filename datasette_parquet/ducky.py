import asyncio
import duckdb
from .debounce import debounce
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from datasette.database import Database, Results
from .ddl import create_views
from .winging_it import ProxyConnection

class SchemaEventHandler(FileSystemEventHandler):
    """React to files being added/removed from the watched directory."""

    def __init__(self, reload):
        super().__init__()

        self.reload = reload

    @debounce(1)
    def on_event(self):
        self.reload()

    def on_moved(self, event):
        super().on_moved(event)
        self.on_event()

    def on_created(self, event):
        super().on_created(event)
        self.on_event()

    def on_deleted(self, event):
        super().on_deleted(event)
        self.on_event()

    def on_modified(self, event):
        super().on_modified(event)
        self.on_event()

def connection_config(settings):
    # persistent secrets live in the home directory, which is off-limits once the
    # database is locked down
    return {'allow_persistent_secrets': False, **(settings or {})}

def run_setup(raw_conn, setup_sql):
    # e.g. LOAD httpfs, or CREATE SECRET for S3 - these have to run before lock_down
    for statement in setup_sql or []:
        raw_conn.execute(statement)

def lock_down(raw_conn, allowed_directories):
    # Anyone can run a SELECT through Datasette, and a DuckDB SELECT can read local
    # files and URLs, e.g. read_text('/proc/self/environ'). Switch off external access
    # apart from allowed_directories, then lock the configuration so SQL can't undo it.
    # Nothing in run_setup is possible once this has run.
    raw_conn.execute('SET allowed_directories = ?', [list(allowed_directories or [])])
    raw_conn.execute('SET enable_external_access = false')
    raw_conn.execute('SET lock_configuration = true')

def create_directory_connection(directory, settings=None, setup_sql=None, allowed_directories=None):
    raw_conn = duckdb.connect(config=connection_config(settings))
    run_setup(raw_conn, setup_sql)
    conn = ProxyConnection(raw_conn)

    for create_view_stmt in create_views(directory):
        conn.conn.execute(create_view_stmt)

    lock_down(raw_conn, [directory] + list(allowed_directories or []))
    return conn

class DuckDatabase(Database):
    def __init__(self, ds, directory=None, file=None, watch=None, settings=None, setup_sql=None, allowed_directories=None):
        # settings are DuckDB configuration options, e.g. memory_limit, threads and
        # temp_directory. They apply to the whole database, so every per-query
        # connection inherits them. Without them DuckDB uses up to 80% of the machine's
        # memory and every core.
        super().__init__(ds)

        self.engine = 'duckdb'

        if directory:
            conn = create_directory_connection(directory, settings, setup_sql, allowed_directories)

            def reload():
                self.conn.conn.close()
                self.conn = create_directory_connection(directory, settings, setup_sql, allowed_directories)

            event_handler = SchemaEventHandler(reload)
            observer = Observer()
            observer.schedule(event_handler, directory, recursive=True)
            observer.start()
        elif file:
            raw_conn = duckdb.connect(file, read_only=True, config=connection_config(settings))
            run_setup(raw_conn, setup_sql)
            lock_down(raw_conn, allowed_directories)
            conn = ProxyConnection(raw_conn)
        else:
            raise Exception('must specify directory or file')

        self.conn = conn

    @property
    def size(self):
        # TODO: implement this? Not sure if it's useful.
        return 0

    async def execute_fn(self, fn):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            # A DuckDB connection can't be shared between threads, so give each call its
            # own. This also lets a query timeout interrupt just the query that ran over.
            conn = self.conn.for_query()
            try:
                return fn(conn)
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    async def execute_write_fn(self, fn, block=True):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            conn = self.conn.for_query()
            try:
                return fn(conn)
            finally:
                conn.close()


        # We lie, we'll always block.
        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

