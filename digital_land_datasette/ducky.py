import asyncio
import duckdb
import logging
import os

from distutils.util import strtobool
from .debounce import debounce
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from datasette.database import Database, Results
from .ddl import create_views
from .winging_it import ProxyConnection

logger = logging.getLogger('__name__')

def get_bool(value,default=None):
    try:
        value = bool(strtobool(value))
        return value
    except ValueError as e:
        if default is not None:
            logging.debug(f'could not convert value to bool assigning default {default}')
            return default
        else:
            raise e

use_aws_credential_chain = get_bool(os.environ.get("USE_AWS_CREDENTIAL_CHAIN",""),True)
def create_duckdb_conn(use_aws_credential_chain=True):
    conn = duckdb.connect()
    logger.debug(conn.execute('INSTALL httpfs;').fetchall())
    logger.debug(conn.execute('LOAD httpfs;').fetchall())
    logger.debug(conn.execute("SET disabled_filesystems = 'LocalFileSystem';").fetchall())
    logger.debug(conn.execute("SET allow_community_extensions = false;").fetchall())
    
    logging.error(use_aws_credential_chain)
    if use_aws_credential_chain:
        logging.error('chain  is activate')

    #     logger.debug(conn.execute("CREATE SECRET aws (TYPE S3, PROVIDER CREDENTIAL_CHAIN);").fetchall())
    #     logger.debug(conn.execute("FROM duckdb_secrets();").fetchall())
    
    # lock configuration so no changes can be made
    logger.debug(conn.execute("SET lock_configuration=TRUE").fetchall())
   
    return conn

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

def create_directory_connection(directory,httpfs,db_name):
    raw_conn = create_duckdb_conn(use_aws_credential_chain)
    conn = ProxyConnection(raw_conn)

    for create_view_stmt in create_views(directory,httpfs,db_name):
        conn.conn.execute(create_view_stmt)

    return conn

class DuckDatabase(Database):
    def __init__(self, ds, directory=None, file=None, httpfs=None, watch=None, db_name=None):
        super().__init__(ds)

        self.engine = 'duckdb'
        self.db_name = db_name
        logger.info(f'make db {db_name} for directory {directory}')
        if directory:
            conn = create_directory_connection(directory,httpfs,db_name)

            def reload():
                self.conn.conn.close()
                self.conn = create_directory_connection(directory,httpfs)


        elif file:
            raw_conn = duckdb.connect()
            conn = ProxyConnection(raw_conn)
            if httpfs:
                conn.conn.execute('install httpfs;').fetchall()
                conn.conn.execute('load httpfs;').fetchall()
                conn.execute(f"CREATE VIEW issue AS SELECT * FROM read_parquet('{self.file}')", []).fetchall()
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
            return fn(self.conn)

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    async def execute_write_fn(self, fn, block=True):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        # We lie, we'll always block.
        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

