from datasette import hookimpl

PLUGIN_NAME = 'datasette-parquet'

@hookimpl
def startup(datasette):
    config = datasette.plugin_config(
        PLUGIN_NAME
    )

    if not config:
        return

    from .ducky import DuckDatabase
    from .patches import monkey_patch

    monkey_patch()

    for db_name, options in config.items():
        if not 'directory' in options and not 'file' in options:
            raise Exception('datasette-parquet: expected directory or file key for db {}'.format(db_name))

        if 'directory' in options:
            directory = options['directory']
            db = DuckDatabase(datasette, directory=directory, watch=options.get('watch', False) == True, settings=options.get('settings'), setup_sql=options.get('setup_sql'), allowed_directories=options.get('allowed_directories'))
            datasette.add_database(db, db_name)
        else:
            file = options['file']
            db = DuckDatabase(datasette, file=file, settings=options.get('settings'), setup_sql=options.get('setup_sql'), allowed_directories=options.get('allowed_directories'))
            datasette.add_database(db, db_name)

