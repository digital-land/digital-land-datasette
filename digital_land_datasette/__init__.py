from datasette import hookimpl
import logging
PLUGIN_NAME = 'digital-land-datasette'

@hookimpl
def startup(datasette):
    config = datasette.plugin_config(
        PLUGIN_NAME
    )
    datasette._settings["suggest_facets"] = False
    if not config:
        return

    from .ducky import DuckDatabase
    from .patches import monkey_patch

    monkey_patch()

    for db_name, options in config.items():
        try:
            if not 'directory' in options and not 'file' in options:
                raise Exception('digital-land-datasette: expected directory or file key for db {}'.format(db))

            if 'directory' in options:
                directory = options['directory']
                db = DuckDatabase(datasette, directory=directory,httpfs = options['httpfs'], watch=options.get('watch', False) == True, db_name=db_name)
                datasette.add_database(db, db_name)
            else:
                file = options['file']
                db = DuckDatabase(datasette, file=file,httpfs = options['httpfs'])
                datasette.add_database(db, db_name)
        except Exception as e:
            logging.error(f"digital_land_datasette raised error {e}")

