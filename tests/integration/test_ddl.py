from digital_land_datasette.ddl import create_views

import logging
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


@pytest.mark.parametrize("keys, count",
    [
        (
            [
                'log/data_1.parquet',
                'log/data_2/city=new-york/data_1.parquet'
            ],
            2
        ),
        (
            [
                'log/data_2/city=new-york/data_1.parquet'
            ],
            1
        )
    ]
)
def test_create_views_creates_from_s3_directory(keys,count,s3_client,tmp_path,mocker):
    """
    A test to ensure parquet filles se are picked up at the moment httpfs
    is used as an indicator for it being in s3
    """
    httpfs = True
   
    db_name = "log"
    
    # will use the same datafor both files
    bucket_name = 'collection_data'
    
    data = [
        {"name": "Alice", "age": 25, "city": "new-york"},
        {"name": "Bob", "age": 30, "city": "new-york"},
        {"name": "Charlie", "age": 35, "city": "new-york"}
    ]

    file_path = tmp_path / 'data_1.parquet'

    table = pa.Table.from_pandas(pd.DataFrame(data))
    pq.write_table(table, file_path)


    s3_client.create_bucket(Bucket=bucket_name)

    "load a data file in for all keys supplied"
    for key in keys:
        s3_client.upload_file(str(file_path),bucket_name,key)

    # create and store data in the testing bucket
    dirname = f"s3://{bucket_name}/log/"
    views = create_views(dirname,httpfs,db_name)

    # mock the client in the function
    mocker.patch('digital_land_datasette.ddl.create_s3_client',return_value=s3_client)

    logging.debug(views)
    assert len(views) == count, f"expected 2 views to be generated fromm s3 bucket but only {len(views)} created"
    # all filles are parquet so parquet should be in all views
    for view in views:
        assert '.parquet' in view