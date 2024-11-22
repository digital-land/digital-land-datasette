import json
import os
import boto3
import logging
from pathlib import Path

def view_for(view_name, fname, glob):
    view_name = view_name.replace('.', '_')
    if fname.endswith(('.csv', '.tsv')):
        return "CREATE VIEW \"{}\" AS SELECT * FROM read_csv_auto('{}', header=true)".format(view_name, glob)
    elif fname.endswith('.parquet'):
        return "CREATE VIEW \"{}\" AS SELECT * FROM '{}'".format(view_name, glob)
    elif fname.endswith(('.ndjson', '.jsonl')):
        return "CREATE VIEW \"{}\" AS SELECT * FROM read_ndjson_auto('{}')".format(view_name, glob)

# def view_for_combined(view_name, file_list):
#     # Create a UNION ALL query for all files in file_list
#     return f"CREATE VIEW \"{view_name}\" AS " + " UNION ALL ".join(
#         [f"SELECT * FROM read_parquet('{file}')" for file in file_list]
#     )

def view_for_parquet(view_name, fname, glob):
    view_name = view_name.replace('.', '_')
    return "CREATE VIEW \"{}\" AS SELECT * FROM '{}'".format(view_name, glob)

def create_s3_client():
    env_endpoint_url = os.getenv("AWS_ENDPOINT_URL")
    if env_endpoint_url:
        s3 = boto3.client('s3', endpoint_url=env_endpoint_url)
    else:
        s3 = boto3.client('s3')
    return s3

def ensure_trailing_slash(path):
    if not path.endswith('/'):
        path += '/'
    return path

def create_views(dirname,httpfs,db_name):
    """
    funcction to create a list of views for a parquet based database
    as tables do not exist, instead we create views that can be queried rather 
    than having to know the structure of the url
    """
    view_list = []
    
    if httpfs: 
        # Parse the bucket name and prefix
        bucket_name = dirname.split('/')[2]
        prefix = '/'.join(dirname.split('/')[3:])
        s3 = create_s3_client()
        
        # List all .parquet files in the specified bucket and prefix
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=ensure_trailing_slash(prefix),Delimiter='/')
            if 'Contents' not in response and 'CommonPrefixes' not in response:
                logging.error(f"No files found in the specified bucket/prefix: {bucket_name}/{prefix}")
                return view_list
        except Exception as e:
            print(f"Error listing objects in S3 bucket: {e}")
            return view_list
        
        # by using the delimeter above and filtering on the directory
        # we get files listed in the contents setion and directories
        # in common prrefixes. Either could be empty
        keys = []

        contents = response.get('Contents')
        if contents:
            for obj in contents:
                keys.append(obj['Key'])
       

        # check for Common prrefixes
        common_prefixes = response.get('CommonPrefixes')
        if common_prefixes:
            for prefix  in common_prefixes:
                keys.append(prefix['Prefix'])

        # create a view for each key in the top level of the bucket
        for key in keys:
            key_path = Path(key)
            if key_path.suffix:
                view_list.append(view_for(key_path.stem, '.parquet', key_path))
                
            else:
                view_list.append(view_for(key_path.stem, '.parquet', key_path / '**/*.parquet'))
        
        # Use the directory name as the view name
        
    else:
    # Add in sorted order so the user sees alphabetically stable sort
        for f in sorted(os.scandir(dirname), key=lambda x: x.path):
            fname = f.path
            if f.is_dir():
                files = list(os.scandir(f.path))

                if not files:
                    continue

                # We only sniff the first file, we assume all files in the directory
                # will have the same extension and shape. YOLO.
                file = files[0]
                view_list.append(view_for(Path(fname).stem, file.path, os.path.join(fname, '*' + Path(file).suffix)))
            else:
                view_list.append(view_for(Path(fname).stem, fname, fname))

    return [x for x in view_list if x]