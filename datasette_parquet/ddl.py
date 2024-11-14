import os
import boto3
from pathlib import Path

def view_for(view_name, fname, glob):
    view_name = view_name.replace('.', '_')
    if fname.endswith(('.csv', '.tsv')):
        return "CREATE VIEW \"{}\" AS SELECT * FROM read_csv_auto('{}', header=true)".format(view_name, glob)
    elif fname.endswith('.parquet'):
        return "CREATE VIEW \"{}\" AS SELECT * FROM '{}'".format(view_name, glob)
    elif fname.endswith(('.ndjson', '.jsonl')):
        return "CREATE VIEW \"{}\" AS SELECT * FROM read_ndjson_auto('{}')".format(view_name, glob)

def view_for_combined(view_name, file_list):
    # Create a UNION ALL query for all files in file_list
    return f"CREATE VIEW \"{view_name}\" AS " + " UNION ALL ".join(
        [f"SELECT * FROM read_parquet('{file}')" for file in file_list]
    )
def create_views(dirname,httpfs):
    rv = []

    
    if httpfs:
        combined_view_stmt = ""
        # Parse the bucket name and prefix
        bucket_name = dirname.split('/')[2]
        prefix = '/'.join(dirname.split('/')[3:])
        
        # LocalStack endpoint for S3
        s3_endpoint = "http://localhost:4566"
        
        # Initialize S3 client with dummy credentials for LocalStack
        s3 = boto3.client('s3', endpoint_url=s3_endpoint,
                          aws_access_key_id='dummyaccess',
                          aws_secret_access_key='dummysecret')
        
        # List all .parquet files in the specified bucket and prefix
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if 'Contents' not in response:
                print("No files found in the specified bucket/prefix.")
                return combined_view_stmt
        except Exception as e:
            print(f"Error listing objects in S3 bucket: {e}")
            return combined_view_stmt
            
        # Collect all S3 file URLs to be UNIONed in the view
        file_urls = []
        for obj in response['Contents']:
            fname = obj['Key']
            if fname.endswith('.parquet'): # An extra check to ensure only parquet files are considered
                file_urls.append(f"{s3_endpoint}/{bucket_name}/{fname}")
        
        # Use the directory name as the view name
        view_name = Path(dirname).name
        combined_view_stmt = view_for_combined(view_name, file_urls)
        rv.append(combined_view_stmt)
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
                rv.append(view_for(Path(fname).stem, file.path, os.path.join(fname, '*' + Path(file).suffix)))
            else:
                rv.append(view_for(Path(fname).stem, fname, fname))

    return [x for x in rv if x]
