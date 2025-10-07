import io
import json
import boto3
import pandas as pd 
import multiprocessing
from lambda_multiprocessing import Pool

from botocore.exceptions import ClientError

def lambda_handler(event, context):

    #df_parquet = read_parquet_from_s3_as_df('ca-central-1', 'webpresence-geocore-geojson-to-parquet-stage', 'records.parquet')
    #df_sentinel1 = read_parquet_from_s3_as_df('ca-central-1', 'webpresence-geocore-geojson-to-parquet-stage', 'sentinel1.parquet')
    #df_rcm = read_parquet_from_s3_as_df('ca-central-1', 'webpresence-geocore-geojson-to-parquet-stage', 'rcm-ard.parquet')
    #df = pd.concat([df_parquet, df_sentinel1, df_rcm], ignore_index=True)
    s3_paginate_options = {'Bucket': 'webpresence-geocore-geojson-to-parquet-stage'}
    region = 'ca-central-1'
    limit = int(event.get("limit", 10000))
    page = int(event.get("page", 1))
    source_system = event.get("source_system")

    print(source_system)

    try:
        filename_list = s3_filenames_paginated(region, **s3_paginate_options)
    except ClientError as e:
        print("Could not paginate the geojson bucket:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    
    print(filename_list)
      

    with Pool() as p:
        # for each json file, open for reading, add to dataframe (df), close
        # note: if there are too many records to process, we may need to paginate 
        frames = p.map(read_parquet_from_s3_as_df, filename_list)

    df = pd.concat(frames, ignore_index=True)

    # Select only relevant columns
    if 'features_properties_id' not in df.columns or 'features_properties_date_modified' not in df.columns  or 'features_properties_sourceSystemName' not in df.columns:
        missing = [c for c in ['features_properties_id', 'features_properties_date_modified', 'features_properties_sourceSystemName'] if c not in df.columns]
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f"Missing columns: {missing}"})
        }

    # Sort and select needed columns
    df = df[['features_properties_id', 'features_properties_date_modified', 'features_properties_sourceSystemName']].sort_values(
        by='features_properties_date_modified', ascending=False
    )
    df['features_properties_date_modified'] = pd.to_datetime(
        df['features_properties_date_modified']
    ).dt.strftime('%Y-%m-%dT%H:%M:%S')


    if source_system:
        if 'features_properties_sourceSystemName' not in df.columns:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': "Missing column: features_properties_sourceSystemName"})
            }
        df = df[df['features_properties_sourceSystemName'] == source_system]

    # --- Pagination ---

    lower = max((page - 1) * limit, 0)
    upper = lower + limit

    paged_df = df.iloc[lower:upper]
    paged_df = paged_df.rename(columns={
        'features_properties_id': 'id',
        'features_properties_date_modified': 'modified',
        'features_properties_sourceSystemName': 'source'
    })
    response_records = paged_df.to_dict(orient='records')

    #print(ids_json)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'page': page,
            'limit': limit,
            'total': len(df),
            'results': response_records
        }, default=str)
    }

def read_parquet_from_s3_as_df(s3_key):
    """
    Load a Parquet file from an S3 bucket into a pandas DataFrame.

    Parameters:
    - region: AWS region where the S3 bucket is located.
    - s3_bucket: Name of the S3 bucket.
    - s3_key: Key (path) to the Parquet file within the S3 bucket.

    Returns:
    - df: pandas DataFrame containing the data from the Parquet file.
    """
    s3_bucket = 'webpresence-geocore-geojson-to-parquet-stage'
    region = 'ca-central-1'
    # Setup AWS session and clients
    session = boto3.Session(region_name=region)
    s3 = session.resource('s3')

    # Load the Parquet file as a pandas DataFrame
    object = s3.Object(s3_bucket, s3_key)
    body = object.get()['Body'].read()
    df = pd.read_parquet(io.BytesIO(body))
    return df

def s3_filenames_paginated(region, **kwargs):
    """Paginates a S3 bucket to obtain file names. Pagination is needed as S3 returns 999 objects per request (hard limitation)
    :param region: region of the s3 bucket 
    :param kwargs: Must have the bucket name. For other options see the list_objects_v2 paginator: 
    :              https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
    :return: a list of filenames within the bucket
    """
    client = boto3.client('s3', region_name=region)
    
    paginator = client.get_paginator('list_objects_v2')
    result = paginator.paginate(**kwargs)
    
    filename_list = []
    count = 0
    
    for page in result:
        if "Contents" in page:
            for key in page[ "Contents" ]:
                keyString = key[ "Key" ]
                #print(keyString)
                count += 1
                filename_list.append(keyString)
    
    print("Bucket contains:", count, "files")
                
    return filename_list