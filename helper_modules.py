import pandas as pd
import pyarrow
import logging
import boto3
import s3fs
import botocore

log = logging.getLogger(__name__)

def download_s3_file(bucket,key,filename):
    s3 = boto3.resource('s3')

    try:
        log.info('Downloading data file : '+key)
        s3.Bucket(bucket).download_file(key, filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            log.error("The object does not exist.")
        else:
            raise

def load_data(filename):
    log.info('Loading file : '+filename)
    df = pd.read_csv(filename)
    df.columns = df.columns.str.lower()
    return df

def process_data(bucket, df_ped_cntph, df_sloc):

    N=10
    ## Get top N sensors with highest pedestrian traffic by day
    df_topn_by_day = df_ped_cntph.groupby(["sensor_id","day"])["hourly_counts"].sum().groupby(level=1).nlargest(N).reset_index(-1, drop=True).reset_index()

    ## Get top N sensors with highest pedestrian traffic by month
    df_topn_by_month = df_ped_cntph.groupby(["sensor_id","month"])["hourly_counts"].sum().groupby(level=1).nlargest(N).reset_index(-1, drop=True).reset_index()

    ## The sensor_name field already contains human readable location, but true location (latitude and longitude) can be joined from sensor_locations data
    df_loc = df_sloc[['sensor_id','sensor_description','location']]
    df_topn_by_day_loc = pd.merge(df_topn_by_day,df_loc,how="left",on="sensor_id")
    df_topn_by_month_loc = pd.merge(df_topn_by_month,df_loc,how="left",on="sensor_id")

    ## Write the required Top N data to files
    log.info('Writing local data files')
    df_topn_by_day_loc.to_csv('topn_by_day_loc.csv')
    df_topn_by_month_loc.to_csv('topn_by_month_loc.csv')

def write_to_s3(bucket,root,df,partitiions=False):
    # s3 = boto3.client('s3')
    s3_url = "s3://" + bucket + '/' + root #sid-coding-test-data/procdata/ped_loc_count_np/"
    # df.to_parquet(s3_url,engine='auto', index=None, partition_cols=partitiions)
    log.info('Writing to S3 : '+root)
    if partitiions:
        df.to_parquet(s3_url,engine='auto', index=None, partition_cols=partitiions)
    else:
        df.to_parquet(s3_url,engine='auto', index=None)

def run_athena_query(query,database, output_loc):
    client = boto3.client('athena', region_name='ap-southeast-2')
    log.info('Running Athena query')
    query_id = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': database
        },
        ResultConfiguration = { 'OutputLocation': output_loc })
    return query_id

def is_aws_creds_valid():
    sts = boto3.client('sts')
    try:
        sts.get_caller_identity()
        return True
    except botocore.exceptions.NoCredentialsError or boto3.exceptions.ClientError:
        return False


