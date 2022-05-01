import helper_modules as hp
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if __name__=="__main__":
    ## Check aws credentials
    if not hp.is_aws_creds_valid():
        print("Invalid/missing AWS credentials. Please add valid AWS credentials to `~/.aws/credentials`")
        sys.exit()
        
    ## Publically accessible S3 bucket
    bucket = 'sid-coding-test-data'

    ## List of data files
    ## The 2 data files have been manually downloaded and placed in S3 for easier processing
    ## With more time, the download process from original urls can also be automated
    file_list = [
                    {
                        'key': 'rawdata/Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv',
                        'filename': 'pedestrian_counts.csv'
                    },
                    {
                        'key': 'rawdata/Pedestrian_Counting_System_-_Sensor_Locations.csv',
                        'filename': 'sensor_locations.csv'
                    }
                ]
    
    ## Get the data files from S3
    for f in file_list:
        hp.download_s3_file(bucket,f['key'],f['filename'])

    ## Load the data into dataframes
    df_ped_cntph = hp.load_data('pedestrian_counts.csv')
    df_sloc = hp.load_data('sensor_locations.csv')

    ## Process and query the data
    hp.process_data(bucket, df_ped_cntph, df_sloc)

    ## Write the original data to S3 for future querying
    hp.write_to_s3(bucket,'procdata/ped_sensor_count/',df_ped_cntph,['year','month','mdate'])
    hp.write_to_s3(bucket,'procdata/sensor_locations/data',df_sloc)

    output_loc = "s3://"+bucket+'/athena_output/'
    database = 'default'

    ## Create external table in Athena to enable future querying
    query = """ CREATE EXTERNAL TABLE IF NOT EXISTS ped_loc_data 
        (
        id int,
        date_time string,
        day string,
        time int,
        sensor_id int,
        sensor_name string,
        hourly_counts int
        )
        PARTITIONED BY (year int, month string, mdate int)
        STORED AS PARQUET
        LOCATION 's3://sid-coding-test-data/procdata/ped_sensor_count/'
    """

    query1_id = hp.run_athena_query(query, database, output_loc)
    print(query1_id)

    query = "MSCK REPAIR TABLE `ped_loc_data`;"
    query2_id = hp.run_athena_query(query, database, output_loc)
    print(query2_id)

    query = """ CREATE EXTERNAL TABLE IF NOT EXISTS sensor_locations
        (
        sensor_id int,
        sensor_description string,
        sensor_name string,
        installation_date string,
        status string,
        note string,
        direction_1 string,
        direction_2 string,
        latitude double,
        longitude double,
        location string
        )
        STORED AS PARQUET
        LOCATION 's3://sid-coding-test-data/procdata/sensor_locations/'
    """

    query3_id = hp.run_athena_query(query, database, output_loc)
    print(query3_id)