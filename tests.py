import pytest
import helper_modules as hp
import os.path
import boto3


bucket = 'sid-coding-test-data'

def test_download_s3_file():
    test_file = './test_data/sensor_locations.csv'
    key = 'rawdata/Pedestrian_Counting_System_-_Sensor_Locations.csv'
    hp.download_s3_file(bucket, key, test_file)
    assert os.path.isfile(test_file)

def test_load_data():
    df_test_sloc = hp.load_data('./test_data/sensor_locations.csv')
    assert df_test_sloc.shape == (78,11)

def test_process_data():
    df_test_ped_cntph = hp.load_data('./test_data/pedestrian_counts.csv')
    df_test_sloc = hp.load_data('./test_data/sensor_locations.csv')
    hp.process_data(bucket, df_test_ped_cntph, df_test_sloc)
    test_file1 = 'topn_by_day_loc.csv'
    test_file2 = 'topn_by_month_loc.csv'
    assert os.path.isfile(test_file1)
    assert os.path.isfile(test_file2)
    os.remove('topn_by_day_loc.csv')
    os.remove('topn_by_month_loc.csv')

def test_write_to_s3():
    df_test_sloc = hp.load_data('./test_data/sensor_locations.csv')
    hp.write_to_s3(bucket,'test/data',df_test_sloc)
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket = bucket, Key = 'test/data')
    except:
        assert False

