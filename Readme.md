# Coding Challenge

### Requirements
Developed using Python 3. 
Working AWS credentials should be available in the system 
e.g. `for mac, in ~/.aws/credentials`

Install required python modules with  
```
pip3 install requirements.txt
```

### Files included
* helper_modules - Modules to help process the data
* process_sensor_data - Main module to start the script
* tests - Unit tests
* requirements.txt - List of required python modules

### Usage
The two data files have been manually downloaded and added to S3 `s3://sid-coding-test-data/rawdata/` for easier access
Given more time, it is possible to automate that as well

What the script does
1.  Downloads the 2 data files from S3
2.  Load the data files in to pandas dataframes
3.  Process the dataframes to extract the required Top 10 sensor locations by day and month by pedestrian count and writes them to local files `topn_by_day_loc.csv` and `topn_by_month_loc.csv`
4.  Write the original data to S3 in parquet format for future querying. The original downloaded data was ~ 370MB, while the parquet files written to S3 are about 70MB
5.  Create external tables in AWS Athena to enable querying of the data - `ped_loc_data` and `sensor_locations`. The Athena queries take about 17 mins to complete, after which the data can be queried through Athena

```
python3 process_sensor_data.py

# Testing
pytest tests.py
```

A very basic Tableau dashboard is published which is connected to this data in AWS Athena.
`https://public.tableau.com/app/profile/siddharth.bose/viz/TopNpedSensorsMelb/TopNSensorsbypedestriancount`

![Alt text](tableau.png?raw=true "Tableau dashboard")