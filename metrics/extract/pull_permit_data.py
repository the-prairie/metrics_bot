# %%
from ast import parse
import requests
from requests.exceptions import ConnectionError, Timeout , TooManyRedirects
import pandas as pd
from flatten_json import flatten
import pendulum 
from google.cloud import bigquery
import sys

import json

import logging
from urllib.parse import urlparse, parse_qs
import os

# %%
# create logger
LOGGER = logging.getLogger('log_building_permits')
fhandler = logging.FileHandler(filename='log_building_permits.log', mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
LOGGER.addHandler(fhandler)
LOGGER.setLevel(logging.DEBUG)

# %%
def get_data_in_date_range(url, start_date, end_date):

    if not url:
        url = 'https://maps.victoria.ca/server/rest/services/OpenData/OpenData_PermitsAndLicences/MapServer/2/query'
        LOGGER.info(f"No url provided, using default: {url}")
    
    if not start_date:
        start_date = pendulum.now().subtract(days=7)
        LOGGER.info(f"No end date provided. Defaulting to last 7 days")
    else:
        start_date = pendulum.parse(start_date)
        
        
    if not end_date:
        end_date = pendulum.now()
        LOGGER.info(f"No end date provided. Defaulting to latest date")
    else:
        end_date = pendulum.parse(end_date)
        


    
    batches = pendulum.period(start_date, end_date).range('months')
    
    for start_year in batches:
        end_year = start_year.add(months=1)
    
    # Prevent the end_date from going into the future
        if end_year > end_date:
            end_year = end_date
    
    
        start_year_str = start_year.format('M-D-Y HH:MM:SS')
        end_year_str = end_year.format('M-D-Y HH:MM:SS')
        
        params = {
            'f': ['json'],
            'outFields': ['*'],
            'returnIdsOnly': ['false'],
            'returnCountOnly': ['false'],
            'returnGeometry': ['false'],
            'spatialRel': ['esriSpatialRelIntersects'],
            'where': [f"(CREATED_DATE >= TIMESTAMP '{start_year_str}' AND CREATED_DATE <= TIMESTAMP '{end_year_str}')"]
            }
        
        response = requests.get(            
        url=url, 
        params=params)
        
        data = response.json()
        
        if data.get('exceededTransferLimit') is not None:
            LOGGER.info(f"{start_year_str} to {end_year_str} exceeded transfer limits.")
            
        
        for row in data.get('features'):
            yield flatten(row)

def parse_data(data):
    
    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    df['attributes_created_date'] = df['attributes_created_date'].apply(lambda x: pendulum.from_format(str(x), "x").date())
    df['attributes_completed_date'] = df['attributes_completed_date'].apply(lambda x: pendulum.from_format(str(x), "x").date())
    
    df = df.assign(
    attributes_address=df['attributes_house'] + " " + df['attributes_street'],
    attributes_work_value=df['attributes_bldgvalue'].astype('float32')
    )
    
    return df


def _get_field_schema(field):
    name = field['name']
    field_type = field.get('type', 'STRING')
    mode = field.get('mode', 'NULLABLE')
    fields = field.get('fields', [])

    if fields:
        subschema = []
        for f in fields:
            fields_res = _get_field_schema(f)
            subschema.append(fields_res)
    else:
        subschema = []

    field_schema = bigquery.SchemaField(name=name, 
        field_type=field_type,
        mode=mode,
        fields=subschema
    )
    return field_schema


def parse_bq_json_schema(schema_filename):
    schema = []
    with open(schema_filename, 'r') as infile:
        jsonschema = json.load(infile)

    for field in jsonschema:
        schema.append(_get_field_schema(field))

    return schema


if (__name__ == '__main__'):
    # this code block is used to parse commandline arguments into title and folder filters
    if len(sys.argv) > 1:
        valid = True
        url = None
        start_date = None
        end_date = None
        schema_file = None
        project = os.getenv("PROJECT_ID")
        dataset= "yyj_raw"
        table="permit_data"

        for item in sys.argv[1:]:
            if '--url' in item:
                try:
                    url = item.split('=')[1]
                    break
                except ValueError:
                    print("Couldn't parse url")

            if '--start_date' in item:
                try:
                    start_date = item.split('=')[1]
                    break
                except ValueError:
                    print("Couldn't parse start date. Example: --start_date=2021-01-01")

            if '--end_date' in item:
                try:
                    end_date = item.split('=')[1]
                    break
                except ValueError:
                    print("Couldn't parse end date. Example: --end_date=2021-01-01")
 
            if '--schema_file' in item:
                try:
                    schema_file = item.split('=')[1]
                    break
                except ValueError:
                    print("Couldn't parse schema file.")
                    valid = False
                    break
            if valid:
                
                data = get_data_in_date_range(url=url, start_date=start_date, end_date=end_date)
                df = parse_data(data)
                schema = parse_bq_json_schema(schema_file)
                
                client = bigquery.Client(project=project)
                
                # configure job
                job_config = bigquery.LoadJobConfig(
                    schema = schema,
                    write_disposition='WRITE_APPEND'
                
                )
                
                # execute job 
                load_job = client.load_table_from_dataframe(
                    df, '.'.join([project, dataset, table]), job_config = job_config
                )
                
                result = load_job.result()
                print("Written {} rows to {}".format(result.output_rows, result.destination))
    else:
        LOGGER.info("NONE")