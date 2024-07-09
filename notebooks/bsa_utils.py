import grequests
import pandas as pd
import re
import requests
import warnings
import urllib.parse
from datetime import datetime
import time
import gzip
import io
import os

warnings.simplefilter("ignore", category=UserWarning)

base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
package_list_method = 'package_list'     # List of data-sets in the portal
package_show_method = 'package_show?id=' # List all resources of a data-set
action_method = 'datastore_search_sql?'  # SQL action method

# Create data directory if doesn't exist
dir_path = os.path.join("..", "data")
if not os.path.exists(dir_path):
    os.makedirs(dir_path)

# Function to convert YYYYMM to YYYY-MM-DD
def convert(date_str):
    return datetime.strptime(date_str, "%Y%m").strftime("%Y-%m-%d")

def show_new_items(df_existing, df_latest):
    # Merge the dataframes with an indicator
    merged_df = df_latest.merge(df_existing, on='BNF_CODE', how='left', indicator=True)

    # Filter the rows where BNF_CODE in df_latest does not appear in df_existing
    new_codes = merged_df[merged_df['_merge'] == 'left_only']

    # Drop the BNF_DESCRIPTION_y column and the indicator column
    new_codes = new_codes.drop(columns=['BNF_DESCRIPTION_y', '_merge'])

    # Rename BNF_DESCRIPTION_x to BNF_DESCRIPTION
    new_codes = new_codes.rename(columns={'BNF_DESCRIPTION_x': 'BNF_DESCRIPTION'})

    # Sort the dataframe by BNF_CODE
    new_codes = sort_by_bnf_code(new_codes)

    return new_codes

def sort_by_bnf_code(df):
    # Sort the dataframe by BNF_CODE
    #Add column BNF_CHAPTER which is the first 2 characters of BNF_CODE
    df['BNF_CHAPTER'] = df['BNF_CODE'].str[:2]
    #Add column BNF_SECTION which is the 3rd and 4th characters of BNF_CODE
    df['BNF_SECTION'] = df['BNF_CODE'].str[2:4]
    #Add column BNF_PARAGRAPH which is the 5th and 6th characters of BNF_CODE
    df['BNF_PARAGRAPH'] = df['BNF_CODE'].str[4:6]
    #Add column BNF_SUBPARAGRAPH which is the 7th character of BNF_CODE
    df['BNF_SUBPARAGRAPH'] = df['BNF_CODE'].str[6]

    # Sort the dataframe by BNF_CHAPTER Ascending, BNF_SECTION Ascending, BNF_PARAGRAPH Ascending, BNF_SUBPARAGRAPH Ascending
    df = df.sort_values(by=['BNF_CHAPTER', 'BNF_SECTION', 'BNF_PARAGRAPH', 'BNF_SUBPARAGRAPH'])

    # Reset the index
    df = df.reset_index(drop=True)

    return df

def show_available_datasets():
    # Extract list of datasets
    datasets_response = requests.get(base_endpoint +  package_list_method).json()
    
    # Get as a list
    dataset_list=datasets_response['result']
    
    # Excluse FOIs from the results
    list_to_exclude=["foi"]
    filtered_list = [item for item in dataset_list if not any(item.startswith(prefix) for prefix in list_to_exclude)]
    
    # Print available datasets
    for item in filtered_list:
        print (item)

def resource_name_list_filter(resource, date_from="earliest", date_to="latest"):
    metadata_repsonse  = requests.get(f"{base_endpoint}" \
                                      f"{package_show_method}" \
                                      f"{resource}").json()
    
    resources_table  = pd.json_normalize(metadata_repsonse['result']['resources'])
    
    # Extract date from bq_table_name and add this as a new column date
    resources_table['date'] = pd.to_datetime(resources_table['bq_table_name'].str.extract(r'(\d{6})')[0], format='%Y%m', errors='coerce')
    
    # Set date filters based on input
    if date_from == "earliest" or date_from == "":
        date_from = resources_table['date'].min()
    elif date_from == "latest":
        date_from = resources_table['date'].max()
    else:
        date_from = convert(date_from)  # Convert from YYYYMM to YYYY-MM-DD
    
    if date_to == "latest" or date_to=="":
        date_to = resources_table['date'].max()
    else:
        date_to = convert(date_to)  # Convert from YYYYMM to YYYY-MM-DD

    # Filter the DataFrame
    filtered_df = resources_table[(resources_table['date'] >= date_from) & (resources_table['date'] <= date_to)]
    
    # Extract the 'bq_table_name' column as a list
    bq_table_name_list = filtered_df['bq_table_name'].tolist()

    # Return list of resource names
    return bq_table_name_list

def resource_name_list_filter(resource, date_from="earliest", date_to="latest"):
    metadata_repsonse  = requests.get(f"{base_endpoint}" \
                                      f"{package_show_method}" \
                                      f"{resource}").json()
    
    resources_table  = pd.json_normalize(metadata_repsonse['result']['resources'])
    
    # Extract date from bq_table_name and add this as a new column date
    resources_table['date'] = pd.to_datetime(resources_table['bq_table_name'].str.extract(r'(\d{6})')[0], format='%Y%m', errors='coerce')
    
    # Set date filters based on input
    if date_from == "earliest" or date_from == "":
        date_from = resources_table['date'].min()
    elif date_from == "latest":
        date_from = resources_table['date'].max()
    else:
        date_from = convert(date_from)  # Convert from YYYYMM to YYYY-MM-DD
    
    if date_to == "latest" or date_to=="":
        date_to = resources_table['date'].max()
    else:
        date_to = convert(date_to)  # Convert from YYYYMM to YYYY-MM-DD

    # Filter the DataFrame
    filtered_df = resources_table[(resources_table['date'] >= date_from) & (resources_table['date'] <= date_to)]
    
    # Extract the 'bq_table_name' column as a list
    bq_table_name_list = filtered_df['bq_table_name'].tolist()

    # Return list of resource names
    return bq_table_name_list

def async_query(resource_id, sql):
    placeholder = "{FROM_TABLE}"
    if placeholder not in sql:
        raise ValueError(f"Placeholder {placeholder} not found in the SQL query.")
    sql = sql.replace(placeholder, f"FROM `{resource_id}`")
    return sql

def fetch_data(resource, sql, date_from, date_to):
    print("Initializing fetch_data function")
    
    # Create blank list to append each URL to call
    async_api_calls = []

    # Create blank list of resources for error handling
    resource_names = []
    
    # Get a list of resources matching criteria (months to include)
    print("Getting a list of available datasets based on date range")
    resource_name_list = resource_name_list_filter(resource, date_from, date_to)
    
    # Generate a list of API calls (and resource_names for error handling)
    print("Generating list of API calls")
    for resource_id in resource_name_list:
        resource_names.append(resource_id)
        async_api_calls.append(
            f"{base_endpoint}" \
            f"{action_method}" \
            "resource_id=" \
            f"{resource_id}" \
            "&" \
            "sql=" \
            f"{urllib.parse.quote(async_query(resource_id, sql))}" # Encode spaces in the url 
        )
    
    # Function to make the API call with retries
    def make_request(url, retries=2):
        for attempt in range(retries):
            response = grequests.get(url).send().response
            if response and response.ok:
                print(f"API call successful for URL: {url}")
                return response
            print(f"Attempt {attempt + 1} failed for URL: {url}")
            time.sleep(1)  # Optional: add a delay between retries
        print(f"API call failed after {retries} attempts for URL: {url}")
        return None

    print("Processing API calls with retries")
    # Process API calls with retries
    res = [make_request(url) for url in async_api_calls]

    # Count the number of failed attempts and store failed details
    failed_attempts = sum(1 for x in res if not (x and x.ok))
    failed_details = [(async_api_calls[idx], resource_names[idx]) for idx, x in enumerate(res) if not (x and x.ok)]
    
    if failed_attempts >= 3:
        print("3 or more API calls failed. Exiting function.")
        for call, resource in failed_details:
            print(f"Failed API call: {call} for resource ID: {resource}")
        raise ValueError("API call error: 3 or more failed attempts.")
    
    # Check all API calls ran OK
    all_ok = all(x and x.ok for x in res)
    any_ok = any(x and x.ok for x in res)
        
    if all_ok:
        print("All API calls ran successfully")
    else:
        # Print information about successful and failed API calls
        for idx, response in enumerate(res):
            if response and response.ok:
                print(f"API call for resource {resource_names[idx]} succeeded")
            else:
                print(f"API call for resource {resource_names[idx]} failed with status code {response.status_code if response else 'No response'} and response {response.text if response else 'No response'}")
                print(f"Failed URL call {async_api_calls[idx]}")

    if not all_ok:
        raise ValueError("API call error.")
    
    print("Processing response data")
    # Initialize an empty list to store the temporary dataframes
    dataframes = []
    
    for idx, x in enumerate(res):
        if x and x.ok:
            # Grab the response JSON as a temporary list
            tmp_response = x.json()
            
            # Check if the response is truncated and contains a download URL
            if 'records_truncated' in tmp_response['result'] and tmp_response['result']['records_truncated'] == 'true':
                download_url = tmp_response['result']['gc_urls'][0]['url']
                print(f"Downloading truncated data from URL: {download_url}")
                
                # Download and process the gzip file
                r = requests.get(download_url)
                with gzip.open(io.BytesIO(r.content), 'rt') as f:
                    tmp_df = pd.read_csv(f)
            else:
                # Extract records in the response to a temporary dataframe
                tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])
            
            # Append the temporary dataframe to the list
            dataframes.append(tmp_df)
    
    # Concatenate all dataframes in the list into a single dataframe
    print("Concatenating dataframes")
    async_df = pd.concat(dataframes, ignore_index=True)

    # Return dataframe
    print("Returning final dataframe")
    return async_df