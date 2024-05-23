import json
import boto3
import os

import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd 
import io
from io import BytesIO
from datetime import datetime, timedelta
 
# Define the S3 bucket and file paths
source_bucket = os.environ['source_bucket']
linkedin_mapping_file = os.environ['linkedin_mapping_file']
linkedin_target_file = os.environ['linkedin_target_file']


weekly_paid_folder=os.environ['weekly_paid_folder']
paid_adjectivemapping=os.environ['adjectivemapping']
weekly_paid_file=os.environ['weekly_paid_file']


master_table=os.environ['master_table']
organic_account_mapping=os.environ['organic_account_mapping']
paid_account_mapping=os.environ['paid_account_mapping']

Database_url=os.environ['Database_url']
 

# Initialize the S3 client
s3 = boto3.client('s3')

def save_df_to_s3(df, bucket, key):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def read_excel_from_s3(bucket, key):
    """Reads an Excel file from S3 and returns a DataFrame."""
    response = s3.get_object(Bucket=bucket, Key=key)
    excel_data = io.BytesIO(response['Body'].read())
    return pd.read_excel(excel_data, engine='openpyxl')  # Ensure to use 'openpyxl' for .xlsx files


def read_json_lines_from_s3(bucket, key):
    """Reads a JSON file line-by-line from S3 and returns a DataFrame."""
    response = s3.get_object(Bucket=bucket, Key=key)
    json_lines = response['Body'].read().decode('utf-8').splitlines()
    data = [json.loads(line) for line in json_lines]
    return pd.DataFrame(data)
  
# def read_csv_from_s3(bucket, key, encoding='utf-8'):
#     """
#     Reads a CSV file from an S3 bucket into a pandas DataFrame, using the specified encoding.

#     Parameters:
#     - bucket: The name of the S3 bucket.
#     - key: The key of the CSV file in the S3 bucket.
#     - encoding: The encoding to use for reading the CSV file. Defaults to 'utf-8'.

#     Returns:
#     - A pandas DataFrame containing the CSV data.
#     """
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     df = pd.read_csv(BytesIO(obj['Body'].read()), encoding=encoding)
#     return df


def read_csv_from_s3(bucket, key, encoding='utf-8', usecols=None):
    """
    Reads a CSV file from an S3 bucket into a pandas DataFrame, using the specified encoding.
    Optionally reads only specified columns to save memory.

    Parameters:
    - bucket (str): The name of the S3 bucket.
    - key (str): The key of the CSV file in the S3 bucket.
    - encoding (str): The encoding to use for reading the CSV file. Defaults to 'utf-8'.
    - usecols (list, optional): Specifies a list of column names to read. Default is None, which reads all columns.

    Returns:
    - pd.DataFrame: A DataFrame containing the CSV data.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(BytesIO(obj['Body'].read()), encoding=encoding, usecols=usecols)
    return df 
    
    
def check_targeted_geography_availability(source_bucket, linkedin_mapping_file, linkedin_target_file, mapping_file_encoding='ISO-8859-1'):
    df_mapping = read_csv_from_s3(source_bucket, linkedin_mapping_file, encoding=mapping_file_encoding)
    df_target = read_json_lines_from_s3(source_bucket, linkedin_target_file)
    
    target_geographies = df_target['TARGETED_GEOGRAPHY'].unique()
    mapping_geographies = df_mapping['TARGETED_GEOGRAPHY'].unique()
    
    missing_geographies = [geo for geo in target_geographies if geo not in mapping_geographies]
    
    if missing_geographies:
        missing_info = "<h3>Missing TARGETED_GEOGRAPHY values from Linkedin region mapping :</h3><ul>" + \
                       "".join([f"<li>{geo}</li>" for geo in missing_geographies]) + "</ul>"
    else:
        missing_info = "<p><b> All targeted geographies match the LinkedIn mapping..</b></p>"
    
    return missing_info



def list_all_objects(bucket, prefix):
    """List all objects in the S3 bucket under a specified prefix."""
    objects = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects.extend(response.get('Contents', []))
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    return objects
       
 
def read_weekly_paid_file(bucket, base_folder, file_name):
    """Finds today's folder based on the date and reads the weekly paid file from it."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    objects = list_all_objects(bucket, base_folder)
    # Find today's folder by checking if today's date is in the object key
    todays_folder = None
    for obj in objects:
        if today_str in obj['Key']:
            todays_folder = '/'.join(obj['Key'].split('/')[:-1]) + '/'  # Ensure it ends with '/'
            
            break
    if not todays_folder:
        print('No folder found for today')
        return None
    # Construct the path to the weekly paid file and read it
    weekly_paid_file_path = f"{todays_folder}{file_name}"
   
    return read_json_lines_from_s3(bucket, weekly_paid_file_path)
 
def check_adjectives_in_objectives(df_weekly_paid, df_adjective_mapping):
    ad_objectives = df_weekly_paid['AD_OBJECTIVE'].unique()
    objectives = df_adjective_mapping['Objective'].unique()

    missing_objectives = [obj for obj in ad_objectives if obj not in objectives]

    # Prepare a message based on missing objectives
    if missing_objectives:
        missing_objectives_str = "<h3>The following 'AD_OBJECTIVE' values from weekly paid are not found in 'adjective mapping':</h3><ul>" + \
                                 "".join([f"<li>{obj}</li>" for obj in missing_objectives]) + "</ul>"
    else:
        missing_objectives_str = "<p>All 'AD_OBJECTIVE' values from paid data exist in 'adjective mapping' file .</p>"

    return missing_objectives_str



def get_account_analysis_enhanced(master_table_df, master_account_column_name, country_mapping_df, country_mapping_account_column_name, account_type):
    current_date = datetime.now().date()
    two_weeks_ago = current_date - timedelta(days=14)
    
    accounts_analysis_data = []  # Will hold data for accounts not seen in the last two weeks
    
    # Ensure the 'Pull Date' column is in datetime format for accurate date comparison
    master_table_df['Pull Date'] = pd.to_datetime(master_table_df['Pull Date'])
    
    # No separate collection for missing accounts as you don't want to flag never appeared accounts
    
    for account in country_mapping_df[country_mapping_account_column_name]:
        account_records = master_table_df[master_table_df[master_account_column_name] == account]
        
        # Proceed only if there are records for the account
        if not account_records.empty:
            last_appearance_date = account_records['Pull Date'].max().date()
            
            # Include in analysis if last appearance is older than two weeks
            if last_appearance_date < two_weeks_ago:
                last_appearance_date_str = last_appearance_date.strftime('%Y-%m-%d')
                
                sorted_dates = account_records['Pull Date'].sort_values().reset_index(drop=True)
                gaps = sorted_dates.diff().dt.days[1:]
                average_gap = gaps.mean() if not gaps.empty else None
                
                platform_string = ', '.join(account_records['platform_name'].dropna().astype(str).unique())
                
                accounts_analysis_data.append({
                    "Account": account,
                    "Account Type": account_type,
                    "Last Appearance Date": last_appearance_date_str,
                    "Average Appearance Rate (Days)": average_gap,
                    "Platform": platform_string
                })

    # Convert the list of dictionaries to DataFrame
    accounts_analysis_df = pd.DataFrame(accounts_analysis_data)
    
    return accounts_analysis_df


def find_new_accounts_details_in_master(master_table_df, master_account_column_name, mapping_df, mapping_account_column_name, account_type):
    """
    Identifies new accounts in the master_table_df that are not present in the mapping_df and provides details on their first and last appearance, along with the platform and account type, excluding NaN values from the platform aggregation.

    Parameters:
    - master_table_df (pd.DataFrame): DataFrame containing the master account data.
    - master_account_column_name (str): The column name in master_table_df that identifies accounts.
    - mapping_df (pd.DataFrame): DataFrame containing the mapping data.
    - mapping_account_column_name (str): The column name in mapping_df that identifies accounts.
    - account_type (str): The type of account (e.g., "Organic", "Paid").

    Returns:
    - pd.DataFrame: A DataFrame with details of new accounts including first and last appearance, platforms (excluding NaN values), and account type.
    """
    
    # Ensure the 'Pull Date' column is in datetime format for accurate comparison
    master_table_df['Pull Date'] = pd.to_datetime(master_table_df['Pull Date'])
    
    # Identify new accounts not present in mapping
    master_accounts_set = set(master_table_df[master_account_column_name].unique())
    mapping_accounts_set = set(mapping_df[mapping_account_column_name].unique())
    new_accounts = list(master_accounts_set - mapping_accounts_set)
    
    # Filter master table for new accounts only
    new_accounts_df = master_table_df[master_table_df[master_account_column_name].isin(new_accounts)]
    
    # Aggregate information for new accounts
    new_accounts_details = new_accounts_df.groupby(master_account_column_name).agg(
        First_Appearance=pd.NamedAgg(column='Pull Date', aggfunc='min'),
        Last_Appearance=pd.NamedAgg(column='Pull Date', aggfunc='max'),
        Platforms=pd.NamedAgg(column='platform_name', aggfunc=lambda x: ', '.join(x.dropna().astype(str).unique())),
    ).reset_index()
    
    # Add account type column
    new_accounts_details['Account Type'] = account_type
    
    return new_accounts_details

def dataframe_to_html(df):
    """Converts a Pandas DataFrame to an HTML table."""
    return df.to_html(index=False, border=0, classes='dataframe')
 
# def prepare_html_content(geography_availability_info, missing_objectives_str,
#                          combined_missing_accounts_analysis, combined_new_accounts_details):
#     """Prepares HTML content by combining given information and dataframes."""
#     html_content = f"""
#     <html>
#     <head>
#     <style>
#         .dataframe {{font-size: 12px; border-collapse: collapse;}}
#         .dataframe th, .dataframe td {{text-align: left; padding: 8px;}}
#         .dataframe tr:nth-child(even) {{background-color: #f2f2f2;}}
#     </style>
#     </head>
#     <body>
#     {geography_availability_info}
#     {missing_objectives_str}
#     <h3>Missing Accounts Analysis For Daily and Weekly Data:</h3>
#     {dataframe_to_html(combined_missing_accounts_analysis)}
#     <h3>Newly Added  Accounts in Daily and Weekly Data :</h3>
#     {dataframe_to_html(combined_new_accounts_details)}
#     </body>
#     </html>
#     """
#     return html_content

def prepare_html_content(geography_availability_info, missing_objectives_str,
                         combined_missing_accounts_analysis, combined_new_accounts_details,
                         benchmark_comparison_message):
    """Prepares HTML content by combining given information and dataframes."""
    html_content = f"""
    <html>
    <head>
    <style>
        .dataframe {{font-size: 12px; border-collapse: collapse;}}
        .dataframe th, .dataframe td {{text-align: left; padding: 8px;}}
        .dataframe tr:nth-child(even) {{background-color: #f2f2f2;}}
    </style>
    </head>
    <body>
    {geography_availability_info}
    {missing_objectives_str}
    <h3>Missing Accounts Analysis For Daily and Weekly Data:</h3>
    {dataframe_to_html(combined_missing_accounts_analysis)}
    <h3>Newly Added Accounts in Daily and Weekly Data:</h3>
    {dataframe_to_html(combined_new_accounts_details)}
    <h3>Benchmark Comparison:</h3>
    {benchmark_comparison_message}
    </body>
    </html>
    """
    return html_content
 
    
def send_email_via_ses(html_content):
    """Sends an email with the given HTML content using AWS SES."""
        # Specify the source email and destination email(s)
    source_email = 'prabhatmishra160@gmail.com'  # Make sure this email is verified in AWS SES
    destination_emails = ['prabhatmishra160@gmail.com','prabhat.mishra@mcsaatchi.com','david.lawton@mcsaatchi.com']  # List of recipient emails
    #destination_emails = ['prabhatmishra160@gmail.com','prabhat.mishra@mcsaatchi.com'] 
        # Specify the subject of the email
    subject = 'Core Flagging system Report'
    
    client = boto3.client('ses')
    try:
        response = client.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': destination_emails
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Html': {
                        'Data': html_content,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        return response
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None

 
 

def read_table_from_rds(db_url, table_name):
    """
    Connects to a PostgreSQL database and reads a table into a Pandas DataFrame.

    Parameters:
    - db_url (str): The database URL in the format expected by SQLAlchemy.
    - table_name (str): The name of the table to read.

    Returns:
    - pd.DataFrame: A DataFrame containing the data from the specified table.
    """
    # Initialize the database engine
    engine = create_engine(db_url)

    # Use pandas to load the SQL table into a DataFrame
    df = pd.read_sql_table(table_name, con=engine)

    return df



def find_missing_benchmarks(df_source, df_target, column_name="Benchmark Matcher"):
    """
    Identifies benchmarks present in the source DataFrame but missing in the target DataFrame.

    Parameters:
    - df_source (pd.DataFrame): The source DataFrame to check benchmarks from.
    - df_target (pd.DataFrame): The target DataFrame to check benchmarks against.
    - column_name (str): The name of the column containing benchmark identifiers.

    Returns:
    - list: A list of benchmarks present in the source but missing in the target.
    """
    # Find unique benchmarks in both DataFrames
    benchmarks_source = set(df_source[column_name].unique())
    benchmarks_target = set(df_target[column_name].unique())

    # Identify benchmarks present in source but missing in target
    missing_benchmarks = list(benchmarks_source - benchmarks_target)

    return missing_benchmarks
 
 
def lambda_handler(event, context):
    # Check targeted geography availability

    geography_availability_info = check_targeted_geography_availability(
    source_bucket, linkedin_mapping_file, linkedin_target_file, 'ISO-8859-1'
)

    
    # Read weekly paid file from today's folder
    df_weekly_paid = read_weekly_paid_file(source_bucket, weekly_paid_folder, weekly_paid_file)

    # Read paid adjective mapping Excel file
    df_adjective_mapping = read_excel_from_s3(source_bucket, paid_adjectivemapping)
    print(df_adjective_mapping)

         
           
    # Check if all AD_OBJECTIVE values exist in Objective
    missing_objectives_str=check_adjectives_in_objectives(df_weekly_paid, df_adjective_mapping)

    # Read the master table CSV file
    df_master_table = read_csv_from_s3(source_bucket, master_table, encoding='utf-8',usecols=['AD_ACCOUNT', 'is_paiddata', 'ACCOUNT', 'Pull Date','platform_name'])

        # Filter the DataFrame to create one DataFrame for organic and boosted data (is_paiddata = 0)
    df_organic_boosted = df_master_table[df_master_table['is_paiddata'] == 0].reset_index(drop=True)
   
      
  
    
    # Filter the DataFrame to create another DataFrame for paid data (is_paiddata = 1)
    df_paid = df_master_table[df_master_table['is_paiddata'] == 1].reset_index(drop=True)
    # Read the organic account mapping Excel file
    df_organic_account_mapping = read_excel_from_s3(source_bucket, organic_account_mapping)
    
    organic_accounts_analysis = get_account_analysis_enhanced(
    master_table_df=df_organic_boosted,
    master_account_column_name="ACCOUNT",
    country_mapping_df=df_organic_account_mapping,
    country_mapping_account_column_name="Account",
    account_type="Organic"
)
    
    new_organic_accounts_details = find_new_accounts_details_in_master(
    master_table_df=df_organic_boosted,
    master_account_column_name="ACCOUNT",
    mapping_df=df_organic_account_mapping,
    mapping_account_column_name="Account",
    account_type="Organic"
   
) 
    
       
    # Read the paid account mapping Excel file
    df_paid_account_mapping = read_excel_from_s3(source_bucket, paid_account_mapping)
    
    
    paid_accounts_analysis = get_account_analysis_enhanced(
    master_table_df=df_paid,
    master_account_column_name="AD_ACCOUNT",
    country_mapping_df=df_paid_account_mapping,
    country_mapping_account_column_name="Paid_Account",
    account_type="Paid"
    )
    
    new_paid_accounts_details = find_new_accounts_details_in_master(
    master_table_df=df_paid,
    master_account_column_name="AD_ACCOUNT",
    mapping_df=df_paid_account_mapping,
    mapping_account_column_name="Paid_Account",
    account_type="Paid"
)

 
    
        # Assuming organic_accounts_analysis and paid_accounts_analysis are already DataFrames
    combined_missing_accounts_analysis = pd.concat([organic_accounts_analysis, paid_accounts_analysis], ignore_index=True)
    
    # Assuming new_organic_accounts_details and new_paid_accounts_details are already DataFrames
    combined_new_accounts_details = pd.concat([new_organic_accounts_details, new_paid_accounts_details], ignore_index=True)


    
    df_sprinklr = read_table_from_rds(Database_url, "sprinklr_test")
    df_dynamic_benchmark = read_table_from_rds(Database_url, "dynamic_benchmark")
    
    # Find missing benchmarks
    missing_benchmarks = find_missing_benchmarks(df_sprinklr, df_dynamic_benchmark, "Benchmark Matcher")
    if missing_benchmarks:
        benchmark_comparison_message = f"<p>The following benchmarks are present in Sprinklr Table but missing in Dynamic Benchmark Table:</p><ul>" + \
                                       "".join([f"<li>{benchmark}</li>" for benchmark in missing_benchmarks]) + "</ul>"
    else:
        benchmark_comparison_message = "<p><b>No missing benchmarks found between Sprinklr Table and Dynamic Benchmark Table.</b></p>"


    html_content = prepare_html_content(
        geography_availability_info,
        missing_objectives_str,
        combined_missing_accounts_analysis,
        combined_new_accounts_details,
        benchmark_comparison_message
    )
    
 

    # Send the email
    send_email_response = send_email_via_ses(html_content)
    
    if send_email_response:
        print("Email sent successfully!")
    else:
        print("Failed to send the email.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Email sent successfully!')
    }
