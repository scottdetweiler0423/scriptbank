

print ("Google Analytics Reporting API v4 ICAR/NCAR Data Pull")
print ('')

import csv
import datetime
from datetime import date
from datetime import timedelta
import time
import sqlalchemy
import pymssql
import pandas as pd
import getpass
from tqdm import tqdm
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# View IDs for each account
# ICAR CAN = 158896057
# ICAR_US = '158852828'
# Infiniti = 103387753

# NCAR Canada = 158829437
# NCAR_US = '158878001'
# Nissan = 103399948

start_time = time.time() # start clock


def read_parameter(file_name = 'python_APItoSQL_parameters.txt'):
    with open(file_name,'r') as f:
        lines = f.readlines()
        parameter_dict = {}
        for line in lines:
            first_part = line.split('#')[0]
            key,value = first_part.strip().split("=")
            parameter_dict[key.strip()] = value.strip()
    return parameter_dict


col_dict = {'dateHourMinute': [],
            'eventAction': [],
            'eventCategory': [],
            'eventLabel': [],
            'mobileDeviceModel': [],
            'sessionCount': [],
            'operatingSystemVersion': [],
            'totalEvents': [],
            'uniqueEvents': [],
            'account': []
            }


account_list = ['NCAR_CAN',
                'ICAR_CAN',
                'NCAR_US',
                'ICAR_US'
                ]

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

# Start of account for loop
for account in account_list:
    if account == 'ICAR_US':
        VIEW_ID = '158852828'
        KEY_FILE_LOCATION = r"C:\Users\sd301759\ICAR_secrets.json"
    if account == 'ICAR_CAN':
        VIEW_ID = '158896057'
        KEY_FILE_LOCATION = r"C:\Users\sd301759\ICAR_secrets.json"
    if account == 'NCAR_US':
        VIEW_ID = '158878001'
        KEY_FILE_LOCATION = r"C:\Users\sd301759\NCAR_secrets.json"
    elif account == 'NCAR_CAN':
        VIEW_ID = '158829437'
        KEY_FILE_LOCATION = r"C:\Users\sd301759\NCAR_secrets.json"


    def initialize_analyticsreporting():
        credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics

    #defining start and end dates
    
    base = datetime.date.today()
    week = datetime.timedelta(days=6)
    dayago = datetime.timedelta(days=1)
    end_date1 = base - dayago
    start_date1 = end_date1 - week
    end_date = str(end_date1)
    start_date = str(start_date1)

    delta = end_date1 - start_date1      # timedelta

    days_of_week = []
    #list of days of the last week
    for i in range(delta.days + 1):
        days_of_week.append(str(start_date1 + timedelta(i)))

    print ('-----------------Pulling data from '+account+' for period: ' + start_date + ' to ' + end_date + '----------------------')

    #need to loop through each day of week just in case there are more than 10 tokens in a week
    for day in days_of_week:                     
        def initial_report(analytics):
            """Queries the Analytics Reporting API V4.

            Args:
              analytics: An authorized Analytics Reporting API V4 service object.
            Returns:
              The Analytics Reporting API V4 response.
            """
            return analytics.reports().batchGet(
                            body={
                              'reportRequests': [
                                {
                                    'viewId': VIEW_ID,
                                    'pageSize': 1000000000,  # set to a value well above true limit: 100000
                                    # 'pageToken': '',
                                    'dateRanges': [{'startDate': day, 'endDate': day}],
                                    'metrics': [{'expression': 'ga:totalEvents'},
                                                {'expression': 'ga:uniqueEvents'}],
                                    'dimensions': [{'name': 'ga:dateHourMinute'},
                                                   {'name': 'ga:eventAction'},
                                                   {'name': 'ga:eventCategory'},
                                                   {'name': 'ga:eventLabel'},
                                                   {'name': 'ga:mobileDeviceModel'},
                                                   {'name': 'ga:sessionCount'},
                                                   {'name': 'ga:operatingSystemVersion'}]
                                }
                              ]
                            }
              ).execute()


        analytics = initialize_analyticsreporting()
        initial_response = initial_report(analytics)
        # print_response(response)

        # rowCount is an input into full request
        for report in initial_response.get('reports', []):
            rowCount = report.get('data', {}).get('rowCount', [])

        """Generating the Page Token based on rowCount from initial_response"""

        number = (rowCount//100000)  # divide with no remainder
        L1 = list(range(1, number+1))  # defines a list of numbers between 1 and number+1
        lastrows = (rowCount - (number*100000))
        pageToken = [str(i * 100000) for i in L1]  # creates a list of tokens
        pageToken.insert(0, '')  # add a null token to beginning of token list

        # print ('Number of Tokens (without Null): ' + str(number))
        print('')
        print('Date in week: ' + day)
        print ('Page Tokens (with Null): ' + str(pageToken))
        print ('Row Count: ' + str(rowCount))
        if number + 1 > 11:
            print('Warning: Max number of tokens reached.  Split this date range into smaller increments to avoid '
                  'missing data.')
        print('')

        for Token in pageToken:
            def get_full_report(analytics):
                return analytics.reports().batchGet(
                    body={
                        'reportRequests': [
                            {
                              'viewId': VIEW_ID,
                              'pageSize': 1000000000, # set to a value well above true limit: 100000
                              'pageToken': Token,
                              'dateRanges': [{'startDate': day, 'endDate': day}],
                              'metrics': [{'expression': 'ga:totalEvents'},
                                          {'expression': 'ga:uniqueEvents'}],
                              'dimensions': [{'name': 'ga:dateHourMinute'},
                                             {'name': 'ga:eventAction'},
                                             {'name': 'ga:eventCategory'},
                                             {'name': 'ga:eventLabel'},
                                             {'name': 'ga:mobileDeviceModel'},
                                             {'name': 'ga:sessionCount'},
                                             {'name': 'ga:operatingSystemVersion'}]
                            }
                        ]
                    }
                ).execute()
            response = get_full_report(analytics)

            """Parses and prints the Analytics Reporting API V4 response.

            Args:
              response: An Analytics Reporting API V4 response.
            """

            for report in response.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])
                col_dict['account'].append(account)

                for header, dimension in zip(dimensionHeaders, dimensions):
                    col_dict[header[3:]].append(dimension)

                for i, values in enumerate(dateRangeValues):
                    col_dict["totalEvents"].append(values["values"][0])
                    col_dict["uniqueEvents"].append(values["values"][1])

# end of account for loop

# final dataframe
df = pd.DataFrame(col_dict) 
print(df.head())


"""Writing to SQL from DF"""

# reading paramaters from python_APItoSQL_parameters.txt
parameter_dict = read_parameter()

username = parameter_dict['username']
password = parameter_dict['password']
database = parameter_dict['database']
server_name = parameter_dict['server_name']
table_name = parameter_dict['table_name']
table_status = parameter_dict['table_status']


def insert_with_progress(df, con_engine, table_status, table_name):
    # Function for insertion into sql table
    chunk_size = 5000

    with tqdm(total=len(df)) as pbar:

        for i, small_df in enumerate(df_chunck(df,chunk_size)):
            replace = table_status if i == 0 else 'append'
            small_df.to_sql(name=table_name, con=con_engine, if_exists=replace, index=False)
            pbar.update(chunk_size)

    return


def df_chunck(df, chunck_size):
    # function which defines the number of rows to insert with each chunk
    return [df[pos:pos+chunck_size] for pos in range(0,len(df),chunck_size)]

# Credentials to connect to database
# username = input('Give User Name i.e CABLE\<username> : ')
# password = getpass.getpass('Password: ')


engine = sqlalchemy.create_engine('mssql+pymssql://'+username+':'+password+'@'+server_name+'/' + database)

# working on sending from dataframe to sql
print('                   -----Sending dataframe to SQL-----')

insert_with_progress(df,engine,table_status,table_name)

# total run time
elapsed_time_secs = time.time() - start_time

msg = "Total execution took: %s secs (Wall clock time)" % timedelta(seconds=round(elapsed_time_secs))

print(msg)


# Optional block for sending notification email
"""
import smtplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
 
email_password = parameter_dict['email_password']

fromaddr = "scottdetweiler0423@gmail.com" 
toaddr_list = ["scott.detweiler@northhighland.com", "scottdetweilerjr@hotmail.com"]
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['Subject'] = "Weekly GA API Call and SQL Import was Successful"

body = ("Data imported to SQL for date range: "+str(start_date)+" to "+str(end_date))

msg.attach(MIMEText(body, 'plain'))
 
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(fromaddr, email_password)
text = msg.as_string()

for toaddr in toaddr_list:
    msg['To'] = toaddr
    server.sendmail(fromaddr, toaddr, text)
server.quit()
"""
