# Package used to connect to MySQL Databases
import mysql.connector

# XML Creation
import xml.etree.ElementTree as ET
from collections import defaultdict
import os

#Upload XML
import requests
from zipfile import ZipFile, ZIP_DEFLATED
import pathlib

# Data Manipulation Packages
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime, timedelta

# Package To Ignore Warnings
import warnings
warnings.filterwarnings("ignore")

Root = os.path.normpath(os.getcwd() + os.sep + os.pardir)

file = open(Root + '/Connect/Connect.txt', 'r')
text = file.readlines()

Month = (datetime.now() - timedelta(days=1)).strftime('%m') + '_' + (datetime.now() - timedelta(days=1)).strftime('%B') + '/'
Year  = (datetime.now() - timedelta(days=1)).strftime('%Y') + '/'
Day   = (datetime.now() - timedelta(days=1)).strftime('%Y_%m_%d')

name        = '?xml version="1.0" encoding="UTF-8" standalone="yes"?'
date        = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
title       = 'RRI_playerActivity' 
report_path = Root +'/'+ 'Reports/playerActivity/'+Year + Month +date

base_filename = "playerActivity"
max_size_mb = 64

zip_name = report_path+'/'+base_filename+'_'+date
zip_path = report_path+'/Zip/'+base_filename+'_'+date+'.zip'
zip_dir = report_path+'/Zip'
file_path = report_path+'/'+base_filename+'_001_'+date+'.xml'

if not os.path.exists(report_path):
        os.makedirs(report_path)

if not os.path.exists(zip_dir):
        os.makedirs(zip_dir)

# Code To Connect MySQL
cobi_betika = mysql.connector.connect(host=text[0].strip()
                                      ,database=text[7].strip()
                                      ,user=text[5].strip()
                                      ,password=text[6].strip()
                                      ,port=text[4].strip())

# Connect to MySQL database
try:
    with cobi_betika.cursor() as cursor:
        df = pd.read_sql("SELECT x.profile_id \
                                ,(x.opening_depostit_amt - x.opening_withdrawal_amt) AS openingBalance\
                                ,(x.closing_depostit_amt - x.closing_withdrawal_amt) AS closingBalance\
                                ,x.depostit_amt\
                                ,x.withdrawal_amt\
                                ,x.summary_date\
                                ,x.registration_date\
                                ,x.payment_method\
\
                            FROM (SELECT a.profile_id \
                                        ,SUM(CASE WHEN DATE(a.summary_date) <= DATE(CURDATE()- INTERVAL 2 DAY) THEN a.deposit_amt END)      AS opening_depostit_amt\
                                        ,SUM(CASE WHEN DATE(a.summary_date) <= DATE(CURDATE()- INTERVAL 2 DAY) THEN a.withdrawal_amt END)   AS opening_withdrawal_amt\
                                        ,SUM(CASE WHEN DATE(a.summary_date) <= DATE(CURDATE()- INTERVAL 1 DAY) THEN a.deposit_amt END)      AS closing_depostit_amt\
                                        ,SUM(CASE WHEN DATE(a.summary_date) <= DATE(CURDATE()- INTERVAL 1 DAY) THEN a.withdrawal_amt END)   AS closing_withdrawal_amt\
                                        ,SUM(CASE WHEN DATE(a.summary_date) =  DATE(CURDATE()- INTERVAL 1 DAY) THEN a.deposit_amt END)      AS depostit_amt\
                                        ,SUM(CASE WHEN DATE(a.summary_date) =  DATE(CURDATE()- INTERVAL 1 DAY) THEN a.withdrawal_amt END)   AS withdrawal_amt\
                                        ,MAX(CASE WHEN DATE(a.summary_date) =  DATE(CURDATE()- INTERVAL 1 DAY) THEN a.summary_date END)     AS summary_date\
                                        ,b.registration_date\
                                        ,a.payment_method\
                                FROM betika_bi_mw.f_cashier_mw AS a \
\
                                LEFT JOIN betika_bi_mw.dim_first_last_mw AS b\
                                ON a.profile_id = b.profile_id\
\
                                GROUP BY profile_id\
                                        ,b.registration_date\
                                        ,a.payment_method\
                                ) AS x;"
                         ,cobi_betika)
        
finally:
    cobi_betika.close()

df.update(df[['openingBalance','closingBalance','depostit_amt','withdrawal_amt']].fillna(0))
df['summary_date'].fillna(df['registration_date'], inplace=True)

df['opener']            = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
df['xmlns:rri']         = 'urn:GSA:RRI:1.0:GSA:rri'
df['xmlns:xsi']         = 'http://www.w3.org/2001/XMLSchema-instance'
df['version']           = '1.0'
df['partionid']         = '1'
df['SeqNumber']         = '1'
df['OperatorId']        = 'Betika'
df['periodType']        = 'RRI_day'
df['reportType']        = 'RRI_playerActivity'
df['dateTime']          = datetime.now().strftime('%Y-%m-%dT%M:%S.%T')
df['period_Start']      = date


# PlayerActivity
df['brandID']           = 'Betika'

# PlayerActivityDetail
df['ActivityDescOW']    = 'Player Account Opened'
df['ActivityDescDP']    = 'RRI_deposit'
df['ActivityDescWD']    = 'RRI_withdrawal'
df['fundsType']         = 'RRI_realMoney'
df['currencyCode']      = 'UGX'
df['transClass']        = 'RRI_online'

# Create New dataframe that will be used to group data by game id
df_Hash = df

# Place all the row's data into one column
df_Hash['data'] = str(df_Hash).encode()

# convert data column to string 
df_Hash['data'] = df_Hash['data'].astype(str)

# 
df_Hash['data'] = df_Hash.groupby(['summary_date'])['data'].transform(lambda x: ', '.join(x))

# 
df_Hash = df_Hash.groupby('summary_date').first().reset_index()

# Keep only the data field & game id
df_Hash = df_Hash[['summary_date','data']]

def hash_data(data):
    
    # Encode the transaction data as a string
    data_str = str(data).encode()

    # Create a hash variable and use it to generate a hash of the transaction data
    hashing = hashlib.sha256()
    hashing.update(data_str)
    data_hash = hashing.hexdigest()

    return data_hash

df_Hash['reportId'] = df_Hash['data'].apply(hash_data)

opener                  = df['opener'].astype(str).tolist()
rri                     = df['xmlns:rri'].astype(str).tolist()
xsi                     = df['xmlns:xsi'].astype(str).tolist()
report_id               = df_Hash['reportId'].astype(str).tolist()
Operator_Id             = df['OperatorId'].astype(str).tolist()
period_Start            = df['period_Start'].astype(str).tolist()
version                 = df['version'].astype(str).tolist()
partion_id              = df['partionid'].astype(str).tolist()
seq_Number              = df['SeqNumber'].astype(str).tolist()
period_Type             = df['periodType'].astype(str).tolist()
date_Time               = df['dateTime'].astype(str).tolist()
report_Type             = df['reportType'].astype(str).tolist()
brand_Id                = df['brandID'].astype(str).tolist()

# Player
player_id               = df['profile_id'].astype(str).tolist()
Activity_Date_Time      = df['period_Start'].astype(str).tolist()

# playerActivityDetail
player_Activity_Desc    = df['payment_method'].astype(str).tolist()
player_Activity_Desc_WD = df['ActivityDescWD'].astype(str).tolist()
currency_Code           = df['currencyCode'].astype(str).tolist()

# gameMovement
trans_Class             = df['transClass'].astype(str).tolist()
funds_Type              = df['fundsType'].astype(str).tolist()

trans_Type_dep          = df['ActivityDescDP'].astype(str).tolist()
trans_Type_with         = df['ActivityDescWD'].astype(str).tolist()

trans_Amt_obl          = df['openingBalance'].astype(str).tolist()
trans_Amt_cbl          = df['closingBalance'].astype(str).tolist()
trans_Amt_dep          = df['withdrawal_amt'].astype(str).tolist()
trans_Amt_wth          = df['withdrawal_amt'].astype(str).tolist()

def create_xml_file(filename, serial):
    # Create the root element
    root = ET.Element('?xml version="1.0" encoding="UTF-8" standalone="yes"?')
    
    gaming_Report = ET.SubElement(root, 'rri:gamingReport',
                                  xmlnsrri=rri[0],
                                  xmlnsxsi=xsi[0],
                                  version=version[0],
                                  operatiorId=Operator_Id[0],
                                  partionId=partion_id[0],
                                  periodType=period_Type[0],
                                  periodStart=period_Start[0],
                                  reportType=report_Type[0],
                                  reportid=report_id[0],
                                  seqNumber=serial,
                                  dateTime=date_Time[0])
    
    return root

def save_xml_to_file(root, filename):
    # Create an ElementTree object from the root element
    tree = ET.ElementTree(root)
    # Write the tree to an XML file
    tree.write(filename, encoding='utf-8', xml_declaration=False)

def add_element(root, tag, text):
    
    # Create a new element and add it to the root
    
    gaming_Report = ET.SubElement(root, 'rri:gamingReport'
                                ,xmlnsrri=rri[0]
                                ,xmlnsxsi=xsi[0]
                                ,version=version[0]
                                ,operatiorId=Operator_Id[0]
                                ,partionId=partion_id[0]
                                ,periodType=period_Type[0]
                                ,periodStart=period_Start[0]
                                ,reportType=report_Type[0]
                                ,reportid=report_id[0]
                                ,seqNumber=seq_Number[0]
                                ,dateTime=date_Time[0])
    playerActivity = ET.SubElement(gaming_Report, 'playerActivity' 
                                ,brandId=brand_Id[0])

    for i in range(len(player_id)):
        
        ET.SubElement(playerActivity, 'player', playerId=player_id[i])
        
        ET.SubElement(playerActivity, 'playerActivityDetail',
                    playerActivity=trans_Type_dep[i],
                    playerActivityDesc=player_Activity_Desc[i],
                    playerActivityDateTime=Activity_Date_Time[i])
        
        startingBalanceList = ET.SubElement(playerActivity, 'startingBalanceList')
        
        ET.SubElement(playerActivity, 'summaryBalance',
                    fundsType=funds_Type[i],
                    balanceAmt=trans_Amt_obl[i],
                    currencyCode=currency_Code[i])
        
        playerMovementList = ET.SubElement(playerActivity, 'playerMovementList')
        
        ET.SubElement(playerMovementList, 'playerMovement',
                    transClass=trans_Class[i],
                    transType=trans_Type_dep[i],
                    fundsType=funds_Type[i],
                    transAmt=trans_Amt_dep[i],
                    currencyCode=currency_Code[i])
        
        endingBalanceList = ET.SubElement(playerActivity, 'endingBalanceList')
        
        ET.SubElement(endingBalanceList, 'summaryBalance',
                    fundsType=funds_Type[i],
                    balanceAmt=trans_Amt_cbl[i],
                    currencyCode=currency_Code[i])
        
        playerMovementList = ET.SubElement(playerActivity, 'playerMovementList')
        
        ET.SubElement(playerActivity, 'summaryBalance',
                    fundsType=funds_Type[i],
                    balanceAmt=trans_Amt_obl[i],
                    currencyCode=currency_Code[i])
        
        ET.SubElement(playerActivity, 'playerActivityDetail',
                    playerActivity=trans_Type_with[i],
                    playerActivityDesc=player_Activity_Desc[i],
                    playerActivityDateTime=Activity_Date_Time[i])
        
        playerMovementList = ET.SubElement(playerActivity, 'playerMovementList')
        
        ET.SubElement(playerMovementList, 'playerMovement',
                    transClass=trans_Class[i],
                    transType=trans_Type_with[i],
                    fundsType=funds_Type[i],
                    transAmt=trans_Amt_wth[i],
                    currencyCode=currency_Code[i])
        
        endingBalanceList = ET.SubElement(playerActivity, 'endingBalanceList')
        
        ET.SubElement(endingBalanceList, 'summaryBalance',
                    fundsType=funds_Type[i],
                    balanceAmt=trans_Amt_cbl[i],
                    currencyCode=currency_Code[i])

def manage_xml_files(base_filename, max_size_mb):
    current_file_index = 1
    current_file_size = 0
    serial = f"{current_file_index:03}"
    current_root = create_xml_file(f"{report_path}/{base_filename}_{serial}_{date}.xml", serial)
    
    while True:
        # Add your elements to the current_root as needed
        add_element(current_root, "item", "Some text")
        
        # Save to a temporary file to check the size
        temp_filename = f"{report_path}/temp_{base_filename}.xml"
        save_xml_to_file(current_root, temp_filename)
        
        current_file_size = os.path.getsize(temp_filename) / (1024 * 1024)  # Size in MB
        
        if current_file_size > max_size_mb:
            # Save the current file and start a new one
            final_filename = f"{report_path}/{base_filename}_{serial}_{date}.xml"
            save_xml_to_file(current_root, final_filename)
            os.remove(temp_filename)  # Clean up temporary file
            
            # Increment file index and reset root and size
            current_file_index += 1
            serial = f"{current_file_index:03}"
            current_root = create_xml_file(f"{report_path}/{base_filename}_{serial}_{date}.xml", serial)
        else:
            # Move the temp file to the final name
            final_filename = f"{report_path}/{base_filename}_{serial}_{date}.xml"
            os.rename(temp_filename, final_filename)
            break

def update_seq_number(xml_file, new_seq_number):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    namespaces = {'rri': 'http://example.com/rri', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
    gaming_report = root.find('rri:gamingReport', namespaces)
    if gaming_report is not None:
        gaming_report.set('seqNumber', new_seq_number)
        tree.write(xml_file, encoding='utf-8', xml_declaration=True)
    else:
        print("Element not found")

manage_xml_files(base_filename, max_size_mb)

def zip_files_in_folder(report_path, zip_path):
    
    folder = pathlib.Path(report_path)
    
    with ZipFile(zip_path, 'w', ZIP_DEFLATED) as zipf:
        for file in folder.iterdir():
            if file.is_file() and file.suffix == '.xml': 
                zipf.write(file, file.name)

zip_files_in_folder(zip_dir, zip_path)

print('Test Successful')