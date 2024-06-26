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
report_path = Root +'/'+ 'Reports/playerRegistration/'+Year + Month +date

base_filename = "playerRegistration"
max_size_mb = 64

zip_name = report_path+'/'+base_filename+'_'+date
zip_path = report_path+'/Zip/'+base_filename+'_'+date+'.zip'
zip_dir = report_path+'/Zip'
file_path = report_path+'/'+base_filename+'_001_'+date+'.xml'

if not os.path.exists(report_path):
        os.makedirs(report_path)

if not os.path.exists(zip_dir):
        os.makedirs(zip_dir)

cobi_betika = mysql.connector.connect(host=text[0].strip()
                                      ,database=text[7].strip()
                                      ,user=text[5].strip()
                                      ,password=text[6].strip()
                                      ,port=text[4].strip())

# Connect to MySQL database
try:
    with cobi_betika.cursor() as cursor:
        df = pd.read_sql("SELECT a.playerID\
                                ,a.transDateTime\
                                ,b.msisdn AS phoneNumber\
                                ,left(b.msisdn,3) AS phoneCountry\
                            FROM (SELECT profile_id AS playerID\
                                        ,registration_date AS transDateTime\
                                FROM betika_bi_mw.dim_first_last_mw\
                                WHERE DATE(registration_date) = DATE(CURDATE()- INTERVAL 1 DAY)\
                                ) AS a\
\
                            LEFT JOIN betika_bi_mw.profile AS b\
                            ON a.playerID = b.profile_id;"
                         ,cobi_betika)
        
finally:
    cobi_betika.close()

df['opener']            = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
df['xmlns:rri']         = 'urn:GSA:RRI:1.0:GSA:rri'
df['xmlns:xsi']         = 'http://www.w3.org/2001/XMLSchema-instance'
df['version']           = '1.0'
df['partionid']         = '1'
df['OperatorId']        = 'Betika'
df['periodType']        = 'RRI_day'
df['reportType']        = 'RRI_playerRegistration'
df['SeqNumber']         = '1'
df['dateTime']          = datetime.now().strftime('%Y-%m-%dT%M:%S.%T')

# PlayerRegistration
df['brandID']           = 'Betika'

# Playerinfo
df['givenNames']        = np.nan
df['familyNames']       = np.nan
df['Nationality']       = np.nan
df['birthDate']         = np.nan
df['genderType']        = np.nan


# PlayerStatusinfo
df['playerStatus']      = 'RRI_active'
df['operatorStatus']    = 'ACTV'

# playerPhoneList
df['phoneType']         = 'RRI_mobile'

#playerIdentification
df['idType']           = np.nan
df['countryCode']      = 'UG'
df['subDivision']      = np.nan
df['idNumber']         = np.nan
df['issueDate']        = np.nan
df['expirationDate']   = np.nan

# playerExclusionList
df['exclusionSource']  = 'RRI_player'
df['startDate']        = np.nan
df['endDate']          = np.nan
df['fromTime']         = np.nan
df['toTime']           = np.nan
df['autoContinuance']  = np.nan
df['removalDateTime']  = np.nan

# Create New dataframe that will be used to group data by game id
df_Hash = df

# Place all the row's data into one column
df_Hash['data'] = str(df_Hash).encode()

# convert data column to string 
df_Hash['data'] = df_Hash['data'].astype(str)

# 
df_Hash['data'] = df_Hash.groupby(['transDateTime'])['data'].transform(lambda x: ', '.join(x))

# 
df_Hash = df_Hash.groupby('transDateTime').first().reset_index()

# Keep only the data field & game id
df_Hash = df_Hash[['transDateTime','data']]

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
period_Start            = df['transDateTime'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
version                 = df['version'].astype(str).tolist()
partion_id              = df['partionid'].astype(str).tolist()
seq_Number              = df['SeqNumber'].astype(str).tolist()
period_Type             = df['periodType'].astype(str).tolist()
date_Time               = df['dateTime'].astype(str).tolist()
report_Type             = df['reportType'].astype(str).tolist()
brand_Id                = df['brandID'].astype(str).tolist()

# PlayerRegistration
brand_id                = df['brandID'].astype(str).tolist()
trans_Date_Time         = df['transDateTime'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()

# player
player_id               = df['playerID'].astype(str).tolist()

# Playerinfo
given_Names             = df['givenNames'].astype(str).tolist()
family_Names            = df['familyNames'].astype(str).tolist()
Nationality             = df['Nationality'].astype(str).tolist()
birth_Date              = df['birthDate'].astype(str).tolist()
gender_Type             = df['genderType'].astype(str).tolist()
from_Date_Time          = df['transDateTime'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()


# PlayerStatusinfo
player_Status           = df['playerStatus'].astype(str).tolist()
operator_Status         = df['operatorStatus'].astype(str).tolist()
verified_Date_Time      = df['transDateTime'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()

# playerPhoneList
phone_Type              = df['phoneType'].astype(str).tolist()
phone_Country           = df['phoneCountry'].astype(str).tolist()
phone_Number            = df['phoneNumber'].astype(str).tolist()
verified_Date_Time      = df['transDateTime'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()

#playerIdentification
id_Type                 = df['idType'].astype(str).tolist()
country_Code            = df['countryCode'].astype(str).tolist()
sub_Division            = df['subDivision'].astype(str).tolist()
id_Number               = df['idNumber'].astype(str).tolist()
issue_Date              = df['issueDate'].astype(str).tolist()
expiration_Date         = df['expirationDate'].astype(str).tolist()

# playerExclusionList
exclusion_Source        = df['exclusionSource'].astype(str).tolist()
start_Date              = df['startDate'].astype(str).tolist()
end_Date                = df['endDate'].astype(str).tolist()
from_Time               = df['fromTime'].astype(str).tolist()
to_Time                 = df['toTime'].astype(str).tolist()
auto_Continuance        = df['autoContinuance'].astype(str).tolist()
removal_Date_Time       = df['removalDateTime'].astype(str).tolist()

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
    
    return root

def save_xml_to_file(root, filename):
    # Create an ElementTree object from the root element
    tree = ET.ElementTree(root)
    # Write the tree to an XML file
    tree.write(filename, encoding='utf-8', xml_declaration=False)

def add_element(root, tag, text):
    
    # Create a new element and add it to the root
    
    playerRegistration = ET.SubElement(root, 'playerRegistration' 
                             ,brandId=brand_Id[0]
                             ,transDateTime=trans_Date_Time[0])

    for i in range(len(player_id)):
        
        
        ET.SubElement(playerRegistration,'player'
                    ,playerId=player_id[i])
        ET.SubElement(playerRegistration,'playerInfo'
                    ,givenNames=given_Names[i]
                    ,familyNames=family_Names[i]
                    ,nationality=Nationality[i]
                    ,birthDate=birth_Date[i]
                    ,genderType=gender_Type[i]
                    ,fromDateTime=date_Time[i])
        playerStatusInfo = ET.SubElement(playerRegistration, 'playerStatusInfo' 
                                        ,playerStatus=player_Status[i]
                                        ,operatorStatus=operator_Status[i]
                                        ,fromDateTime=from_Date_Time[i]
                                        ,verifiedDateTime=verified_Date_Time[i])
        playerPhoneList = ET.SubElement(playerRegistration, 'playerPhoneList')
        ET.SubElement(playerPhoneList,'playerPhone'
                    ,phoneType=phone_Type[i]
                    ,phoneCountry=phone_Country[i]
                    ,phoneNumber=phone_Number[i]
                    ,verifiedDateTime=verified_Date_Time[i])
        playerIdentificationList = ET.SubElement(playerRegistration, 'playerIdentificationList')
        ET.SubElement(playerIdentificationList,'playerIdentification'
                    ,idType=id_Type[i]
                    ,countryCode=country_Code[i]
                    ,subDivision=sub_Division[i]
                    ,idNumber=id_Number[i]
                    ,issueDate=issue_Date[i]
                    ,expirationDate=expiration_Date[i])
        playerExclusion = ET.SubElement(playerRegistration, 'playerExclusionList')
        ET.SubElement(playerExclusion,'playerExclusion'
                    ,exclusionSource=exclusion_Source[i]
                    ,startDate=start_Date[i]
                    ,endDate=end_Date[i]
                    ,fromTime=from_Time[0]
                    ,toTime=to_Time[i]
                    ,autoContinuance=auto_Continuance[i]
                    ,removalDateTime=removal_Date_Time[i])

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
            if file.is_file() and file.suffix == '.xml':  # Only zip regular files
                zipf.write(file, file.name)

zip_files_in_folder(zip_dir, zip_path)

print('Test Successful')
