# Package used to connect to MySQL Databases
import mysql.connector

# XML Creation
import xml.etree.ElementTree as ET
import os

#Upload XML
import requests
import shutil

# Data Manipulation Packages
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime

# Package To Ignore Warnings
import warnings
warnings.filterwarnings("ignore")

Root = os.path.normpath(os.getcwd() + os.sep + os.pardir)

file = open(Root + '/Connect/Connect.txt', 'r')
text = file.readlines()

# Code To Connect MySQL
cobi_betika = mysql.connector.connect(host=text[0].strip()
                                      ,database=text[7].strip()
                                      ,user=text[5].strip()
                                      ,password=text[6].strip()
                                      ,port=text[4].strip())

# Connect to MySQL database
try:
    with cobi_betika.cursor() as cursor:
        df = pd.read_sql("SELECT summary_date\
                                ,COUNT(DISTINCT profile_id) AS playerCnt\
                                ,COUNT(DISTINCT (CASE WHEN free_bet_qty > 0 THEN profile_id END)) as freePlayerCnt\
                                ,game_name AS gameName\
                                ,SUM(total_bet_qty) AS gamesPlayedCnt\
                                ,SUM(total_bet_amt) AS stakeAmt\
                                ,SUM(total_payout) AS baseWinAmt\
                                ,SUM(withheld_tax_amt) AS taxPlayerIncomeAmt\
                                ,SUM(cancelled_bet_amt) AS refundAmt\
                                ,SUM(GGR) AS revenueAmt\
                            FROM betika_bi_ken.f_spribe_kpi\
                            WHERE DATE(summary_date) = DATE(CURDATE()- INTERVAL 1 DAY)\
                            GROUP BY summary_date\
                                    ,game_name;"
                         ,cobi_betika)
        
finally:
    cobi_betika.close()

Month = df['summary_date'][0].strftime('%m') + '_' + df['summary_date'][0].strftime('%B') + '/'
Year  = df['summary_date'][0].strftime('%Y') + '/'
Day   = df['summary_date'][0].strftime('%Y_%m_%d')

name        = '?xml version="1.0" encoding="UTF-8" standalone="yes"?'
date        = df['summary_date'].min().strftime('%Y%m%d')
counter     = 1
title       = 'RRI_gameSummary' 
report_path = Root +'/'+ 'Reports/Online_Casino_Games/'+Year + Month +date

if not os.path.exists(report_path):
        os.makedirs(report_path)

for file in os.listdir(report_path):
        if "xml" in file:
            counter += 1

serial = f"{counter:03}"

df['opener']            = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
df['xmlns:rri']         = 'urn:GSA:RRI:1.0:GSA:rri'
df['xmlns:xsi']         = 'http://www.w3.org/2001/XMLSchema-instance'
df['version']           = '1.0'
df['partionid']         = '1'
df['SeqNumber']         = '1'
df['OperatorId']        = 'Betika'
df['periodType']        = 'RRI_day'
df['reportType']        = 'RRI_gameSummary'
df['dateTime']          = datetime.now().strftime('%Y-%m-%dT%M:%S.%T')
df['brandID']           = 'Betika'
df['game_id']           = np.nan
df['gameType']          = 'RRI_other'
df['gameCategory']      = 'RRI_casinoGame'
df['gameVariant']       = 'RRI_none'
df['jackpotContribAmt'] = np.nan
df['jackpotWinAmt']     = np.nan
df['freeStakeAmt']      = np.nan
df['currencyCode']      = 'KES'

df['transType_Wager']   = 'RRI_wager'
df['transType_Win']     = 'RRI_baseGameWin'
df['transClass']        = 'RRI_online'
df['fundsType']         = 'RRI_realMoney'

# Create New dataframe that will be used to group data by game id
df_Hash = df

# Place all the row's data into one column
df_Hash['data'] = str(df_Hash).encode()

# convert data column to string 
df_Hash['data'] = df_Hash['data'].astype(str)

# 
df_Hash['data'] = df_Hash.groupby(['gameName'])['data'].transform(lambda x: ', '.join(x))

# 
df_Hash = df_Hash.groupby('gameName').first().reset_index()

# Keep only the data field & game id
df_Hash = df_Hash[['gameName','data']]

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
period_Start            = df['summary_date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
version                 = df['version'].astype(str).tolist()
partion_id              = df['partionid'].astype(str).tolist()
seq_Number              = df['SeqNumber'].astype(str).tolist()
period_Type             = df['periodType'].astype(str).tolist()
date_Time               = df['dateTime'].astype(str).tolist()
report_Type             = df['reportType'].astype(str).tolist()
brand_Id                = df['brandID'].astype(str).tolist()
game_Id                 = df['game_id'].astype(str).tolist()
game_Name               = df['gameName'].astype(str).tolist()
game_Category           = df['gameCategory'].astype(str).tolist()
game_Type               = df['gameType'].astype(str).tolist()
game_Variant            = df['gameVariant'].astype(str).tolist()
stake_Amt               = df['stakeAmt'].astype(str).tolist()
free_Stake_Amt          = df['freeStakeAmt'].astype(str).tolist()
jackpot_Contrib_Amt     = df['jackpotContribAmt'].astype(str).tolist()
jackpot_Win_Amt         = df['jackpotWinAmt'].astype(str).tolist()
games_Played_Cnt        = df['gamesPlayedCnt'].astype(str).tolist()
currency_Code           = df['currencyCode'].astype(str).tolist()
player_Cnt              = df['playerCnt'].astype(str).tolist()
free_Player_Cnt         = df['freePlayerCnt'].astype(str).tolist()
refund_Amt              = df['refundAmt'].astype(str).tolist()
wager_Amt               = df['stakeAmt'].astype(str).tolist()
base_Win_Amt            = df['baseWinAmt'].astype(str).tolist()
revenue_Amt             = df['revenueAmt'].astype(str).tolist()
tax_Player_Income_Amt   = df['taxPlayerIncomeAmt'].astype(str).tolist()
funds_Type              = df['fundsType'].astype(str).tolist()
trans_Class             = df['transClass'].astype(str).tolist()
trans_type_Wager        = df['transType_Wager'].astype(str).tolist()
trans_type_Win          = df['transType_Win'].astype(str).tolist()
funds_Type              = df['fundsType'].astype(str).tolist()
trans_Amt_Wager         = df['stakeAmt'].astype(str).tolist()
trans_Amt_Win           = df['baseWinAmt'].astype(str).tolist()

def prettify(element, indent='  '):
    queue = [(0, element)]
    while queue:
        level, element = queue.pop(0)
        children = [(level + 1, child) for child in list(element)]
        if children:
            element.text = '\n' + indent * (level+1)
        if queue: 
            element.tail = '\n' + indent * queue[0][0]
        else:
            element.tail = '\n' + indent * (level-1) 
        queue[0:0] = children

xml_doc = ET.Element(name)
gaming_Report = ET.SubElement(xml_doc, 'rri:gamingReport'
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
game_summary = ET.SubElement(gaming_Report, 'gameSummary' 
                             ,periodType=period_Type[0]
                             ,periodStart=period_Start[0]
                             ,brandId=brand_Id[0])
ET.SubElement(game_summary,'game'
              ,gameId=game_Id[0]
              ,gameName=game_Name[0]
              ,gameCategory=game_Category[0]
              ,gameType=game_Type[0]
              ,gameVariant=game_Variant[0])
game_sd = ET.SubElement(game_summary, 'gameSummaryDetail')
ET.SubElement(game_sd,'casinoGameSummary'
             ,stakeAmt=stake_Amt[0]
             ,freeStakeAmt=free_Stake_Amt[0]
             ,refundAmt=refund_Amt[0]
             ,baseWinAmt=base_Win_Amt[0]
             ,jackpotContribAmt=jackpot_Contrib_Amt[0]
             ,jackpotWinAmt=jackpot_Win_Amt[0]
             ,gamesPlayedCnt=games_Played_Cnt[0]
             ,currencyCode=currency_Code[0])
game_ml = ET.SubElement(game_summary, 'gameMovementList')
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_type_Wager[0]
             ,fundsType=funds_Type[0]
             ,transAmt=trans_Amt_Wager[0]
             ,currencyCode=currency_Code[0])
game_ml = ET.SubElement(game_summary, 'gameMovementList')
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_type_Win[0]
             ,fundsType=funds_Type[0]
             ,transAmt=trans_Amt_Win[0]
             ,currencyCode=currency_Code[0])

prettify(xml_doc)

file_path = report_path+'/'+title+'_'+serial+'_'+date+'.xml'
zip_name = report_path+'/'+title+'_'+serial+'_'+date
zip_path = report_path+'/'+title+'_'+serial+'_'+date+'.zip'

tree = ET.ElementTree(xml_doc)
tree.write(file_path,encoding="utf-8")

shutil.make_archive(zip_name, 'zip', report_path)

package_name = title+'_'+serial+'_'+date+'.zip'
file_size = os.path.getsize(file_path)
chunk_size = os.path.getsize(zip_path)
offset = 0