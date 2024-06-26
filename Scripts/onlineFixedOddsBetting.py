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

cobi_betika = mysql.connector.connect(host=text[0].strip()
                                      ,database=text[7].strip()
                                      ,user=text[5].strip()
                                      ,password=text[6].strip()
                                      ,port=text[4].strip())

# Connect to MySQL database
try:
    with cobi_betika.cursor() as cursor:
        df = pd.read_sql("SELECT a.summary_date\
                                ,a.players\
                                ,a.betsPlacedCnt\
                                ,b.gamesPlayedCnt\
                                ,a.StakeAmt\
                                ,a.freeStakeAmt\
                                ,a.baseWinAmt\
                                ,a.taxPlayerIncomeAmt\
                                ,a.revenueAmt\
                                ,a.transAmt_c_wager\
                                ,a.transAmt_c_win\
                                ,a.transAmt_b_wager\
                                ,a.transAmt_b_win\
                                ,a.trans_Amt_r\
\
                            FROM (SELECT summary_date\
                                        ,COUNT(DISTINCT profile_id) AS players\
                                        ,SUM(total_bet_qty) AS betsPlacedCnt\
                                        ,SUM(total_to) AS StakeAmt\
                                        ,SUM(free_to) AS freeStakeAmt\
                                        ,SUM(cancelled_to) AS refundAmt\
                                        ,SUM(total_payout) AS baseWinAmt\
                                        ,SUM(withheld_tax) AS taxPlayerIncomeAmt\
                                        ,SUM(cancelled_to) AS trans_Amt_r \
                                        ,SUM(cash_to) AS transAmt_c_wager\
                                        ,SUM(cash_payout) AS transAmt_c_win\
                                        ,SUM(total_bonus_to) AS transAmt_b_wager\
                                        ,SUM(bonus_payout) AS transAmt_b_win\
                                        ,SUM(GGR) AS revenueAmt\
                                \
                            FROM betika_bi_gh.f_sp_kpi_gh\
                            WHERE DATE(summary_date) = DATE(CURDATE()- INTERVAL 1 DAY)\
                            GROUP BY summary_date\
                                ) AS a\
\
                            LEFT JOIN (SELECT DATE(a.created) AS summary_date\
                                            ,SUM(b.total_games) AS gamesPlayedCnt\
                                    FROM betika_bi_gh.bet AS a\
\
                                    LEFT JOIN betika_bi_gh.bet_slip_new AS b\
                                    ON a.bet_id = b.bet_id\
                                                                    \
                                    WHERE DATE(a.created) = DATE(CURDATE()- INTERVAL 1 DAY)\
\
                                    GROUP BY DATE(a.created)\
\
                                    ) AS b\
\
                            ON a.summary_date = b.summary_date;"
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
report_path = Root +'/'+ 'Reports/onlineFixedOddsBetting/'+Year + Month +date

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

# Game
df['game_id']           = np.nan
df['gameName']          = np.nan
df['gameCategory']      = 'RRI_fixedOdds'
df['gameType']          = 'RRI_sports'
df['gameVariant']       = np.nan

# GameSummaryDetail
# fixedOddsSummary

df['adjustmentAmt']     = np.nan
df['betsAdjustedCnt']   = np.nan


df['refundAmt']         = np.nan
df['forfeitAmt']        = np.nan
df['betsRefundedCnt']   = np.nan
df['betsWonCnt']        = np.nan
df['fundsType']         = np.nan

# gameMovementList
df['currencyCode']     = 'UGX'
df['free_players']     = np.nan
df['total_to']         = np.nan

# gameMovement
df['transClass']       = 'RRI_online'
df['transType_refund'] = 'RRI_refund'
df['transType_wager']  = 'RRI_wager'
df['transType_win']    = 'RRI_baseGameWin'
df['fundsType_c']      = 'RRI_realMoney'
df['fundsType_b']      = 'RRI_bonusMoney'

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
period_Start            = df['summary_date'].apply(lambda x: x.strftime('%Y-%m-%d')).tolist()
version                 = df['version'].astype(str).tolist()
partion_id              = df['partionid'].astype(str).tolist()
seq_Number              = df['SeqNumber'].astype(str).tolist()
period_Type             = df['periodType'].astype(str).tolist()
date_Time               = df['dateTime'].astype(str).tolist()
report_Type             = df['reportType'].astype(str).tolist()
brand_Id                = df['brandID'].astype(str).tolist()

# Game
game_Id                 = df['game_id'].astype(str).tolist()
game_Name               = df['gameName'].astype(str).tolist()
game_Category           = df['gameCategory'].astype(str).tolist()
game_Type               = df['gameType'].astype(str).tolist()
game_Variant            = df['gameVariant'].astype(str).tolist()

# GameSummaryDetail
# fixedOddsSummary
stake_Amt               = df['total_to'].astype(str).tolist()
free_Stake_Amt          = df['freeStakeAmt'].astype(str).tolist()
refund_Amt              = df['refundAmt'].astype(str).tolist()
forfeit_Amt             = df['forfeitAmt'].astype(str).tolist()
base_Win_Amt            = df['baseWinAmt'].astype(str).tolist()

adjustment_Amt          = df['adjustmentAmt'].astype(str).tolist()

bets_Placed_Cnt          = df['betsPlacedCnt'].astype(str).tolist()
bets_Refunded_Cnt        = df['betsRefundedCnt'].astype(str).tolist()
bets_Won_Cnt             = df['betsWonCnt'].astype(str).tolist()

bets_Adjusted_Cnt        = df['betsAdjustedCnt'].astype(str).tolist()

revenue_Amt             = df['revenueAmt'].astype(str).tolist()
tax_Player_Income_Amt   = df['taxPlayerIncomeAmt'].astype(str).tolist()
currency_Code           = df['currencyCode'].astype(str).tolist()
funds_Type              = df['fundsType'].astype(str).tolist()

# gameMovementList
games_Played_Cnt        = df['gamesPlayedCnt'].astype(str).tolist()
player_Cnt              = df['players'].astype(str).tolist()
free_Player_Cnt         = df['free_players'].astype(str).tolist()
refund_Amt              = df['refundAmt'].astype(str).tolist()
wager_Amt               = df['total_to'].astype(str).tolist()

# gameMovement
trans_Class             = df['transClass'].astype(str).tolist()

funds_Type_cash         = df['fundsType_c'].astype(str).tolist()
funds_Type_bonus        = df['fundsType_b'].astype(str).tolist()

trans_Type_refund       = df['transType_refund'].astype(str).tolist()
trans_Type_wager        = df['transType_wager'].astype(str).tolist()
trans_Type_win          = df['transType_win'] .astype(str).tolist()

trans_Amt_r             = df['trans_Amt_r'].astype(str).tolist()
trans_Amt_c_wager       = df['transAmt_c_wager'].astype(str).tolist()
trans_Amt_c_win         = df['transAmt_c_win'].astype(str).tolist()
trans_Amt_b_wager       = df['transAmt_b_wager'].astype(str).tolist()
trans_Amt_b_win         = df['transAmt_b_win'].astype(str).tolist()

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
                             ,periodStart=period_Start[0])
ET.SubElement(game_summary,'game'
              ,gameId=game_Id[0]
              ,gameName=game_Name[0]
              ,gameCategory=game_Category[0]
              ,gameType=game_Type[0]
              ,gameVariant=game_Variant[0])
game_sd = ET.SubElement(game_summary, 'gameSummaryDetail')
ET.SubElement(game_sd,'fixedOddsSummary'
             ,stakeAmt=stake_Amt[0]
             ,freeStakeAmt=free_Stake_Amt[0]
             ,refundAmt=refund_Amt[0]
             ,baseWinAmt=base_Win_Amt[0]
             ,adjustmentAmt=adjustment_Amt[0]
             ,betsPlacedCnt=bets_Placed_Cnt[0]
             ,betsRefundedCnt=bets_Refunded_Cnt[0]
             ,betsWonCnt=bets_Won_Cnt[0]
             ,betsAdjustedCnt=bets_Adjusted_Cnt[0]
             ,revenueAmt=revenue_Amt[0]
             ,taxPlayerIncomeAmt=tax_Player_Income_Amt[0]
             ,currencyCode=currency_Code[0]
             ,fundsType=funds_Type[0])
game_ml = ET.SubElement(game_summary, 'gameMovementList')
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_Type_wager[0]
             ,fundsType=funds_Type_cash[0]
             ,transAmt=trans_Amt_c_wager[0]
             ,currencyCode=currency_Code[0])
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_Type_win[0]
             ,fundsType=funds_Type_cash[0]
             ,transAmt=trans_Amt_c_win[0]
             ,currencyCode=currency_Code[0])
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_Type_wager[0]
             ,fundsType=funds_Type_bonus[0]
             ,transAmt=trans_Amt_b_wager[0]
             ,currencyCode=currency_Code[0])
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_Type_win[0]
             ,fundsType=funds_Type_bonus[0]
             ,transAmt=trans_Amt_b_win[0]
             ,currencyCode=currency_Code[0])
ET.SubElement(game_ml,'gameMovement'
             ,transClass=trans_Class[0]
             ,transType=trans_Type_refund[0]
             ,fundsType=funds_Type_cash[0]
             ,transAmt=trans_Amt_r[0]
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

reqUrl = "http://localhost:14077/ems/trigger/"

post_files = {
  "file": open(file_path, "rb"),
}

payload = {'packageName': package_name,
           'size': file_size,
           'chunkSize': chunk_size,
           'offSet': offset}

response = requests.request("POST", reqUrl, data=payload, files=post_files)

print(response.text)