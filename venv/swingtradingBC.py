import requests
import pandas as pd
from pandas.tseries.offsets import BDay
from datetime import date
import subprocess
# from getDatabackup import telegram_bot_sendtext
import datetime
from math import ceil
import numpy
import matplotlib.pyplot as plt
import pandas as pd
import six
from pandas.plotting import table

headers = {
    'authority': 'www.nseindia.com',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
}

url_oc = "https://www.nseindia.com/option-chain"
session = requests.Session()
request = session.get(url_oc, headers=headers, timeout=20)
cookies = dict(request.cookies)


insider_url = "https://www.nseindia.com/api/corporates-pit"
shareholding_url = "https://www.nseindia.com/api/corporate-share-holdings-master"
equity_csv_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
sast_url = "https://www.nseindia.com/api/corporate-sast-reg29"
pledged_url = "https://www.nseindia.com/api/corporate-pledgedata"
bhav_url = "https://www1.nseindia.com/products/content/sec_bhavdata_full.csv"


today = date.today().strftime("%d-%m-%Y")
last30Days = (pd.datetime.today() - BDay(66)).strftime("%d-%m-%Y")

params = (
    ('index', 'equities'),
    ('from_date', last30Days),
    ('to_date', today),
)


def telegram_bot_sendHtml(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001380413685'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=HTML&text=' + bot_message

    sendRes = requests.get(send_text)
    # print(response)
    return sendRes.json()

def telegram_bot_sendtext(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001168164977'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message.replace('},','\n\t')

    response = requests.get(send_text)
    print(response)
    return response.json()

def render_mpl_table(data, col_width=4.0, row_height=0.625, font_size=14,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='w',
                     bbox=[0, 0, 1, 1], header_columns=0,
                     ax=None, **kwargs):
    if ax is None:
        size = (numpy.array(data.shape[::-1]) + numpy.array([0, 1])) * numpy.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax


def updateCookies():
    global cookies
    session = requests.Session()
    request = session.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)
    print("Updating cookies")


def send_image(botToken, imageFile, chat_id):
    command = 'curl -s -X POST https://api.telegram.org/bot' + botToken + '/sendPhoto -F chat_id=' + chat_id + " -F photo=@" + imageFile
    subprocess.call(command.split(' '))
    return

# Responsible for searching a company's proper name from their symbol
def lookup_name(symbol, lookup_df):
    company_df = lookup_df.loc[lookup_df['SYMBOL'] == symbol]

    # If the length is 0 then the company doesn't exist and nan will be placed in the shareholder spot
    if len(company_df) == 0:
        return numpy.nan
    return company_df.iloc[0]['NAME OF COMPANY'].lower()

def getResponse(url,params):
    jsonRes = {}
    # with requests.get(url, headers=headers, params=params, cookies=cookies, stream=True) as r:
    #     print(r)

    response = requests.get(url, headers=headers, params=params, cookies=cookies,timeout=30)
    if(response.status_code  == 200):
        jsonRes = response.json()
    else:
        updateCookies()
        response = requests.get(url, headers=headers, params=params, cookies=cookies)
        jsonRes = response.json()

    return jsonRes

# Called by lambda
def get_sast_pledge_value(symbol, sast_pledge_df, lookup_df):
    company = lookup_name(symbol, lookup_df)
    company_df = sast_pledge_df.loc[sast_pledge_df['comName'] == company]

    if len(company_df) == 0:
        return numpy.nan
    return float(100 if(company_df.iloc[0]['percPromoterShares'] == None) else company_df.iloc[0]['percPromoterShares'])


def set_signal_column_value(row):
    # Assigning these to variables so it doesn't get haywire with all these proper column names
    value = row['secVal']
    selling_qty = row['SELLING QTY PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)']
    shareholding = row['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP']
    sast_reg = row['SAST REGULATIONS (SOLD QTY)']
    pledged_data = row['PLEDGED DATA OF PROMOTER/PROMOTER GROUP']
    buying_avg = row['BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP']
    close_price = row['CLOSE_PRICE']

    # Actually doing the second filter first, since it's less likely to happen, thus reducing execution time
    if (buying_avg - (buying_avg * .5)) <= close_price <= (buying_avg + (buying_avg * .1)):
        if (value >= 10000000 and
                selling_qty == 0 and
                shareholding >= 50):
            if sast_reg == 0 and pledged_data == 0:
                return "PASS"
            elif pd.isnull(sast_reg) and pd.isnull(pledged_data):
                return "PASS/confirm SAST/PLEDGED"
            elif pd.isnull(sast_reg) and pledged_data == 0:
                return "PASS/confirm SAST"
            elif sast_reg == 0 and pd.isnull(pledged_data):
                return "PASS/confirm PLEDGED"
        else:
            return ""
    else:
        return ""




# Called by lambda
def get_sast_reg_value(symbol, sast_reg_df):
    symbol_df = sast_reg_df.loc[sast_reg_df['symbol'] == symbol]

    if len(symbol_df) == 0:
        return numpy.nan

    # Sum the total sale column and return it
    # symbol_df = symbol_df[symbol_df['noOfShareSale'].notna()]
    symbol_df['noOfShareSale'] = symbol_df['noOfShareSale'].astype(float)
    return symbol_df['noOfShareSale'].sum(skipna=True)

# Called by lambda
def get_value(mode, symbol, insider_df):
    # Get the dataframe that represents purchasing and selling data for this particular symbol
    # We need this whether we're doing purchase or sales
    symbol_df = insider_df.loc[insider_df['symbol'] == symbol]

    # Get the appropriate dataframe for the mode we're in
    if mode in ['selling_qty', 'selling_avg']:
        symbol_df = symbol_df[symbol_df['acqMode'] == 'Market Sale']
    elif mode in ['purchase_qty', 'purchase_avg']:
        symbol_df = symbol_df[symbol_df['acqMode'] == 'Market Purchase']

    # Convert the appropriate columns to floats for summation
    symbol_df['secAcq'] = symbol_df['secAcq'].astype(float)
    symbol_df['secVal'] = symbol_df['secVal'].astype(float)

    # Filter only for promoters and promoter group
    symbol_df = symbol_df[symbol_df['personCategory'].isin(['Promoters', 'Promoter Group'])]

    # If we just want the quantity, return the quantity
    if mode.find('qty') >= 0:
        return symbol_df.sum(axis=0, skipna=True)['secAcq']
    # If we want the average, calc the average and return it
    elif mode.find('avg') >= 0:
        return symbol_df.sum(axis=0, skipna=True)['secVal'] / symbol_df.sum(axis=0, skipna=True)['secAcq']

# Called by lambda
def get_shareholder_value(symbol, shareholding_df, lookup_df):
    # Need to look up company name from
    company = lookup_name(symbol, lookup_df)

    if type(company) == float:
        return numpy.nan

    try:
        return shareholding_df.loc[shareholding_df['name'] == company.lower()].iloc[0]['pr_and_prgrp']
    except IndexError:  # Index error means there was no entry
        return numpy.nan

# Long/Short ratio
def longShort():
    # lsUrl = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_27112020.csv"
    today = datetime.date.today()
    offset = max(1, (today.weekday() + 6) % 7 - 3)
    timedelta = datetime.timedelta(offset)
    getLastBussinessDay = today - timedelta
    url = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_"+getLastBussinessDay.strftime("%d%m%Y")+".csv"
    lsDf = pd.read_csv(url)
    lsDf.columns = lsDf.iloc[0]
    lsDf = lsDf.loc[lsDf['Client Type'] == 'FII']
    ratio = (float(lsDf['Future Index Long'].values[0])/(float(lsDf['Future Index Long'].values[0]) + float(lsDf['Future Index Short'].values[0])))*100
    return ratio
    # print(ratio)











# Insider response -- START

response = getResponse(insider_url,params)
insider_df = pd.DataFrame(response['data'])

insider_df.to_csv('insider.csv',index=False)
working_df = insider_df[insider_df['personCategory'].isin(['Promoters', 'Promoter Group'])]
working_df = working_df[working_df['acqMode'] == 'Market Purchase']

# We only care about 2 columns in the working dataframe
working_df = working_df[['symbol', 'secVal']]

# Convert the Value of Security column to a float for summation below
working_df['secVal'] = working_df['secVal'].astype(float)

# Consolidate by the symbol, summing the values of the securities
working_df['secVal'] = working_df.groupby('symbol')['secVal'].transform('sum')
working_df.drop_duplicates(subset='symbol', inplace=True, ignore_index=True)

''' GET THE BUYING AND SELLING QUANTITIES AND AVERAGES COLUMNS '''
bhav_df = pd.read_csv(bhav_url)

# Make the date column in the working df the first unique date from the bhav file
# In theory there should only be one date in the entire file, but grab the first unique one just in case
system_date = datetime.datetime.strptime(bhav_df[' DATE1'].unique()[0].strip(' '), '%d-%b-%Y')
working_df['DATE'] = system_date

# Get the bhav symbol and close price to join with the working df
bhav_df.drop_duplicates(subset='SYMBOL', inplace=True, ignore_index=True)
bhav_df.columns = [x.strip(' ') for x in bhav_df.columns]
bhav_df = bhav_df.rename(columns={'SYMBOL': 'symbol'})
bhav_df = bhav_df[['symbol', 'CLOSE_PRICE']]
working_df = pd.merge(left=working_df, right=bhav_df, left_on='symbol', right_on='symbol', how='left')  # Left merge so that if any symbols are not found in the bhav file they are just N/A instead of dropped

# Gather buying and selling quantities and averages
working_df['BUYING QTY OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_value('purchase_qty', row['symbol'], insider_df), axis=1)
working_df['BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_value('purchase_avg', row['symbol'], insider_df), axis=1)
working_df['SELLING QTY PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)'] = working_df.apply(lambda row : get_value('selling_qty', row['symbol'], insider_df), axis=1)
working_df['SELLING AVG PRICE OF PROMOTERS/PROMOTER GROUP (MARKET SELL DATA)'] = working_df.apply(lambda row: get_value('selling_avg', row['symbol'], insider_df), axis=1)

''' GATHER SHAREHOLDER DATA COLUMN '''
shareholding_response  = getResponse(shareholding_url,params)
shareholding_df = pd.DataFrame(shareholding_response)
shareholding_df['name'] = shareholding_df['name'].str.lower()
lookup_df = pd.read_csv(equity_csv_url)
working_df['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP'] = working_df.apply(lambda row: get_shareholder_value(row['symbol'], shareholding_df, lookup_df), axis=1)
working_df['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP'] = working_df['SHAREHOLDING PATTERN OF PROMOTERS/PROMOTER GROUP'].astype(float)

''' GATHER SAST REGULATIONS COLUMN '''
sast_res = getResponse(sast_url,params)
sast_reg_df = pd.DataFrame(sast_res['data'])
working_df['SAST REGULATIONS (SOLD QTY)'] = working_df.apply(lambda row: get_sast_reg_value(row['symbol'], sast_reg_df), axis=1)

''' GATHER SAST PLEDGED COLUMN '''
pledged_res = getResponse(pledged_url,params)
sast_pledge_df = pd.DataFrame(pledged_res['data'])
sast_pledge_df['comName'] = sast_pledge_df['comName'].str.lower()
working_df['PLEDGED DATA OF PROMOTER/PROMOTER GROUP'] = working_df.apply(lambda row: get_sast_pledge_value(row['symbol'], sast_pledge_df, lookup_df), axis=1)

''' MARK SOME TO BE GREEN FILLED BASED ON COMPLEX FILTER '''
working_df['SIGNAL'] = working_df.apply(lambda row: set_signal_column_value(row), axis=1)
# Filter good ones
working_df = working_df[working_df['SIGNAL'].str.len() > 0]
working_df = working_df[['symbol', 'secVal','CLOSE_PRICE','BUYING QTY OF PROMOTERS/PROMOTER GROUP','BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP','SIGNAL']]
working_df = working_df.sort_values('BUYING QTY OF PROMOTERS/PROMOTER GROUP',ascending=False)
working_df = working_df.rename(columns={'symbol': 'Company','secVal':'Security','BUYING QTY OF PROMOTERS/PROMOTER GROUP':'QTY','BUYING AVG PRICE OF PROMOTERS/PROMOTER GROUP':'AvgPrice','SIGNAL':'Signal','CLOSE_PRICE':'Close'})

renderImg = render_mpl_table(working_df, header_columns=0, col_width=3.0)
renderImg.get_figure().savefig('swing.png')
sendImg = send_image('1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ','swing.png','-1001168164977')
longToShortRatio = longShort()
telegram_bot_sendtext("longToShortRatio: "+str(longToShortRatio))

