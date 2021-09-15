import requests
import pandas as pd
from math import ceil
# BDay is business day, not birthday...
import time
import numpy as np
import threading
import investpy
from pandas.tseries.offsets import BDay
import json
import sys
from datetime import datetime, date, timedelta
# pd.datetime is an alias for datetime.datetime
today = pd.datetime.today()

# Start of Sheets imports
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = '/home/iamslk94/keys.json'
# SERVICE_ACCOUNT_FILE = '/home/sk/keys.json'

credentials = None
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1VUF-tqas7iSa2HfbHxwmrGWvtt7XMfph0fZvnpWwwBw'
SAMPLE_RANGE_NAME = 'screener!A1:G13'

service = build('sheets', 'v4', credentials=credentials)

# Call the Sheets API
appendSheet = service.spreadsheets()
updateSheet = build('sheets', 'v4', credentials=credentials).spreadsheets()
# ENd of Sheets imports

chainRes = {}

prevFactor= 1


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
cookies = {}
try:
    session = requests.Session()
    request = session.get(url_oc, headers=headers, timeout=10)
    cookies = dict(request.cookies)
except Exception as e:
    print(str(e))

# List of stocks to be watched

sectorStocks = {'NIFTYAUTO': {'BAJA':{},'EXID':{},'BLKI':{},'BFRG':{},'MRTI':{},'ASOK':{}},
                'NIFTY BANK': {'KTKM':{},'AXBK':{},'ICBK':{},'HDBK':{}},
                'NIFTY ENERGY':{'RELI':{},'HPCL':{},'ONGC':{},'NTPC':{}},
                'NIFTY FMCG': {'ITC':{},'MRCO':{},'DABU':{},'GOCP':{},'HLL':{},'JUBI':{}},
                'NIFTY IT':  {'INFY':{},'TCS':{},'HCLT':{},'TEML':{},'WIPR':{}},
                'NIFTY MEDIA': {'ZEE':{},'PVRL':{}},
                'NIFTY METAL':{'TISC':{},'COAL':{}},
                'NIFTY PHARMA':{'SUN':{},'LUPN':{},'CADI':{},'CIPL':{},'ARBN':{},'REDY':{}},
                }

indicesCPR = {'Nifty 50':{},'Nifty Bank':{},'Nifty Financial Services':{}}

commoditiesCPR = {'gold':{},'silver':{},'copper':{},'natural gas':{},'Crude Oil WTI':{},'MCX Zinc':{}}

stocks = {'INFY': {}, 'LT': {}, 'UPL': {},'TATASTEEL':{},'NTPC':{},'TITAN':{},'HDFC':{},'BHARTIARTL':{},
          'BPCL':{},'BAJFINANCE':{},'SUNPHARMA':{},'SBIN':{},'HINDUNILVR':{},'HCLTECH':{},'DLF':{},
          'LUPIN':{},'CIPLA':{},'TCS':{},'TECHM':{},'HCLTECH':{},'DABUR':{},'JUBLFOOD':{},'KOTAKBANK':{},'PEL':{},'AMARAJABAT':{},
          'ESCORTS':{}}

indices = ['NIFTY']

prevDay = (today - BDay(prevFactor)).strftime("%d-%m-%Y")
prevHistIndicesDay = (today - BDay(prevFactor)).strftime("%d%m%Y")

histUrl = 'https://www.nseindia.com/api/historical/cm/equity'
stockUrl = 'https://www.nseindia.com/api/quote-equity'
sectorUrl = 'https://www.nseindia.com/api/equity-stockIndices'
# Option cntracts Url
contracts_url = "https://www.nseindia.com/api/snapshot-derivatives-equity"
# Stock options Url
stockoptions_url = "https://www.nseindia.com/api/option-chain-equities"

# Indices Historical data
indicesHistUrl = "https://archives.nseindia.com/content/indices/ind_close_all_"

last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

start_day_of_prev_month = (date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)).strftime('%d/%m/%Y')

current_day = date.today().strftime('%d/%m/%Y')

rules = "RULES \n 1) Strictly follow CPR \n 2) Protect your capital - Most Important \n 3) Stay disciplined. \n ------------------------------"




init = 0
# cookies = {'Name': 'sk','Value':'_gsdgs','Domain':'.nseindia.com','Path':'/'}
# Init data

def initStocks():
    try:
        for sector, sectorStock in sectorStocks.items():
            for stock in sectorStock.keys():

                # response = requests.get(histUrl, headers=headers, params=initParams,cookies=cookies)
                dayWiseData = investpy.get_stock_recent_data(stock=stock, country='india',order='descending').head(3)
                prevDayObj = dayWiseData.to_dict('records')[1]
                prevPrevDayObj = dayWiseData.to_dict('records')[2]
                prevprevPivot = (prevPrevDayObj['High'] + prevPrevDayObj['Low'] + prevPrevDayObj['Close']) / 3
                obj = genPivots(prevDayObj,False)
                if(abs(obj['sPoints']['pivot']-obj['sPoints']['bc'])/obj['sPoints']['pivot'] * 100 > 0.04):
                    # Get Monthly Pivots
                    monthlyData = investpy.get_stock_historical_data(stock=stock, country='india', order='descending', interval='Monthly',from_date=start_day_of_prev_month, to_date=current_day).head(2).to_dict('records')[1]
                    monthlyPivot = genPivots(monthlyData,False)
                    obj['dPoints'] = {**obj['dPoints'], **{
                                                           'ms2': monthlyPivot['dPoints']['s2'],
                                                           'mr2': monthlyPivot['dPoints']['r2']}}
                    obj['sPoints'] = {**obj['sPoints'], **{'prevprevPivot': prevprevPivot}}
                    sectorStocks[sector][stock] = obj
                # else:
                #     del sectorStocks[sector][stock]

        print("Init Stocks DOne")
    except Exception as e:
        print("Exception occured in Init" + str(e))



def initIndices():
    try:
        indicesHistDf = pd.read_csv(indicesHistUrl+prevHistIndicesDay+".csv")
        for index in indicesCPR:
            niftyDf = indicesHistDf.loc[indicesHistDf['Index Name'] == index]
            o = float(niftyDf['Open Index Value'].values[0])
            h = float(niftyDf['High Index Value'].values[0])
            l = float(niftyDf['Low Index Value'].values[0])
            c = float(niftyDf['Closing Index Value'].values[0])
            pivot = (h + l + c) / 3
            bc = (h + l) / 2
            tc = (pivot - bc) + pivot
            obj = {
                'dPoints':{'r2': pivot + (h - l),'s2': pivot - (h - l),'h':h,'l':l},
                'sPoints':{'r1': 2 * pivot - l,'tc': tc,'pivot': pivot,'bc': bc,'s1': 2 * pivot - h,'prevOpen':o,'prevClose':c}
            }
            indicesCPR[index] = obj

    #     Clear Spreadsheet
        updateSheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                      range="screener!A2:G").execute()
    except Exception as e:
        print("Exception occured in InitIndices" + str(e))

# Get pivots for tdy and prevmnth
def initCommodities():
    try:
        indices = ['Nifty 50','Nifty Bank']
        for commodity in commoditiesCPR:
            # Get pivot values
            dayWiseData = investpy.commodities.get_commodity_recent_data(commodity=commodity, order='descending').head(3)
            prevDayObj = dayWiseData.to_dict('records')[1]
            prevPrevDayObj = dayWiseData.to_dict('records')[2]
            prevprevPivot = (prevPrevDayObj['High'] + prevPrevDayObj['Low'] + prevPrevDayObj['Close']) / 3
            obj = genPivots(prevDayObj,True)
            # Get monthly pivot values
            monthlyData = investpy.commodities.get_commodity_historical_data(commodity=commodity, order='descending',interval='Monthly',from_date=start_day_of_prev_month,to_date=current_day).head(2).to_dict('records')[1]
            monthlyPivot = genPivots(monthlyData,True)
            obj['dPoints'] = {**obj['dPoints'],**{'monthlyPivot':monthlyPivot['dPoints']['pivot'],'ms2':monthlyPivot['dPoints']['s2'],'mr2':monthlyPivot['dPoints']['r2']}}
            obj['sPoints'] = {**obj['sPoints'],**{'prevprevPivot':prevprevPivot}}
            commoditiesCPR[commodity] = obj

        for index in indices:
            # Get Pivot Values
            dayWiseData = investpy.get_index_recent_data(index=index, country='india',order='descending').head(3)
            prevDayObj = dayWiseData.to_dict('records')[1]
            prevPrevDayObj = dayWiseData.to_dict('records')[2]
            prevprevPivot = (prevPrevDayObj['High'] + prevPrevDayObj['Low'] + prevPrevDayObj['Close']) / 3
            obj = genPivots(prevDayObj,True)
            if (abs(obj['sPoints']['pivot'] - obj['sPoints']['bc']) / obj['sPoints']['pivot'] * 100 > 0.04):
                # Get Monthly Pivots
                monthlyData = investpy.get_index_historical_data(index=index, country='india', order='descending',interval='Monthly',from_date=start_day_of_prev_month,to_date=current_day).head(2).to_dict('records')[1]
                monthlyPivot = genPivots(monthlyData,True)
                obj['dPoints'] = {**obj['dPoints'],**{'monthlyPivot':monthlyPivot['dPoints']['pivot'],'ms2':monthlyPivot['dPoints']['s2'],'mr2':monthlyPivot['dPoints']['r2']}}
                obj['sPoints'] = {**obj['sPoints'], **{'prevprevPivot': prevprevPivot}}
                commoditiesCPR[index] = obj
            # else:
            #     del commoditiesCPR[index]

        print("Init Indices DOne")

    except Exception as e:
        print("Exception occured in InitCommodities" + str(e))

def genPivots(prevObj,addPivot):
    try:
        o = prevObj['Open']
        h = prevObj['High']
        l = prevObj['Low']
        c = prevObj['Close']
        pivot = (h + l + c) / 3
        bc = (h + l) / 2
        tc = (pivot - bc) + pivot
        obj = {
            'dPoints':{'r2': pivot + (h - l),'s2': pivot - (h - l)},
            'sPoints':{'r1': 2 * pivot - l,'tc': tc,'pivot': pivot,'bc': bc,'s1': 2 * pivot - h,'prevOpen':o,'prevClose':c}
        }
        if(addPivot):
            obj['dPoints'] = {**obj['dPoints'],**{'pivot':pivot,'h': h, 'l': l}}
            # obj['dPoints'] = {**obj['dPoints'], **{}}


        return obj

    except Exception as e:
        print("Exception occured in genPivots" + str(e))

def runCommodities():
    try:
        commoditiesToBePosted = {}
        for comm, value in commoditiesCPR.items():
            if('nifty' not in comm.lower()):
                ltp = investpy.commodities.get_commodity_recent_data(commodity=comm, order='descending').head(1).to_dict('records')[0]
            else:
                ltp = investpy.get_index_recent_data(index=comm,country='india', order='descending').head(1).to_dict('records')[0]

            if('dPoints' in value):
                for pK, pV in value['dPoints'].items():
                    if(pV*1.001 > ltp['Close'] > pV*0.999):
                        commoditiesToBePosted[comm] = {'ltp': ltp['Close'], pK: pV, 'pivotTrend':'buy' if value['sPoints']['pivot'] > value['sPoints']['prevprevPivot'] else 'sell' ,'openTrend':'buy' if ltp['Open'] > value['sPoints']['pivot'] else 'sell'}



        if bool(commoditiesToBePosted):
            if('monthlyPivot' in str(commoditiesToBePosted) or 'ms2' in str(commoditiesToBePosted) or 'mr2' in str(commoditiesToBePosted)):
                telegram_bot_sendMonthlyCommStock(str(commoditiesToBePosted))
            else:
                telegram_bot_sendCommodities(str(commoditiesToBePosted))


    except Exception as e:
        print("Exception occured in runCommodities" + str(e))
        updateCookies()





def updateCookies():
    global cookies
    session = requests.Session()
    request = session.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)
    print("Updating cookies")

def getResponse(url,params):
    jsonRes = {}
    response = requests.get(url, headers=headers, params=params, cookies=cookies)
    if(response.status_code  == 200):
        jsonRes = response.json()
    else:
        updateCookies()
        response = requests.get(url, headers=headers, params=params, cookies=cookies)
        jsonRes = response.json()

    return jsonRes


def closest(lst, K):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - K))]


def returnDf(url,params):
    # response = requests.get(url, headers=headers, params=params, cookies=cookies)
    global chainRes
    # print(params)
    chainRes = getResponse(url,params)
    volpcr = 0 if(chainRes["filtered"]["PE"]["totVol"]== 0 or chainRes["filtered"]["CE"]["totVol"]== 0) else (chainRes["filtered"]["PE"]["totOI"]/chainRes["filtered"]["PE"]["totVol"]) / (chainRes["filtered"]["CE"]["totOI"]/chainRes["filtered"]["CE"]["totVol"])
    #Near ATMS
    closestVal = closest(chainRes['records']['strikePrices'],chainRes['records']['underlyingValue'])
    pcr = chainRes["filtered"]["PE"]["totOI"] / chainRes["filtered"]["CE"]["totOI"]
    underLyingVal = chainRes['records']['underlyingValue']
    volR = 0 if(chainRes["filtered"]["PE"]["totVol"]== 0 or chainRes["filtered"]["CE"]["totVol"]== 0) else chainRes["filtered"]["PE"]["totVol"]/ chainRes["filtered"]["CE"]["totVol"]
    pedf = map(lambda x: "PE" in x and x["PE"], chainRes['filtered']['data'])
    cedf = map(lambda x: "CE" in x and x["CE"], chainRes['filtered']['data'])
    pedf = pd.DataFrame(list(filter(None, list(pedf))))
    cedf = pd.DataFrame(list(filter(None, list(cedf))))
    resDf = pd.merge(cedf, pedf, on='strikePrice', how='outer')
    return [[round001(pcr),underLyingVal],round001(volR),round001(volpcr),[closestVal-100,closestVal-50,closestVal,closestVal+50,closestVal+100],resDf]


def option_chain():
    try:
        indicesToBePosted = {}
        optionChainUrl = 'https://www.nseindia.com/api/option-chain-indices'
        otherIndex = [{'BANKNIFTY':'Nifty Bank'},{'FINNIFTY':'Nifty Financial Services'}]
        for idx in otherIndex:
            oparams = (
                ('symbol', next(iter(idx))),
            )
            [indexo, volro, volpcro, strikePriceso, resdfo] = returnDf(optionChainUrl, oparams)
            key = list(idx.values())[0]
            for pK,pV in indicesCPR[key]['dPoints'].items():
                if (pV * 1.001 > indexo[1] > pV * 0.999):
                    indicesToBePosted[key] = {'ltp': indexo[1], pK: pV, 'index': 'buy' if (
                                indexo[1] > indicesCPR[key]['sPoints']['prevOpen']) else 'sell'}

        for idx in indices:
            params = (
                ('symbol', idx),
            )
            [index,volr,volpcr,strikePrices,resdf] = returnDf(optionChainUrl,params)
            # print(r.to_dict())
            # r.to_csv("df.csv")
            htcContracts = htc()
            # putContracts = resdf.loc[resdf['strikePrice'].isin([htcContracts[0][0]['strikePrice'],htcContracts[0][1]['strikePrice'],htcContracts[0][2]['strikePrice']])]
            # callContracts = resdf.loc[resdf['strikePrice'].isin([htcContracts[1][0]['strikePrice'],htcContracts[1][1]['strikePrice'],htcContracts[1][2]['strikePrice']])]
            # callContracts['cratio'] = callContracts.apply(lambda row: row['changeinOpenInterest_x']*75/row['totalTradedVolume_x'],axis=1)
            # putContracts['pratio'] = putContracts.apply(lambda row: row['changeinOpenInterest_y']*75/row['totalTradedVolume_y'],axis=1)
            diff20StrikePrices = list(range(strikePrices[2]-19*50,strikePrices[2],50))+list(range(strikePrices[2],strikePrices[2]+19*50,50))
            # Ratio for atm strikes
            contractsATM = resdf.loc[resdf['strikePrice'].isin(strikePrices)]
            diff20Contracts = resdf.loc[resdf['strikePrice'].isin(diff20StrikePrices)]

            # callContractsATM = resdf.loc[resdf['strikePrice'].isin(strikePrices)]
            contractsATM['cratioatm'] = contractsATM['changeinOpenInterest_x']*75/contractsATM['totalTradedVolume_x']
            contractsATM['pratioatm'] = contractsATM['changeinOpenInterest_y']*75/contractsATM['totalTradedVolume_y']
            contractsATM = contractsATM.replace([np.inf, -np.inf], np.nan)


            for pK,pV in indicesCPR['Nifty 50']['dPoints'].items():
                if (pV * 1.001 > index[1] > pV * 0.999):
                    indicesToBePosted['Nifty 50'] = {'ltp': index[1], pK: pV, 'index': 'buy' if (
                                index[1] > indicesCPR['Nifty 50']['sPoints']['prevOpen']) else 'sell'}

            #coiCR = (callContracts['changeinOpenInterest_x'].sum()/callContracts['totalTradedVolume_x'].sum())*1000
            #coiPR = (putContracts['changeinOpenInterest_y'].sum() / putContracts['totalTradedVolume_y'].sum())*1000
            # mood = putContracts['pratio'].sum() - callContracts['cratio'].sum()
            # ratio = putContracts['pratio'].sum()/callContracts['cratio'].sum()
            coiDiff = diff20Contracts['changeinOpenInterest_y'].sum() - diff20Contracts['changeinOpenInterest_x'].sum()

            # ATM mood
            moodATM = contractsATM['pratioatm'].sum() - contractsATM['cratioatm'].sum()
            ratioATM = contractsATM['pratioatm'].sum()/contractsATM['cratioatm'].sum()

            # print(htcContracts)
            htcContractsText = 'HTC => ' + str(htcContracts[0][0]['strikePrice']) + ' ' + htcContracts[0][0]['optionType'] + ' , ' + str(htcContracts[1][0]['strikePrice']) + ' ' + htcContracts[1][0]['optionType']+', PremiumRatio => ' + str(round001(htcContracts[0][0]['premiumTurnover']/htcContracts[1][0]['premiumTurnover']))

            # text = "RATIO => "+str(round001(ratio))+", VOLPCR => "+str(volpcr)+", MOOD => "+str(round001(mood))

            textATM = "RATIO-ATM => "+str(round001(ratioATM))+", MOOD-ATM => "+str(round001(moodATM))
            # telegram_bot_sendindices(str(text) + '\n' + str(htcContractsText) + '\n' + str(textATM) + '\n')
            data = [[datetime.now().strftime("%I:%M %p"),round001(coiDiff),round001(ratioATM),round001(moodATM),index[1]]]

            appendSheet.values().append(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                 range="screener!A2:G2",valueInputOption="USER_ENTERED",
                                  insertDataOption="INSERT_ROWS",body={"values":data}).execute()


            updateSheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="screener!F18:F18", valueInputOption="RAW",
                                        body={"values": [[htcContractsText]]}).execute()



            if bool(indicesToBePosted):
                telegram_bot_sendindices(str(indicesToBePosted))

            # callContracts.to_csv('my_csv.csv', mode='a', header=False)
            # putContracts.to_csv('my_csv.csv', mode='a', header=False)
            # with open("test.txt", "a") as myfile:
            #     myfile.write(datetime.now().strftime("%H:%M:%S") + str(callContracts.to_dict()) + '\n' +str(putContracts.to_dict()) + '\n\n\n\n')
    except Exception as e:
        print("Exception occured in optionchain" + str(e))
        updateCookies()

def htc():
    try:
        callparams = (
            ('index', 'calls'),
        )
        putparams = (
            ('index', 'puts'),
        )
        # callResponse = requests.get(contracts_url, headers=headers, params=callparams, cookies=cookies)
        # putResponse = requests.get(contracts_url, headers=headers, params=putparams, cookies=cookies)
        callContractsRes = getResponse(contracts_url,callparams)
        putsContractsRes = getResponse(contracts_url,putparams)
        filteredCalls = list(filter(lambda x: x['underlying'] == 'NIFTY', callContractsRes['volume']['data']))
        filteredPuts = list(filter(lambda x: x['underlying'] == 'NIFTY', putsContractsRes['volume']['data']))
        calls = sorted(filteredCalls,key=lambda k: k['premiumTurnover'],reverse=True)[:3]
        puts = sorted(filteredPuts, key=lambda k: k['premiumTurnover'], reverse=True)[:3]
        # telegramSend = 'HTC => ' + str(puts['strikePrice']) + ' ' + puts['optionType'] + ' , ' + str(calls['strikePrice']) + ' ' + calls['optionType']+', PremiumRatio => ' + str(round001(puts['premiumTurnover']/calls['premiumTurnover']))
        # return telegramSend
        return [puts,calls]

    except Exception as e:
        print("Exception occured in option Contracts" + str(e))
        updateCookies()



def round001(v):
    return ceil(v * 1000) / 1000



def stockOptions():
    try:
        for sector, sectorStock in sectorStocks.items():
            for stock in sectorStock.keys():
                params = (
                    ('symbol', stock),
                )
                # print(stock)
                [pcr,volr,volpcr,strikePrices,resdf] = returnDf(stockoptions_url,params)
                resistance = resdf[resdf['openInterest_x'] == resdf['openInterest_x'].max()]['openInterest_x']
                support = resdf[resdf['openInterest_y'] == resdf['openInterest_y'].max()]['openInterest_y']
                print(resdf)

    except Exception as e:
        print("Exception occured in stockoptions" + str(e))
        updateCookies()



def liveStock():
    try:
        stocksToBePosted = {}
        optionChainStocks = {}
        for sector, sectorStock in sectorStocks.items():
            params = (
                ('index', sector),
            )
            # response = requests.get(sectorUrl, headers=headers, params=params, cookies=cookies)
            # stockRes = investpy.get_index_recent_data(index=sector, country='india',order='descending').head(3)
            # sectorValue = list(filter(lambda x: x['symbol'] == sector,stockRes['data']))
            for stock,value in sectorStock.items():
                ltp = investpy.get_stock_recent_data(stock=stock, country='india',order='descending').head(1).to_dict('records')[0]
                if('dPoints' in value):
                    for pK, pV in value['dPoints'].items():
                        if(pV*1.001 > ltp['Close'] > pV*0.999):
                            pivotTrend = 'buy' if value['sPoints']['pivot'] > value['sPoints']['prevprevPivot'] else 'sell'
                            openTrend = 'buy' if ltp['Open'] > value['sPoints']['pivot'] else 'sell'
                            stocksToBePosted[stock] = {'ltp': ltp['Close'], pK: pV, 'pivotTrend': pivotTrend, 'openTrend': openTrend}

                    stockParams = (
                        ('symbol', stock),
                    )
                # print(stock)
                # [pcr,volr,volpcr,strikePrices,resdf] = returnDf(stockoptions_url,stockParams)
                # resDf = resdf[resdf['openInterest_x'] == resdf['openInterest_x'].max()]
                # supDf = resdf[resdf['openInterest_y'] == resdf['openInterest_y'].max()]
                # resistance = resDf['strikePrice'].max()
                # support = supDf['strikePrice'].max()
                # if(resistance > ltp['lastPrice'] > resistance*0.99 and resDf['changeinOpenInterest_x'].max() > resDf['changeinOpenInterest_y'].max()):
                #     optionChainStocks[stock] = {'OptionChainResistance': resistance, 'ltp': ltp['lastPrice']}
                #
                # if (support * 1.01 > ltp['lastPrice'] > support and supDf['changeinOpenInterest_y'].max() > supDf['changeinOpenInterest_x'].max()):
                #     optionChainStocks[stock] = {'OptionChainSupport': support, 'ltp': ltp['lastPrice']}



        if bool(stocksToBePosted):
            telegram_bot_sendStocks(str(stocksToBePosted))

        # if bool(optionChainStocks):
        #     telegram_bot_sendcpr(str(optionChainStocks))

    except Exception as e:
        print("Exception occured in liveStock" + str(e))
        updateCookies()



def telegram_bot_sendStocks(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001380413685'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message.replace('},','\n\t')

    response = requests.get(send_text)
    # print(response)
    return response.json()

def telegram_bot_sendCommodities(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001285570704'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message.replace('},','\n\t')

    response = requests.get(send_text)
    # print(response)
    return response.json()

def telegram_bot_sendMonthlyCommStock(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001495299478'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message.replace('},','\n\t')

    response = requests.get(send_text)
    # print(response)
    return response.json()

def runLiveStock():
    while True:
        liveStock()
        time.sleep(300)
        print("Running LiveStock")

def runOptionChain():
    while True:
        option_chain()
        time.sleep(300)
        print("Running OptionChain")

def runCommForAll():
    while True:
        runCommodities()
        time.sleep(300)
        print("Running Commodity")








def main_run(runInfo):
    if(runInfo == "commodity"):
        th1 = threading.Thread(target=runCommForAll)
        th1.start()
    else:
        th3 = threading.Thread(target=runLiveStock)
        th3.start()



if __name__ == '__main__':
    telegram_bot_sendStocks(rules)
    telegram_bot_sendCommodities(rules)
    telegram_bot_sendMonthlyCommStock(rules)
    if(sys.argv[1] == "commodity"):
        initCommodities()
    else:
        initStocks()

    # initIndices()

    main_run(sys.argv[1])
