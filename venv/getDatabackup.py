#
import requests
import pandas as pd
from math import ceil
# BDay is business day, not birthday...
import time
from pandas.tseries.offsets import BDay
import json
# pd.datetime is an alias for datetime.datetime
today = pd.datetime.today()


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
request = session.get(url_oc, headers=headers, timeout=10)
cookies = dict(request.cookies)

# List of stocks to be watched

sectorStocks = {'NIFTY AUTO': {'BAJAJ-AUTO':{},'AMARAJABAT':{},'EXIDEIND':{},'TATAMOTORS':{},'M&M':{},'BALKRISIND':{},'BHARATFORG':{},'MARUTI':{},'ASHOKLEY':{}},
                'NIFTY BANK': {'KOTAKBANK':{},'AXISBANK':{},'ICICIBANK':{},'HDFCBANK':{},'SBIN':{},'INDUSINDBK':{}},
                'NIFTY ENERGY':{'RELIANCE':{},'BPCL':{},'HINDPETRO':{},'ONGC':{},'NTPC':{}},
                'NIFTY FMCG': {'ITC':{},'MARICO':{},'DABUR':{},'GODREJCP':{},'HINDUNILVR':{},'JUBLFOOD':{}},
                'NIFTY IT':  {'INFY':{},'TCS':{},'HCLTECH':{},'TECHM':{},'WIPRO':{}},
                'NIFTY MEDIA': {'ZEEL':{},'SUNTV':{},'PVR':{}},
                'NIFTY METAL':{'TATASTEEL':{},'HINDALCO':{},'COALINDIA':{}},
                'NIFTY PHARMA':{'SUNPHARMA':{},'LUPIN':{},'CADILAHC':{},'CIPLA':{},'AUROPHARMA':{}},
                }
stocks = {'INFY': {}, 'LT': {}, 'UPL': {},'TATASTEEL':{},'NTPC':{},'TITAN':{},'HDFC':{},'BHARTIARTL':{},
          'BPCL':{},'BAJFINANCE':{},'SUNPHARMA':{},'SBIN':{},'HINDUNILVR':{},'HCLTECH':{},'DLF':{},
          'LUPIN':{},'CIPLA':{},'TCS':{},'TECHM':{},'HCLTECH':{},'DABUR':{},'JUBLFOOD':{},'KOTAKBANK':{},'PEL':{},'AMARAJABAT':{},
          'ESCORTS':{}}

indices = ['BANKNIFTY']

prevDay = (today - BDay(1)).strftime("%d-%m-%Y")
histUrl = 'https://www.nseindia.com/api/historical/cm/equity'
stockUrl = 'https://www.nseindia.com/api/quote-equity'
sectorUrl = 'https://www.nseindia.com/api/equity-stockIndices'
# Option cntracts Url
contracts_url = "https://www.nseindia.com/api/snapshot-derivatives-equity"
# Stock options Url
stockoptions_url = "https://www.nseindia.com/api/option-chain-equities"






init = 0
# cookies = {'Name': 'sk','Value':'_gsdgs','Domain':'.nseindia.com','Path':'/'}
# Init data

def initStocks():
    try:
        for sector, sectorStock in sectorStocks.items():
            for stock in sectorStock.keys():
                initParams = (
                ('symbol', stock),
                ('series', '[\"EQ\"]'),
                ('from', prevDay),
                ('to', prevDay),
                )
                # response = requests.get(histUrl, headers=headers, params=initParams,cookies=cookies)
                histRes = getResponse(histUrl,initParams)
                # response = requests.get(stockUrl, headers=histRes.headers, params=params)
                o = histRes['data'][0]['CH_OPENING_PRICE']
                h = histRes['data'][0]['CH_TRADE_HIGH_PRICE']
                l = histRes['data'][0]['CH_TRADE_LOW_PRICE']
                c = histRes['data'][0]['CH_CLOSING_PRICE']
                pivot = (h + l + c) / 3
                bc = (h + l) / 2
                tc = (pivot - bc) + pivot
                obj = {
                    'dPoints':{'r2': pivot + (h - l),'s2': pivot - (h - l),'h':h,'l':l},
                    'sPoints':{'r1': 2 * pivot - l,'tc': tc,'pivot': pivot,'bc': bc,'s1': 2 * pivot - h,'prevOpen':o,'prevClose':c}
                }
                sectorStocks[sector][stock] = obj
    except Exception as e:
        print("Exception occured in Init" + str(e))

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







def returnDf(url,params):
    # response = requests.get(url, headers=headers, params=params, cookies=cookies)
    chainRes = getResponse(url,params)
    pcr = chainRes["filtered"]["PE"]["totOI"] / chainRes["filtered"]["CE"]["totOI"]
    volR = chainRes["filtered"]["PE"]["totVol"]/ chainRes["filtered"]["CE"]["totVol"]
    pedf = map(lambda x: "PE" in x and x["PE"], chainRes['filtered']['data'])
    cedf = map(lambda x: "CE" in x and x["CE"], chainRes['filtered']['data'])
    pedf = pd.DataFrame(list(filter(None, list(pedf))))
    cedf = pd.DataFrame(list(filter(None, list(cedf))))
    resDf = pd.merge(cedf, pedf, on='strikePrice', how='outer')
    return [round001(pcr),round001(volR),resDf]


def option_chain():
    try:
        optionChainUrl = 'https://www.nseindia.com/api/option-chain-indices'
        for idx in indices:
            params = (
                ('symbol', idx),
            )
            resDfs = returnDf(optionChainUrl,params)
            # print(r.to_dict())
            # r.to_csv("df.csv")
            htcContracts = htc()
            callContracts = resDfs[2].loc[resDfs[2]['strikePrice'].isin([htcContracts[0][0]['strikePrice'],htcContracts[0][1]['strikePrice']])]
            putContracts = resDfs[2].loc[resDfs[2]['strikePrice'].isin([htcContracts[1][0]['strikePrice'],htcContracts[1][1]['strikePrice']])]
            coiCR = (callContracts['changeinOpenInterest_x'].sum()/callContracts['totalTradedVolume_x'].sum())*1000
            coiPR = (putContracts['changeinOpenInterest_y'].sum() / putContracts['totalTradedVolume_y'].sum())*1000
            print(htcContracts)
            # text = "PE => "+str(resDfs[0])+", CE => "+str(resDfs[1])+", R => "+str(round001(resDfs[0]/resDfs[1]))
            # telegram_bot_sendtext(str(text) + '\n' + str(contractsText))
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
        filteredCalls = list(filter(lambda x: x['underlying'] == 'BANKNIFTY', callContractsRes['volume']['data']))
        filteredPuts = list(filter(lambda x: x['underlying'] == 'BANKNIFTY', putsContractsRes['volume']['data']))
        calls = sorted(filteredCalls,key=lambda k: k['premiumTurnover'],reverse=True)[:2]
        puts = sorted(filteredPuts, key=lambda k: k['premiumTurnover'], reverse=True)[:2]
        # telegramSend = 'HTC => ' + str(puts['strikePrice']) + ' ' + puts['optionType'] + ' , ' + str(calls['strikePrice']) + ' ' + calls['optionType']+', PremiumRatio => ' + str(round001(puts['premiumTurnover']/calls['premiumTurnover']))
        # return telegramSend
        return [calls,puts]

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
                [pe,ce,resdf] = returnDf(stockoptions_url,params)
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
            stockRes = getResponse(sectorUrl,params)
            sectorValue = list(filter(lambda x: x['symbol'] == sector,stockRes['data']))
            for stock,value in sectorStock.items():
                ltp = list(filter(lambda x: x['symbol'] == stock, stockRes['data']))[0]
                for pK, pV in value['dPoints'].items():
                    if(pV*1.001 > ltp['lastPrice'] > pV*0.999):
                        stocksToBePosted[stock] = {'ltp': ltp['lastPrice'], pK: pV, 'stock': 'buy' if(ltp['open'] > value['sPoints']['prevOpen']) else 'sell', 'sector': 'buy' if(sectorValue[0]['lastPrice'] > sectorValue[0]['open']) else 'sell'}

                stockParams = (
                    ('symbol', stock),
                )
                # print(stock)
                [pe,ce,resdf] = returnDf(stockoptions_url,stockParams)
                resDf = resdf[resdf['openInterest_x'] == resdf['openInterest_x'].max()]
                supDf = resdf[resdf['openInterest_y'] == resdf['openInterest_y'].max()]
                resistance = resDf['strikePrice'].max()
                support = supDf['strikePrice'].max()
                if(resistance > ltp['lastPrice'] > resistance*0.99 and resDf['changeinOpenInterest_x'].max() > resDf['changeinOpenInterest_y'].max()):
                    optionChainStocks[stock] = {'OptionChainResistance': resistance, 'ltp': ltp['lastPrice']}

                if (support * 1.01 > ltp['lastPrice'] > support and supDf['changeinOpenInterest_y'].max() > supDf['changeinOpenInterest_x'].max()):
                    optionChainStocks[stock] = {'OptionChainSupport': support, 'ltp': ltp['lastPrice']}



        if bool(stocksToBePosted):
            telegram_bot_sendtext(str(stocksToBePosted))

        if bool(optionChainStocks):
            telegram_bot_sendtext(str(optionChainStocks))

    except Exception as e:
        print("Exception occured in liveStock" + str(e))
        updateCookies()



def telegram_bot_sendtext(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001380413685'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message.replace('},','\n\t')

    response = requests.get(send_text)
    # print(response)
    return response.json()







def main_run():
    while True:
        # liveStock()
        option_chain()
        # stockOptions()
        time.sleep(240)
        print("Running again")


if __name__ == '__main__':
    # initStocks()
    main_run()













