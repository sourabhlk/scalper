import requests
import pandas as pd
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
request = session.get(url_oc, headers=headers, timeout=5)
cookies = dict(request.cookies)

# List of stocks to be watched

sectorStocks = {'NIFTY AUTO': {'BAJAJ-AUTO':{},'AMARAJABAT':{},'EXIDEIND':{},'TATAMOTORS':{},'M&M':{},'BALKRISHND':{},'BHARATFORG':{},'MARUTI':{},'ASHOKLEY':{}},
                'NIFTY BANK': {'KOTAKBANK':{},'AXISBANK':{},'ICICIBANK':{},'HDFCBANK':{},'SBIN':{},'INDUSINDBK':{}},
                'NIFTY ENERGY':{'RELIANCE':{},'BPCL':{},'HINDPETRO':{},'ADANIGREEN':{},'ONGC':{},'TATAPOWER':{}},
                'NIFTY FMCG': {'ITC':{},'MARICO':{},'DABUR':{},'GODREJCP':{},'HINDUNILVR':{}},
                'NIFTY IT':  {'INFY':{},'TCS':{},'HCLTECH':{},'TECHM':{},'WIPRO':{}},
                'NIFTY MEDIA': {'ZEEL':{},'SUNTV':{}},
                'NIFTY METAL':{'TATASTEEL':{},'JINDALSTEEL':{},'HINDALCO':{},'COALINDIA':{}},
                'NIFTY PHARMA':{'SUNPHARMA':{},'LUPIN':{},'CADILAHC':{},'CIPLA':{}},
                }
stocks = {'INFY': {}, 'LT': {}, 'UPL': {},'TATASTEEL':{},'NTPC':{},'TITAN':{},'HDFC':{},'BHARTIARTL':{},
          'BPCL':{},'BAJFINANCE':{},'SUNPHARMA':{},'SBIN':{},'HINDUNILVR':{},'HCLTECH':{},'DLF':{},
          'LUPIN':{},'CIPLA':{},'TCS':{},'TECHM':{},'HCLTECH':{},'DABUR':{},'JUBLFOOD':{},'KOTAKBANK':{},'PEL':{},'AMARAJABAT':{},
          'ESCORTS':{}}

prevDay = (today - BDay(1)).strftime("%d-%m-%Y")

histUrl = 'https://www.nseindia.com/api/historical/cm/equity'
stockUrl = 'https://www.nseindia.com/api/quote-equity'



init = 0
# cookies = {'Name': 'sk','Value':'_gsdgs','Domain':'.nseindia.com','Path':'/'}
# Init data

def initStocks():
    try:
        for stock, value in stocks.items():
            initParams = (
                ('symbol', stock),
                ('series', '[\"EQ\"]'),
                ('from', prevDay),
                ('to', prevDay),
            )
            response = requests.get(histUrl, headers=headers, params=initParams,cookies=cookies)
            histRes = response.json()
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
            stocks[stock] = obj
    except Exception as e:
        print("Exception occured in Init" + str(e))

def updateCookies():
    request = session.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)
    print("Updating cookies")

def liveStock():
    try:
        stocksToBePosted = {}
        for stock,value in stocks.items():
            params = (
                ('symbol', stock),
            )
            response = requests.get(stockUrl, headers=headers, params=params, cookies=cookies)
            stockRes = response.json()
            for pK, pV in value['dPoints'].items():
                if(pV*1.001 > stockRes['priceInfo']['lastPrice'] > pV*0.999):
                    if(stockRes['priceInfo']['open'] > value['sPoints']['prevOpen']):
                        stocksToBePosted[stock] = {'currPrice': stockRes['priceInfo']['lastPrice'], pK: pV, 'verdict':'buy'}
                    else:
                        stocksToBePosted[stock] = {'currPrice': stockRes['priceInfo']['lastPrice'], pK: pV, 'verdict':'sell'}

        if bool(stocksToBePosted):
            telegram_bot_sendtext(str(stocksToBePosted))

    except Exception as e:
        print("Exception occured in liveStock" + str(e))
        updateCookies()



def telegram_bot_sendtext(bot_message):
    bot_token = '1175180677:AAEFb1hxCYXcQhq0DAiuJcoNfwgbLpT1RdQ'
    bot_chatID = '-1001380413685'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()




try:
    initStocks()
    while True:
        liveStock()
        time.sleep(240)
        print("Running again")

except Exception as e:
    print("Exception occured in Main" + str(e))
    telegram_bot_sendtext(str(e))
    pass









