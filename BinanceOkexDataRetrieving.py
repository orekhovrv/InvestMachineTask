import pandas as pd
import requests
import json
import sys


def getBinanceData(unifyColumnName, binanceColumnName):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    content = json.loads(response.content)
    instrumentsDataFrame = pd.DataFrame.from_dict(content['symbols'])[
        ['symbol', 'baseAsset', 'quoteAsset']
    ]
    instrumentsDataFrame \
        .rename({'symbol': binanceColumnName}, axis=1, inplace=True)
    instrumentsDataFrame[unifyColumnName] = \
        instrumentsDataFrame[['baseAsset', 'quoteAsset']].sum(axis=1)
    instrumentsDataFrame \
        .drop(['baseAsset', 'quoteAsset'], axis=1, inplace=True)
    return instrumentsDataFrame


def getOkexData(unifyColumnName, okexColumnName):
    url = "https://www.okex.com/api/spot/v3/instruments"
    response = requests.get(url)
    content = json.loads(response.content)
    instrumentsDataFrame = pd.DataFrame.from_dict(content)[
        ['instrument_id', 'base_currency',  'quote_currency']
    ]
    instrumentsDataFrame \
        .rename({'instrument_id': okexColumnName}, axis=1, inplace=True)
    instrumentsDataFrame[unifyColumnName] = \
        instrumentsDataFrame[['base_currency', 'quote_currency']].sum(axis=1)
    instrumentsDataFrame \
        .drop(['base_currency', 'quote_currency'], axis=1, inplace=True)
    return instrumentsDataFrame


def uniteInstruments(
        unifyColumnName, okexInstrumentsDataFrame, binanceInstrumentsDataFrame
):
    exchangeNames = okexInstrumentsDataFrame\
        .merge(binanceInstrumentsDataFrame, on=unifyColumnName, how='outer')
    exchangeNames\
        .insert(0, unifyColumnName, exchangeNames.pop(unifyColumnName))
    exchangeNames.sort_values(unifyColumnName, inplace=True, ignore_index=True)
    return exchangeNames


unifyColumnName = "unify name"
binanceColumnName = "binance name"
okexColumnName = "okex name"

okexInstrumentsDataFrame = getBinanceData(unifyColumnName, binanceColumnName)
binanceInstrumentsDataFrame = getOkexData(unifyColumnName, okexColumnName)
exchangeNames = uniteInstruments(
    unifyColumnName, okexInstrumentsDataFrame, binanceInstrumentsDataFrame
)

exchangeNames.to_csv(sys.stdout, index=False)
