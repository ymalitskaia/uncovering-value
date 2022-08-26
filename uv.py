import numpy as np
import pandas as pd
from exchange_calendars import get_calendar

def get_ticker_sids(tickers_df):
    tickers_dropna = tickers_df.dropna(subset=['ticker'])
    ticker_sids = pd.DataFrame(index=tickers_dropna.ticker.unique())
    ticker_sids['sid'] = tickers_dropna.groupby('ticker').apply(lambda x: x.permaticker.values[0])
    return ticker_sids

def read_data_file(data_file, tickers_df, date_tags, data_tags):

    usecols = ['ticker'] + date_tags + data_tags
    raw_data = pd.read_csv(data_file, parse_dates=date_tags, usecols=usecols)
    # raw_data.rename(columns={'ticker': 'symbol'}, inplace=True)

    # remove assets without sids
    all_tickers = tickers_df.ticker.unique()
    common_tickers = list(set(raw_data.ticker.unique()) & set(all_tickers))
    raw_data = raw_data.query('ticker in @common_tickers')

    # add sids
    ticker_sids = get_ticker_sids(tickers_df).reset_index()
    ticker_sids.rename(columns={'index': 'ticker'}, inplace=True)
    raw_data = pd.merge(raw_data, ticker_sids, how='left', on=['ticker'])

    return raw_data

def get_sessions(start_date, end_date):
    sessions = get_calendar('XNYS').sessions
    start_index = sessions.get_loc(start_date)
    end_index = sessions.get_loc(end_date)
    session_dates = sessions[start_index: end_index+1]
    session_dates = session_dates.tz_localize(None)
    session_dates.freq = None
    return session_dates

def get_vw_rtn(group):
    rtns = group['21D'].values
    mt_caps = group['cap'].values  
    mt_caps_sum = group['cap'].sum()
    ws = mt_caps/mt_caps_sum
    rtn = np.sum(rtns*ws)
    return rtn

def calc_var_decile(x, var_name):
    date = x.index.get_level_values(0).unique().values[0]
    deciles = pd.qcut(x.xs(date)[var_name], 10, labels=False) + 1
    return deciles

def get_gpoa_queries(bp_query):
    queries = {}
    queries['NCI'] = bp_query + ' and equity_ratio < 0.8'
    pos_query = ' and equity_ratio > 0.8 and total_equity > 0 '
    queries['High GPOA'] = bp_query + pos_query + ' and gpoa_decile > 5'
    queries['Low GPOA'] = bp_query + pos_query + ' and gpoa_decile < 5'
    return queries
