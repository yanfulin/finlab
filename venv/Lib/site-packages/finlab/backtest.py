import re
import datetime
import requests
import threading
import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset

import finlab
from finlab import report
from finlab import data
from finlab import mae_mfe
from finlab.utils import check_version
from finlab.backtest_core import backtest_, get_trade_stocks

encryption = ''
encryption_time = datetime.datetime.now()


def adjust_dates_to_index(creturn, dates):

    def to_buesiness_day(d):
        if d <= creturn.index[-1]:
            i = creturn.index.get_loc(d, method='bfill')
            ret = creturn.index[i]
        else:
            ret = None  # creturn.index[-1]
        return ret

    return pd.DatetimeIndex(pd.Series(dates).apply(to_buesiness_day).dropna()).drop_duplicates()


def download_backtest_encryption():

    if datetime.datetime.now() < encryption_time + datetime.timedelta(days=1) and encryption:
        return encryption

    res = requests.get('https://asia-east2-fdata-299302.cloudfunctions.net/auth_backtest',
                       {'api_token': finlab.get_token()})

    if not res.ok:
        return ''

    d = res.json()

    if 'v' in d and 'v_msg' in d and finlab.__version__ < d['v']:
        print(d['v_msg'])

    return d['encryption']


def arguments(price, position, resample_dates=None):
    resample_dates = price.index if resample_dates is None else resample_dates
    resample_dates = pd.Series(resample_dates).view(np.int64).values

    position = position.astype(float).fillna(0)
    price = price.astype(float)

    return [price.values,
            price.index.view(np.int64),
            price.columns.astype(str).values,
            position.values,
            position.index.view(np.int64),
            position.columns.astype(str).values,
            resample_dates
            ]

@check_version
def sim(position: pd.DataFrame, resample=None, trade_at_price='close',
        position_limit=1, fee_ratio=1.425/1000,
        tax_ratio=3/1000, name=None, stop_loss=None,
        take_profit=None, touched_exit=False,
        mae_mfe_window=30, mae_mfe_window_step=5, upload=True):

    # check name is valid
    if name:
        head_is_eng = len(re.findall(
            r'[\u0041-\u005a|\u0061-\u007a]', name[0])) > 0
        has_cn = len(re.findall('[\u4e00-\u9fa5]', name[1:])) > 0
        if head_is_eng and has_cn:
            raise Exception('Strategy Name Error: 名稱如包含中文，需以中文當開頭。')

    # check position is valid
    if position.sum().sum() == 0 or len(position.index) == 0:
        raise Exception('Position is empty and zero stock is selected.')

    # format position index
    if isinstance(position.index[0], str):
        position = position.index_str_to_date()

    # asset type
    asset_type = 'tw_stock' if (
        position.columns.str.find('USDT') == -1).all() else 'crypto'

    # adjust for trading price
    if asset_type == 'tw_stock':
        if trade_at_price:
            price = data.get(f'etl:adj_{trade_at_price}')
        else:
            price = (data.get('etl:adj_close') + data.get('etl:adj_open'))/2

    elif asset_type == 'crypto':
        if trade_at_price:
            price = data.get(f'crypto:{trade_at_price}')
        else:
            price = (data.get('crypto:close') + data.get('crypto:open'))/2
    else:
        raise Exception(f'**ERROR: asset type {asset_type} is not allowed')

    # if position date is very close to price end date, run all backtesting dates
    assert position.shape[1] >= 2
    delta_time_rebalance = position.index[-1] - position.index[-3]
    backtest_to_end = position.index[-1] + \
        delta_time_rebalance > price.index[-1]

    position = position[position.index <= price.index[-1]]
    backtest_end_date = price.index[-1] if backtest_to_end else position.index[-1]

    # resample dates
    if isinstance(resample, str):

        offset_days = 0
        if '+' in resample:
            offset_days = int(resample.split('+')[-1])
            resample = resample.split('+')[0]
        if '-' in resample and resample.split('-')[-1].isdigit():
            offset_days = -int(resample.split('-')[-1])
            resample = resample.split('-')[0]

        dates = pd.date_range(
            position.index[0], position.index[-1], freq=resample, tz=position.index.tzinfo)
        dates += DateOffset(days=offset_days)
        dates = [d for d in dates if position.index[0]
                 <= d and d <= position.index[-1]]

        next_trading_date = min(set(pd.date_range(position.index[0],
                                                  datetime.datetime.now(
                                                      position.index.tzinfo) + datetime.timedelta(days=720),
                                                  freq=resample)) - set(dates))

    elif resample is None:
        dates = None
        next_trading_date = position.index[-1] + datetime.timedelta(days=1)

    if stop_loss is None:
        stop_loss = -np.inf

    if take_profit is None:
        take_profit = np.inf

    if dates is not None:
        position = position.reindex(dates, method='ffill')

    encryption = download_backtest_encryption()

    creturn_value = backtest_(*arguments(price, position, dates),
                              encryption=encryption,
                              fee_ratio=fee_ratio, tax_ratio=tax_ratio,
                              stop_loss=stop_loss, take_profit=take_profit,
                              touched_exit=touched_exit, position_limit=position_limit,
                              mae_mfe_window=mae_mfe_window, mae_mfe_window_step=mae_mfe_window_step)

    creturn = pd.Series(creturn_value, price.index)
    creturn = creturn[(creturn != 1).cumsum().shift(-1, fill_value=1) != 0]
    creturn = creturn.loc[:backtest_end_date]
    if len(creturn) == 0:
        creturn = pd.Series(1, position.index)

    d = pd.DataFrame(
        get_trade_stocks(position.columns.astype(str).values, price.index.view(np.int64)))

    if len(d) != 0:

        d.columns = ['stock_id', 'entry_date', 'exit_date',
                     'entry_sig_date', 'exit_sig_date',
                     'position', 'period', 'entry_index', 'exit_index']

        d.index.name = 'trade_index'

        d['entry_date'] = pd.to_datetime(d.entry_date)
        d['exit_date'] = pd.to_datetime(d.exit_date)
        d['entry_sig_date'] = pd.to_datetime(d.entry_sig_date)
        d['exit_sig_date'] = pd.to_datetime(d.exit_sig_date)

        m = pd.DataFrame(mae_mfe.mae_mfe)
        nsets = int((m.shape[1]-1) / 4)-1

        tuples = sum([[(n*mae_mfe_window_step, metric) for metric in ['mae', 'g_mfe', 'b_mfe', 'mdd']] for n in list(range(nsets))], [])\
            + [('exit', 'mae'), ('exit', 'gmfe'), ('exit', 'bmfe'),
                ('exit', 'mdd')] + [('exit', 'return')]

        m.columns = pd.MultiIndex.from_tuples(
            tuples, names=["window", "metric"])
        m.index.name = 'trade_index'

        d['return'] = m.iloc[:, -1]

    report_ret = report.Report(
        creturn, position, fee_ratio, tax_ratio, trade_at_price, next_trading_date)
    if len(d) != 0:
        report_ret.trades = d
        report_ret.mae_mfe = m

    if not upload:
        return report_ret

    result = report_ret.upload(name)

    if 'status' in result and result['status'] == 'error':
        print('Fail to upload result to server')
        print('error message', result['message'])
        return report_ret

    try:
        url = 'https://ai.finlab.tw/strategy/?uid=' + \
            result['uid'] + '&sid=' + result['strategy_id']
        from IPython.display import IFrame, display
        #from IPython.display import HTML, display

        iframe = IFrame(url, width='100%', height=600)
        display(iframe)

    except Exception as e:
        print(e)
        print('Install ipython to show the complete backtest results.')

    return report_ret
