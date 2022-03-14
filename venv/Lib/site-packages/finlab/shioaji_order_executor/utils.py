import pandas as pd
from pydantic import ValidationError
import logging
import shioaji as sj
from shioaji.order import Deal
import os
from datetime import datetime, timedelta
import math

# Get an instance of a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
strategy_pkl_path = os.getenv('STRATEGY_PKL_PATH')
orders_pkl_path = os.getenv('ORDERS_PKL_PATH')
finlab_api_token = os.getenv('FINLAB_API_TOKEN')
fee_discount = os.getenv('FEE_DISCOUNT', 1)

"""
API Settings
"""


def activate_shioaji_api(person_id=None, password=None, ca_path="Sinopac.pfx", ca_password=None):
    person_id = os.getenv('person_id', person_id)
    password = os.getenv('password', password)
    ca_path = os.getenv('ca_path', ca_path)
    ca_password = os.getenv('ca_password', ca_password)
    api = sj.Shioaji()
    api.login(person_id, password)
    api.activate_ca(
        ca_path=ca_path,
        person_id=person_id,
        ca_passwd=ca_password,
    )
    return api


class ShioajiApi:

    def __init__(self, api):
        self.api = api
        self.strategy_pkl_path = os.getenv('STRATEGY_PKL_PATH')
        self.orders_pkl_path = os.getenv('ORDERS_PKL_PATH')
        self.finlab_api_token = os.getenv('FINLAB_API_TOKEN')


"""
Dataframe Process
"""


def df_combine(df_old, df_new, columns_fill=None):
    if columns_fill is None:
        columns_fill = []
    df_old.reset_index(inplace=False)
    df_old.set_index(df_new.index.names, inplace=True)
    if len(columns_fill) > 0:
        for col in columns_fill:
            df_new[col] = df_old[col]
    df_old = pd.concat([df_old, df_new])
    df_old = df_old[~df_old.index.duplicated(keep='last')]
    df_old = df_old.sort_index()
    return df_old


def update_df(model, new_post: dict, df_old=None, columns_fill=None):
    if columns_fill is None:
        columns_fill = []
    try:
        df_new = model(**new_post).get_df()
        if df_old is not None:
            df_new = df_combine(df_old, df_new, columns_fill)
        df_new = df_new.reset_index()
    except ValidationError as e:
        logger.error(e)
        df_new = None
    return df_new


"""
Finance Calculate Function
"""


def get_snapshot(api, stock_list, mode='dict'):
    contracts = [api.Contracts.Stocks.get(code) for code in stock_list]
    market_data = api.snapshots(contracts)
    if mode == 'dict':
        market_data = {snapshot.code: snapshot['close'] for snapshot in market_data}
    elif mode == 'df':
        market_data = pd.DataFrame(market_data)
    return market_data


def cal_fee(number, discount=fee_discount):
    v = math.floor(abs(number * (1.425 / 1000) * discount))
    if v < 20:
        v = 20
    return v


def transfer_order_datetime(order_datetime):
    t = datetime.fromordinal(order_datetime.toordinal())
    deadline = t + timedelta(hours=13, minutes=30)

    if order_datetime < deadline:
        date_value = datetime.fromordinal(deadline.date().toordinal())
    else:
        date_value = (deadline + timedelta(days=1)).date()
    return date_value


# init pkl

def init_df_strategy(format=False):
    strategy_pkl_path = os.getenv('STRATEGY_PKL_PATH')
    df_strategy = pd.DataFrame(columns=['name',
                                        'account',
                                        'enable',
                                        'size',
                                        'schedule',
                                        'update_time'])
    try:
        logger.warning('df_strategy is existed')
        pd.read_pickle(strategy_pkl_path)
        if format:
            df_strategy.to_pickle(strategy_pkl_path)
    except:
        logger.info('init df_strategy')
        df_strategy.to_pickle(strategy_pkl_path)


def init_df_orders(format=False):
    orders_pkl_path = os.getenv('ORDERS_PKL_PATH')
    df_orders = pd.DataFrame(columns=['strategy',
                                      'action',
                                      'cancel_quantity',
                                      'category',
                                      'code',
                                      'deal_quantity',
                                      'exchange',
                                      'modified_price',
                                      'modified_time',
                                      'name',
                                      'order_datetime',
                                      'order_type',
                                      'ordno',
                                      'price',
                                      'price_type',
                                      'quantity',
                                      'reference',
                                      'seqno',
                                      'status',
                                      'status_code',
                                      'deals',
                                      'day_trade',
                                      'trade_type'])
    try:
        logger.warning('df_orders is existed')
        pd.read_pickle(orders_pkl_path)
        if format:
            df_orders.to_pickle(orders_pkl_path)
    except:
        logger.info('init df_orders')
        df_orders.to_pickle(orders_pkl_path)


def convert_orders_from_shioaji_web(df, default_strategy_name='finlab_ml'):
    df.columns = df.iloc[0]
    df = df.iloc[1:-1]
    df = df.rename(columns={'數量': 'quantity', '成交價': 'price', '委託單號': 'ordno'})

    # columns data
    df['seqno'] = [i for i in range(len(df))]
    df['order_datetime'] = df['成交日'].apply(lambda s: pd.to_datetime(s, format='%Y/%m/%d', errors='ignore'))
    df['strategy'] = default_strategy_name
    df['action'] = df['交易別'].apply(lambda s: 'Buy' if '買' in s else 'Sell')
    df['cancel_quantity'] = 0
    df['category'] = ''
    df['deal_quantity'] = df['quantity']
    df['code'] = df['商品'].apply(lambda s: s[:s.index(' ')])
    df['exchange'] = 'TSE'
    df['modified_price'] = 0
    df['modified_time'] = None
    df['name'] = df['商品'].apply(lambda s: s[s.index(' ') + 1:])
    df['order_type'] = 'ROD'
    df['price_type'] = 'LMT'
    df['reference'] = df['price']
    df['status'] = 'Filled'
    df['status_code'] = '00'
    df['deals'] = ''

    # check_daytrade
    df['day_trade'] = 'Yes'
    df['check_daytrade'] = df['交易稅'] / df['價金']
    df['trade_type'] = df['check_daytrade'].apply(
        lambda s: '' if s < 1 / 1000 else 'DayTrade' if s < 2 / 1000 else 'Common')

    # select columns
    df = df[['seqno', 'order_datetime', 'strategy', 'action', 'cancel_quantity',
             'category', 'code', 'deal_quantity', 'exchange', 'modified_price',
             'modified_time', 'name', 'order_type', 'ordno', 'price', 'price_type',
             'quantity', 'reference', 'status', 'status_code', 'deals', 'day_trade',
             'trade_type']]

    # group_order_data
    ordno_list = list(set(df['ordno']))

    def group_order_data(df, ordno):
        sub_df = df[df['ordno'] == ordno]
        final_df = sub_df.copy().iloc[:1]
        final_df['deals'] = [[Deal(seq=seq, price=price, quantity=quantity, ts=1600000000) for seq, price, quantity in
                              zip(sub_df['seqno'], sub_df['price'], sub_df['quantity'])]]
        final_df['deal_quantity'] = sub_df['deal_quantity'].sum()
        final_df['quantity'] = sub_df['quantity'].sum()
        return final_df

    result = pd.concat([group_order_data(df, i) for i in ordno_list])
    result = result.sort_values(['order_datetime', 'code'])
    return result
