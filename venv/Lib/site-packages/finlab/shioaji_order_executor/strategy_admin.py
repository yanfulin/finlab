from finlab.shioaji_order_executor.utils import ShioajiApi, update_df, logger, get_snapshot, transfer_order_datetime, \
    cal_fee
from finlab.shioaji_order_executor.models import StrategyPost
from finlab.shioaji_order_executor.account_admin import AccountAdmin
import pandas as pd
import math
from datetime import datetime
import finlab
from finlab import data as finlab_data


class StrategyAdmin(ShioajiApi):
    def update_strategy(self, strategy_list: list):
        """Update strategy settings,total size shold be smaller than net asset.
        Args:
          strategy_list(list): strategy dict in list,ex:
          [{'account': 'shioaji_stock',
            'name': 'self_a_strategy',
            'enable': 1,
            'schedule': '1 19 * * *',
            'size': 150000},
          {'account': 'shioaji_stock',
            'name': 'self_c_strategy',
            'enable': 1,
            'schedule': '2 19 * * *',
            'size': 220000}]
        Returns:
            dataframe:new strategy data.
        """
        # old strategy settings.If you set strategy initially,make df_old be None.
        try:
            df_old = pd.read_pickle(self.strategy_pkl_path)
        except Exception as e:
            logger.error(e)
            df_old = None
        new_post = {'data': strategy_list}
        df = update_df(StrategyPost, new_post, df_old)
        df.to_pickle(self.strategy_pkl_path)
        logger.info('success update')
        return df

    def check_strategy_enable(self, strategy: str):
        """Check if the strategy is on.
        Args:
          strategy(str): strategy name
        Returns:
            bool.
        """
        df = pd.read_pickle(self.strategy_pkl_path)
        df = df.set_index(['name'])
        result = False
        if strategy in df.index:
            if df.loc[strategy, 'enable'] == 1:
                result = True
        return result

    def get_strategy_position(self, strategy: str):
        """Get information about strategy position right now.
        Args:
          strategy(str): strategy name
        Returns:
            dict:{stock_id:quantity},ex:{'2606': 3}
        """
        df = pd.read_pickle(self.orders_pkl_path)
        df = df[df['strategy'] == strategy]
        df['deal_quantity'] = [-num if action == 'Sell' else num for action, num in
                               zip(df['action'], df['deal_quantity'])]
        df = df.groupby(['code'])['deal_quantity'].sum()
        df = df[df > 0]
        df = df.to_dict()
        return df

    def get_strategy_position_snapshot(self, strategy: str):
        """Get information about strategy position snapshot right now.
        Args:
          strategy(str): strategy name
        Returns:
            dict:{stock_id:price},ex:{'2606': 36}
        """
        df = self.get_strategy_position(strategy)
        stock_list = list(df.keys())
        df = get_snapshot(self.api, stock_list)
        return df

    def get_strategy_net_value(self, strategy: str):
        position = self.get_strategy_position(strategy)
        if len(position) > 0:
            snapshot = self.get_strategy_position_snapshot(strategy)

            data = [{'name': strategy, 'stock_id': k, 'deal_quantity': position[k], 'price': snapshot[k]} for k in
                    position.keys()]
            df = pd.DataFrame(data)
            df['est_cost'] = df['price'] * df['deal_quantity'] * 1000
            df['est_sell_fee'] = df['est_cost'].apply(lambda s: cal_fee(s))
            df['est_sell_tax'] = (df['est_cost']).apply(lambda s: math.floor(s * (3 / 1000)))
            df['est_sell_net_value'] = df['est_cost'] - df['est_sell_fee'] - df['est_sell_tax']
        else:
            df = pd.DataFrame()
        return df

    def get_strategy_account_info(self):
        strategy = pd.read_pickle(self.strategy_pkl_path)
        strategy = strategy[['name', 'size']]
        strategy_name = list(strategy['name'])
        df = pd.concat([self.get_strategy_net_value(s) for s in strategy_name])
        if len(df) > 0:
            df = df.groupby(['name'])[['est_sell_net_value']].sum()
            df = df.join(strategy.set_index('name'), on='name')
            df['available'] = df['size'] - df['est_sell_net_value']
            df = df.reset_index()
        else:
            df = pd.DataFrame()
        account_admin = AccountAdmin(self.api)
        cash = account_admin.get_account_settlement()['available_balance'].values[0]
        df = df.append({'name': 'cash', 'est_sell_net_value': cash, 'available': cash, 'size': 0}, ignore_index=True)
        return df

    def get_strategy_daily_info(self, strategy='all'):

        df = pd.read_pickle(self.orders_pkl_path)
        df['date'] = df['order_datetime'].apply(lambda s: transfer_order_datetime(s))
        if strategy != 'all':
            df = df[df['strategy'] == strategy]

        df['date'] = df['order_datetime'].apply(lambda s: transfer_order_datetime(s))
        df['deal_quantity'] = [
            sum([i.quantity for i in df['deals'].values[i]]) * -1 if df['action'].values[i] == 'Sell' else sum(
                [i.quantity for i in df['deals'].values[i]]) for i in range(len(df['deals'].values))]
        df = df[abs(df['deal_quantity']) > 0].sort_values(['order_datetime'])
        df['cost'] = [sum([i.price * i.quantity for i in df['deals'].values[i]]) * 1000 if df['action'].values[
                                                                                               i] == 'Sell' else sum(
            [i.price * i.quantity for i in df['deals'].values[i]]) * -1000 for i in range(len(df['deals'].values))]
        df['fee'] = [cal_fee(i) for i in df['cost']]
        df['tax'] = [
            math.floor(c * (3 / 1000)) if t == 'Common' else math.floor(c * (1.5 / 1000)) if t == 'DayTrade' else 0 for
            c, t in zip(df['cost'], df['trade_type'])]
        df['total_cost'] = df['cost'] - df['fee'] - df['tax']

        finlab.login(self.finlab_api_token)
        close = finlab_data.get('price:收盤價')

        # create date_range
        min_date = df['date'].min().date()
        max_date = datetime.now()
        date_range = pd.date_range(start=min_date, end=max_date)

        daily_account_data = []
        for date in date_range:
            select_df = df[df['date'] <= date]
            daily_stock = select_df.copy()
            if date in close.index:
                daily_stock['close'] = [close.loc[date, c] for c in daily_stock['code'].values]
            else:
                continue
            daily_stock['est_cost'] = daily_stock['close'] * daily_stock['deal_quantity'] * 1000
            daily_stock['est_sell_fee'] = daily_stock['est_cost'].apply(lambda s: math.floor(s * (1.425 / 1000)))
            daily_stock['est_sell_tax'] = daily_stock['est_cost'].apply(lambda s: math.floor(s * (3 / 1000)))

            daily_stock['est_sell_total_cost'] = daily_stock['est_cost'] - daily_stock['est_sell_fee'] - daily_stock[
                'est_sell_tax']
            daily_stock['est_sell_pnl'] = round(
                daily_stock['est_sell_total_cost'] + daily_stock['cost'] - daily_stock['fee'])

            stock_id_list = list(set(daily_stock['code']))

            def cal_part(daily_stock_df, stock_id):
                target = daily_stock_df.copy()
                target = target[target['code'] == stock_id]
                target['cumsum_deal_quantity'] = target['deal_quantity'].cumsum()
                cumsum_deal_quantity = list(target['cumsum_deal_quantity'])[::-1]

                if 0 in cumsum_deal_quantity:
                    hold_part_index = len(cumsum_deal_quantity) - cumsum_deal_quantity.index(0)
                    realized_part = target.iloc[:hold_part_index]
                    realized_pnl = round(realized_part['total_cost'].sum())
                else:
                    hold_part_index = 0
                    realized_pnl = 0

                hold_quantity = target['cumsum_deal_quantity'].iloc[-1]

                if hold_quantity == 0:
                    est_sell_pnl = 0
                    hold_cost = 0
                else:
                    hold_part = target.iloc[hold_part_index:]
                    hold_cost = round(abs((hold_part['cost'] - hold_part['fee']).sum()))
                    est_sell_pnl = round(hold_part['est_sell_pnl'].sum())

                result = {'stock_id': stock_id, 'hold_cost': hold_cost, 'hold_quantity': hold_quantity,
                          'est_sell_pnl': est_sell_pnl, 'net_value': hold_cost + est_sell_pnl,
                          'realized_pnl': realized_pnl}
                return result

            cal_part_data = [cal_part(daily_stock, s) for s in stock_id_list]
            cal_part_df = pd.DataFrame(cal_part_data)
            daily_account_data.append({'date': date, 'data': cal_part_data, 'hold_cost': cal_part_df['hold_cost'].sum(),
                                       'est_sell_pnl': cal_part_df['est_sell_pnl'].sum(),
                                       'realized_pnl': cal_part_df['realized_pnl'].sum()})
        return daily_account_data