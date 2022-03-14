from finlab.shioaji_order_executor.models import OrderPost, ExecuteOrderPost
from finlab.shioaji_order_executor.utils import ShioajiApi, update_df, logger, get_snapshot, cal_fee
from finlab.shioaji_order_executor.strategy_admin import StrategyAdmin
from datetime import datetime
import pandas as pd
import numpy as np
import time


class OrderExecutor(ShioajiApi):
    """Orders Control"""

    def __init__(self, api):
        super().__init__(api)
        self._trading_methods = []

    def register(self, trading_method):
        """Register TradingMethod object.
        Args:
          trading_method(dict):
                              ex:[{'strategy': {'name':'self_a_strategy','account':'shioaji_stock','enable':1,
                                                'size':150000,'schedule':"1 19 * * *"},
                                   'target_positions':{'5530':2,'1587':2}},
                                  {'strategy': {'name':'self_b_strategy','account':'shioaji_stock','enable':1,
                                                'size':100000,'schedule':"2 19 * * *"},
                                   'target_positions':{'1101':2}}
                                  ]
        """
        self._trading_methods.append(trading_method)

    @staticmethod
    def get_trade_detail(trade):
        """Parse trader object data.
        Args:
         trade(Object):Trade object which is created by shioaj module
        Returns:
            trade_info(dict)
        """
        trade_info = trade.contract
        trade_info = {i: trade_info[i] for i in ['exchange', 'code', 'name', 'category', 'reference', 'day_trade']}
        order = trade.order
        order = {i: order[i] for i in ['action', 'price', 'quantity', 'seqno', 'ordno', 'price_type', 'order_type']}
        status = trade.status
        status = {i: status[i] for i in
                  ['status', 'status_code', 'order_datetime', 'deal_quantity', 'cancel_quantity', 'modified_time',
                   'modified_price', 'deals']}
        trade_info.update(order)
        trade_info.update(status)
        return trade_info

    def update_orders_status(self, df_format=True):
        """Update orders status and update pkl records.
        Args:
         df_format(bool):Whether to convert to dataframe
        Returns:
            new_post(dict or dataframe)
        """
        self.api.update_status()
        df_old = pd.read_pickle(self.orders_pkl_path)

        trade_list = self.api.list_trades()
        data = [self.get_trade_detail(trade) for trade in trade_list]

        new_post = {'data': data}
        if df_format:
            if len(data) < 1:
                return df_old
            # The official api has 'strategy' field
            new_post = update_df(OrderPost, new_post, df_old, columns_fill=['strategy'])

            # update trade_type,add trade_type from api.list_profit_loss_detail.
            df = new_post
            pl_df = self.api.list_profit_loss()
            pl_df = pd.DataFrame(pl_df)
            try:
                range_date = pl_df['date'][0]
                recent_df = df[df['order_datetime'] > range_date]

                pl_df['ordno'] = pl_df['dseq'].apply(lambda s: s[:-2])
                pl_detail = pd.DataFrame(
                    [self.api.list_profit_loss_detail(self.api.stock_account, i)[0] for i in range(len(pl_df))])
                pl_df['trade_type'] = pl_detail['trade_type']
                pl_df['trade_type'] = pl_df['trade_type'].apply(lambda s: s.value)
                pl_df = pl_df.set_index(['ordno'])

                recent_df = recent_df.set_index(['ordno'])
                recent_df['trade_type'] = pl_df['trade_type']
                recent_df['trade_type'] = recent_df['trade_type'].fillna('')
                recent_df = recent_df.reset_index()

                refresh_df = pd.concat([df, recent_df])
                new_post = refresh_df[~refresh_df[['order_datetime', 'ordno']].duplicated(keep='last')]
            except Exception as e:
                logger.warning(f'api.list_profit_loss is None,error:{e}')
                pass

            # update
            new_post = new_post.sort_values(['order_datetime'])
            new_post.to_pickle(self.orders_pkl_path)

        return new_post

    def create_new_orders(self, target_positions, mode='strategy', strategy=None):
        """create new orders in order to rebalance the old positions to new positions

        Args:
          target_positions (dict): a dictionary with stock_id and the number of lot,ex:{stock_id:quantity},
                                   ex:{'2543': 4.0, '2841': 4.0, '2897': 4.0, '3011': 4.0, '5345': 4.0}.
          mode(str): the value is 'stock_account' or 'strategy'.
                                         if you use 'stock_account', use the overall account inventory to calculate.
                                         if you use 'strategy', use the strategic position inventory calculation
          strategy(str):strategy name,if you use 'strategy' as mode parameter ,
                        strategy name is requested.
        Returns:
            dict: new orders which update old positions.
            ex: if your original positions are {'2414': 2, '2330': 1} then with the target_position example above,
                the following return is obtained.
              {
                '2414': 2.0,
                '2330': -1
                '2430': 6.0,
                '2616': 3.0,
              }

        """

        # get present positions
        present_positions = None
        if mode == 'stock_account':
            positions = self.api.list_positions(self.api.stock_account)
            present_positions = {p.code: p.quantity for p in positions}
        elif mode == 'strategy':
            strategy_admin = StrategyAdmin(self.api)
            present_positions = strategy_admin.get_strategy_position(strategy)

        target_positions = pd.Series(target_positions).astype(int).to_dict()
        # print present and target positions
        logger.info('Present positions:')
        logger.info(pd.Series(present_positions))
        logger.info('------------------')
        logger.info('Target positions:')
        logger.info(pd.Series(target_positions))
        logger.info('------------------')

        # calculate the difference between present position and target position
        all_codes = set(list(target_positions.keys()) + list(present_positions.keys()))
        new_orders = (pd.Series(target_positions).reindex(all_codes).fillna(0) -
                      pd.Series(present_positions).reindex(all_codes).fillna(0)).astype(int)
        new_orders = new_orders[new_orders != 0].to_dict()

        # print the new orders
        logger.info('new orders to rebalance:')
        if new_orders:
            logger.info(pd.Series(new_orders))
        else:
            logger.info('None')
        logger.info('------------------')
        return new_orders

    def _check_size(self, order_dict):
        target_list = order_dict['target_positions'].keys()
        strategy_size = order_dict['strategy']['size']
        if len(target_list) > 0:
            snapshot = get_snapshot(self.api, target_list, mode='df')
            snapshot = snapshot[['code', 'close']]
            snapshot['quantity'] = order_dict['target_positions'].values()
            snapshot['cost'] = snapshot['close'] * snapshot['quantity'] * 1000
            snapshot['fee'] = snapshot['cost'].apply(lambda s: cal_fee(s))
            snapshot['total_cost'] = snapshot['cost'] + snapshot['fee']
            total_cost = snapshot['total_cost'].sum()
            check_size = total_cost <= strategy_size
        else:
            # target_list is []
            total_cost = 0
            check_size = np.True_
        return check_size, total_cost, strategy_size

    def execute_orders(self, new_orders: dict, strategy='None', price_type="LMT", order_type="ROD", order_lot="Common",
                       price_opt='close',
                       df_format=True, df_old=None):

        """Execute orders and create record.
        Args:
          new_orders(dict):order list {stock_id:quantity},ex:{'1101':1,'1102':2}
          strategy(str):strategy name
          price_type(str):pricing type of order ,{LMT, MKT, MKP}
          order_type(str):the type of order,{ROD, IOC, FOK}
          order_lot(str):the type of order,{Common, Fixing, Odd, IntradayOdd} (整股、定盤、盤後零股、盤中零股)
          price_opt(str):the price option of order ,close(quick deal) or first_bid(waited deal)
          df_format(bool):whether to activate update_df function
          df_old(dataFrame):old dataframe file
        Returns:
            new_post(dict or DataFrame):execute_orders record
        """

        # query market data
        contracts = [self.api.Contracts.Stocks.get(code) for code in new_orders.keys()]
        market_data = self.api.snapshots(contracts)
        market_data = {snapshot.code: snapshot for snapshot in market_data}

        # make orders
        strategy_dict = {'strategy': strategy}
        data = []
        for code, quantity in new_orders.items():
            contract = self.api.Contracts.Stocks.get(code)
            target_market_data = market_data[code]
            if price_opt == 'close':
                price = target_market_data.close
            elif price_opt == 'first_bid':
                price = target_market_data.buy_price if quantity > 0 else target_market_data.sell_price
            else:
                price = target_market_data.close
            action = 'Buy' if quantity > 0 else 'Sell'

            if quantity == 0:
                continue
            order = self.api.Order(price=price,
                                   quantity=abs(int(quantity)),
                                   action=action,
                                   price_type=price_type,
                                   order_type=order_type,
                                   order_lot=order_lot,
                                   account=self.api.stock_account
                                   )
            trade = self.api.place_order(contract, order)
            logger.info(f'operation: new trade:{trade}')
            order_info = self.get_trade_detail(trade)
            order_info.update(strategy_dict)
            data.append(order_info)

        new_post = {'data': data}
        if df_format:
            new_post = update_df(ExecuteOrderPost, new_post, df_old)
        return new_post

    def execute_strategy_orders(self, price_type="LMT", order_type="ROD", order_lot="Common", price_opt='close'):
        """Execute strategy orders and create record.
        Args:
          price_type(str):pricing type of order ,{LMT, MKT, MKP}
          order_type(str):the type of order,{ROD, IOC, FOK}
          order_lot(str):the type of order,{Common, Fixing, Odd, IntradayOdd} (整股、定盤、盤後零股、盤中零股)
          price_opt(str):the price option of order ,close(quick deal) or first_bid(waited deal)
        """
        self.cancel_all_unfilled_orders(np.nan)
        strategy_admin = StrategyAdmin(self.api)
        strategy_update = strategy_admin.update_strategy([s['strategy'] for s in self._trading_methods])
        if strategy_update is None:
            return None
        for s in self._trading_methods:
            strategy = s['strategy']
            strategy_name = strategy['name']
            target_positions = s['target_positions']
            # check strategy enable
            check_strategy = strategy['enable']
            if check_strategy == 0:
                logger.error('strategy param is not in strategy file or opened.')
                return None

            # check strategy size
            check_size, total_cost, strategy_size = self._check_size(s)
            if check_size is not np.True_:
                logger.error(f'strategy size: {strategy_size} is smaller than orders total net value: {total_cost},'
                             f'please modify strategy size or orders total net value.')
                return None
            # Delete strategic unfilled orders before placing an order.
            self.cancel_all_unfilled_orders(strategy_name)

            new_orders = self.create_new_orders(target_positions, strategy=strategy_name)

            if len(new_orders) < 1:
                logger.warning('No trades should be execute')
                self.update_orders_status()
                return False
            else:
                try:
                    df_old = pd.read_pickle(self.orders_pkl_path)
                except Exception as e:
                    logger.error('read pkl error', e)
                    df_old = None
                self.update_orders_status()
                df_new = self.execute_orders(new_orders=new_orders, strategy=strategy_name, price_type=price_type,
                                             order_type=order_type, order_lot=order_lot, price_opt=price_opt,
                                             df_old=df_old)
                df_new.to_pickle(self.orders_pkl_path)
        time.sleep(2)
        self.update_orders_status()

    def execute_account_orders(self, target_positions, price_type="LMT", order_type="ROD", order_lot="Common",
                               price_opt='close'):
        """Execute account orders and not use strategy record store system.
        Args:
          target_positions (dict): a dictionary with stock_id and the number of lot,ex:{stock_id:quantity},
                                   ex:{'2543': 4.0, '2841': 4.0, '2897': 4.0, '3011': 4.0, '5345': 4.0}.
          price_type(str):pricing type of order ,{LMT, MKT, MKP}
          order_type(str):the type of order,{ROD, IOC, FOK}
          order_lot(str):the type of order,{Common, Fixing, Odd, IntradayOdd} (整股、定盤、盤後零股、盤中零股)
          price_opt(str):the price option of order ,close(quick deal) or first_bid(waited deal)
        """
        self.api.update_status()
        self.cancel_all_unfilled_orders(save_pkl=False)
        new_orders = self.create_new_orders(target_positions, mode='stock_account')
        if len(new_orders) < 1:
            logger.warning('No trades should be execute')
            return False
        self.execute_orders(new_orders=new_orders, price_type=price_type, order_type=order_type, order_lot=order_lot,
                            price_opt=price_opt, df_format=False)
        time.sleep(2)
        self.api.update_status()

    def cancel_all_unfilled_orders(self, strategy=None, save_pkl=True):
        """Cancel all unfilled orders
        Args:
          strategy(str):strategy name which you specify to cancel orders
                        default values is None,cancel all unfilled orders in all strategy
          save_pkl(bool):update orders pkl or not
        """

        if strategy:
            df = self.update_orders_status()
            if strategy is np.nan:
                df = df[df['strategy'].isna()]
            else:
                df = df[df['strategy'] == strategy]
            df = df[~df['status'].isin(['Cancelled', 'Failed', 'Filled'])]
            if len(df) < 1:
                logger.info(f'no {strategy} trades should be canceled')
                return None
            for trade in self.api.list_trades():
                if trade.order.seqno in df['seqno'].values:
                    self.api.cancel_order(trade)
                    logger.info(f'operation: cancel trade {trade}')
            self.update_orders_status()
        else:
            if save_pkl:
                self.update_orders_status()
            else:
                self.api.update_status()
            for trade in self.api.list_trades():
                # Failed PreSubmitted
                if trade.status.status in ['Cancelled', 'Failed', 'Filled']:
                    pass
                else:
                    self.api.cancel_order(trade)
                    logger.info(f'operation: cancel trade {trade}')
            if save_pkl:
                self.update_orders_status()

    def get_unsold_orders(self):
        """Get unsold orders in recent trade day.
        Returns:
            unsold orders data for strategy (list):
            ex:[{'target_list': {'2543': 4, '2841': 4, '2897': 4, '3011': 4, '5345': 4},
                 'strategy': 'self_a_strategy'},
                {'target_list': {'1413': 2, '1453': 2, '1460': 2, '2888': 2, '6167': 2},
                 'strategy': 'self_b_strategy'}]
        """
        df = pd.read_pickle(self.orders_pkl_path)
        max_date = df['order_datetime'].apply(lambda d: d.date()).max()
        max_date = datetime.strptime(max_date.strftime('%Y%m%d'), '%Y%m%d')
        df = df[df['order_datetime'] > max_date]
        df = df[~df['status'].isin(['Cancelled', 'Filled'])]
        df['lack'] = df['quantity'] - df['deal_quantity']
        df = df[df['lack'] > 0]
        df['lack'] = [-num if action == 'Sell' else num for action, num in
                      zip(df['action'], df['lack'])]
        group_data = df.groupby(['code', 'strategy'])['lack'].sum()
        strategy_list = list(set(group_data.index.get_level_values('strategy')))

        all_list = []
        for s in strategy_list:
            data = group_data[group_data.index.get_level_values('strategy') == s]
            dataset = {'strategy': s, 'target_list': {k[0]: v for k, v in data.to_dict().items()}}
            all_list.append(dataset)
        return all_list


def loop_execution(enable, hour, minute, period):
    def decorator(func):
        def warp():
            if enable is False:
                func()
            else:
                now = datetime.now()
                limit_time = datetime(now.year, now.month, now.day, hour, minute)
                while datetime.now() <= limit_time:
                    now = datetime.now()
                    logger.info(f'time:{now}')
                    func_return = func()
                    if func_return is False:
                        logger.info('Filled condition, break the function')
                        break
                    time.sleep(period)

        return warp

    return decorator


def loop_order(order_executor, enable=True, target_positions=None, mode='strategy', hour=13, minute=29,
               period=60, price_opt='first_bid'):
    @loop_execution(enable=enable, hour=hour, minute=minute, period=period)
    def order():
        if mode == 'strategy':
            order_executor.execute_strategy_orders(price_opt=price_opt)
        elif mode == 'account':
            order_executor.execute_account_orders(target_positions=target_positions, price_opt=price_opt)

    order()
    logger.info('Finish loop_order.')


def loop_update(order_executor, enable=True, mode='strategy', hour=13, minute=33, period=60):
    @loop_execution(enable=enable, hour=hour, minute=minute, period=period)
    def update():
        if mode == 'strategy':
            order_executor.update_orders_status()
        else:
            return None

    update()
    logger.info('Finish update orders pkl file at market closed time.')
