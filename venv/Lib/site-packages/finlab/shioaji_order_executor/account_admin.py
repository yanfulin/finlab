import pandas as pd
from finlab.shioaji_order_executor.utils import ShioajiApi, logger, get_snapshot


class AccountAdmin(ShioajiApi):

    def get_account_balance(self):
        """Get information about bank account balance data right now.
        Returns:
            dataframe.
        """
        acc_balance = self.api.account_balance()
        df = pd.DataFrame(acc_balance)
        df = df.rename(columns={'update_time': 'date'})
        df['date'] = df['date'].apply(lambda d: d[:d.index(' ')])
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df = df.drop(columns=['errmsg'])
        return df

    def get_account_transfer(self):
        """Get information about bank account transfer data in recent 3 days.
        Returns:
            dataframe.
        """
        settlement = self.api.list_settlements(self.api.stock_account)
        df = pd.DataFrame(settlement)
        data_values = df.iloc[0].values
        date = data_values[::2]
        transfer = data_values[1::2]
        df_all = pd.DataFrame({'date': date, 'transfer': transfer})
        df_all['date'] = pd.to_datetime(df_all['date'])
        df_all = df_all.sort_values('date')
        return df_all

    def get_account_settlement(self):
        """Get information about bank account settlement include available_balance right now.
        Returns:
            dataframe.
        """
        df_account_balance = self.get_account_balance().set_index(['date'])
        df_account_transfer = self.get_account_transfer().set_index(['date'])
        df = pd.concat([df_account_balance, df_account_transfer], axis=1)
        df['available_balance'] = df['acc_balance'] + (df['transfer'].iloc[1:].sum())
        df['available_balance'] = df['available_balance'].fillna(method='ffill')
        df = df.reset_index()
        return df

    def get_positions(self):
        """Get information about stock assets of the overall position right now.
        Returns:
            dict in list.
        """
        positions = self.api.list_positions(self.api.stock_account)
        positions = [{k: p[k] for k in ['code', 'quantity', 'price', 'pnl']} for p in positions]
        return positions

    def get_positions_cost(self):
        """Get information about stock assets of the overall position cost include cash right now.
        Returns:
            dataframe.
        """
        available_balance = self.get_account_settlement()['available_balance'].values[0]
        positions_data = self.get_positions()
        if len(positions_data) > 0:
            stock_list = [i['code'] for i in positions_data]
            positions = pd.DataFrame(positions_data)
            positions['cost'] = positions['price'] * positions['quantity'] * 1000
            positions['return'] = round(positions['pnl'] / positions['cost'] * 100, 2)

            snapshot = get_snapshot(self.api, stock_list, mode='df')[['code', 'close']]
            positions = positions.join(snapshot.set_index('code'), on='code')
            positions['net_value'] = positions['close'] * positions['quantity'] * 1000
        else:
            positions = pd.DataFrame()
        result = positions.append(
            {'code': 'cash', 'cost': available_balance, 'net_value': available_balance, 'pnl': 0, 'quantity': 1},
            ignore_index=True)
        return result

    def get_total_balance(self):
        """Get information about net asset value right now.
        Returns:
            float
        """
        positions = self.get_positions()
        positions_money = sum([p['price'] * p['quantity'] * 1000 + p['pnl'] for p in positions])
        account_money = self.get_account_settlement()
        available_balance = account_money['available_balance'].values[0]
        total_balance = positions_money + available_balance
        return total_balance

    def get_profit_loss_history(self, start_date: str, end_date: str):
        """Get information about profit loss history.
        Args:
          start_date(str): start date for query,ex:'2021-01-03'
          end_date(str): end date for query,ex:'2021-01-03'
        Returns:
            dataframe.
        """
        profit_loss = self.api.list_profit_loss(self.api.stock_account, start_date, end_date)
        df = pd.DataFrame(profit_loss)
        if len(df) < 1:
            return None
        df['date'] = pd.to_datetime(df['date'])
        return df

    def get_profit_loss_history_detail(self, detail_id: int):
        """Get information about profit loss history detail with detail_id .
        Args:
          detail_id(int): id values in index values.
        Returns:
            dataframe.
        """
        try:
            profit_loss = self.api.list_profit_loss_detail(self.api.stock_account, detail_id)
        except IndexError:
            logger.error('get_profit_loss_history_detail func error: list index out of range.')
            return None
        df = pd.DataFrame(profit_loss)
        if len(df) < 1:
            return None
        df['date'] = pd.to_datetime(df['date'])
        return df

    def get_realized_profit_loss(self, start_date: str, end_date: str):
        """Get information about daily realized profit loss.
        Args:
          start_date(str): start date for query,ex:'2021-01-03'
          end_date(str): end date for query,ex:'2021-01-03'
        Returns:
            dataframe.
        """
        df = self.get_profit_loss_history(start_date, end_date)
        date_range = pd.date_range(start=start_date, end=end_date)
        if df is None:
            df = pd.DataFrame({'date': date_range, 'pnl': [0] * len(date_range)})
        else:
            df = df.groupby(['date'])['pnl'].sum().to_frame().reset_index()
            date_space = pd.DataFrame({'date': date_range})
            df = date_space.join(df.set_index('date'), on='date')
            df['pnl'] = df['pnl'].fillna(0)
        return df
