from finlab.utils import logger
import datetime
import numpy as np
import pandas as pd
from finlab import data

class FinlabDataFrame(pd.DataFrame):

    @property
    def _constructor(self):
        return FinlabDataFrame

    @staticmethod
    def reshape(df1, df2):

        isfdf1 = isinstance(df1, FinlabDataFrame)
        isfdf2 = isinstance(df2, FinlabDataFrame)
        isdf1 = isinstance(df1, pd.DataFrame)
        isdf2 = isinstance(df2, pd.DataFrame)

        both_are_dataframe = (isfdf1 + isdf1) * (isfdf2 + isdf2) != 0

        d1_index_freq = df1.get_index_str_frequency() if isfdf1 else None
        d2_index_freq = df2.get_index_str_frequency() if isfdf2 else None

        if ((d1_index_freq or d2_index_freq)
          and (d1_index_freq != d2_index_freq)
          and both_are_dataframe):

            df1 = df1.index_str_to_date() if isfdf1 else df1
            df2 = df2.index_str_to_date() if isfdf2 else df2

        if isinstance(df2, pd.Series):
            df2 = pd.DataFrame({c: df2 for c in df1.columns})

        if both_are_dataframe:
            index = df1.index.union(df2.index)
            columns = df1.columns.intersection(df2.columns)

            if len(df1.index) * len(df2.index) != 0:
              index_start = max(df1.index[0], df2.index[0])
              index = [t for t in index if index_start <= t]

            return df1.reindex(index=index, method='ffill')[columns], \
                df2.reindex(index=index, method='ffill')[columns]
        else:
            return df1, df2

    def __lt__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__lt__(df1, df2)

    def __gt__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__gt__(df1, df2)

    def __le__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__le__(df1, df2)

    def __ge__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__ge__(df1, df2)

    def __eq__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__eq__(df1, df2)

    def __ne__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__ne__(df1, df2)

    def __sub__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__sub__(df1, df2)

    def __add__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__add__(df1, df2)

    def __mul__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__mul__(df1, df2)

    def __truediv__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__truediv__(df1, df2)

    def __rshift__(self, other):
        return self.shift(-other)

    def __lshift__(self, other):
        return self.shift(other)

    def __and__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__and__(df1, df2)

    def __or__(self, other):
        df1, df2 = self.reshape(self, other)
        return pd.DataFrame.__or__(df1, df2)

    def index_str_to_date(self):

      if len(self.index) == 0 or not isinstance(self.index[0], str):
        return self

      if self.index[0].find('M') != -1:
        return self._index_str_to_date_month()
      elif self.index[0].find('Q') != -1:
        return self._index_str_to_date_season()

      return self

    @staticmethod
    def to_business_day(date):
        close = data.get('price:收盤價')
        return pd.Series(date).apply(lambda d: d if d in close.index or d < close.index[0] or d > close.index[-1] else close.loc[d:].index[0]).values

    def get_index_str_frequency(self):

        if len(self.index) == 0:
          return None

        if not isinstance(self.index[0], str):
          return None

        if (self.index.str.find('M') != -1).all():
          return 'month'

        if (self.index.str.find('Q') != -1).all():
          return 'season'

        return None

    def _index_date_to_str_month(self):

        # index is already str
        if len(self.index) == 0 or not isinstance(self.index[0], pd.Timestamp):
          return self

        index = (self.index - datetime.timedelta(days=30)).strftime('%Y-M%m')
        return FinlabDataFrame(self.values, index=index, columns=self.columns)

    def _index_str_to_date_month(self):

        # index is already timestamps
        if len(self.index) == 0 or not isinstance(self.index[0], str):
          return self

        if not (self.index.str.find('M') != -1).all():
          logger.warning('FinlabDataFrame: invalid index, cannot format index to monthly timestamp.')
          return self

        index = pd.to_datetime(self.index, format='%Y-M%m') + pd.offsets.MonthBegin() + datetime.timedelta(days=9)
        # chinese new year and covid-19 impact monthly revenue deadline
        replacements = {
                        datetime.datetime(2020, 2, 10): datetime.datetime(2020, 2, 15),
                        datetime.datetime(2021, 2, 10): datetime.datetime(2021, 2, 15),
                        datetime.datetime(2022, 2, 10): datetime.datetime(2022, 2, 14),
                        }
        replacer = replacements.get
        index = [replacer(n, n) for n in index]

        index = self.to_business_day(index)

        ret = FinlabDataFrame(self.values, index=index, columns=self.columns)
        ret.index.name = 'date'
        return ret

    def _index_date_to_str_season(self):

        # index is already str
        if len(self.index) == 0 or not isinstance(self.index[0], pd.Timestamp):
          return self

        q = self.index.strftime('%m').astype(int).map({5:1, 8:2, 9:2, 10:3, 11:3, 3:4, 4:4})
        year = self.index.year.copy()
        year -= (q == 4)
        index = year.astype(str) + '-Q' + q.astype(str)

        return FinlabDataFrame(self.values, index=index, columns=self.columns)

    def deadline(self):
        return self._index_str_to_date_season(detail=False)

    def _index_str_to_date_season(self, detail=True):

      disclosure_dates = self.calc_disclosure_dates(detail).unstack()
      self.columns.name = 'stock_id'

      values = self.unstack()
      disclosure_dates = disclosure_dates.loc[values.index]

      df = pd.DataFrame({
        'value': values.values,
        'disclosures': disclosure_dates.values,
      }, index=disclosure_dates.index).reset_index()

      ret = df.drop_duplicates(['disclosures', 'stock_id']).pivot('disclosures', 'stock_id')['value'].ffill()
      ret = FinlabDataFrame(ret)
      ret.index.name = 'date'
      return ret

    @staticmethod
    def calc_disclosure_dates(detail=True):

      cinfo = data.get('company_basic_info').copy()
      cinfo['id'] = cinfo.stock_id.str.split(' ').str[0]
      cinfo = cinfo.set_index('id')
      cinfo = cinfo[~cinfo.index.duplicated(keep='last')]

      def calc_default_disclosure_dates(s):
        sid = s.name
        cat = cinfo.loc[sid].產業類別 if sid in cinfo.index else 'etf'

        if cat == '金融業':
          calendar = {
            '1': '-05-15',
            '2': '-08-31',
            '3': '-11-14',
            '4': '-03-31',
          }
        elif cat == '金融保險業':
          calendar = {
            '1': '-04-30',
            '2': '-08-31',
            '3': '-10-31',
            '4': '-03-31',
          }
        else:
          calendar = {
            '1': '-05-15',
            '2': '-08-14',
            '3': '-11-14',
            '4': '-03-31',
          }

        get_year = lambda year, season: str(year) if int(season) != 4 else str(int(year) + 1)

        return pd.to_datetime(s.index.map(lambda d: get_year(d[:4], d[-1]) + calendar[d[-1]]))

      def season_end(s):

        calendar = {
          '1': '-3-31',
          '2': '-6-30',
          '3': '-9-30',
          '4': '-12-31',
        }
        return pd.to_datetime(s.index.map(lambda d: d[:4] + calendar[d[-1]]))

      disclosure_dates = data.get('financial_statement:board_approval_date').copy()
      audit_dates = data.get('financial_statement:audit_date')
      disclosure_dates = disclosure_dates.where(audit_dates > disclosure_dates).fillna(audit_dates)
      disclosure_dates = pd.DataFrame(disclosure_dates)

      disclosure_dates = disclosure_dates.applymap(
        lambda d: pd.Timestamp(datetime.datetime.fromordinal(int(d))) if d == d else pd.NaT)
      financial_season_end = disclosure_dates.apply(season_end)
      default_disclosure_dates = disclosure_dates.apply(calc_default_disclosure_dates)

      disclosure_dates[(disclosure_dates > default_disclosure_dates)
                       | (disclosure_dates < financial_season_end)] = pd.NaT
      disclosure_dates[(disclosure_dates.diff() <= datetime.timedelta(days=0))] = pd.NaT
      disclosure_dates.loc['2019-Q1', '3167'] = pd.NaT
      disclosure_dates.loc['2015-Q1', '5536'] = pd.NaT
      disclosure_dates.loc['2018-Q1', '5876'] = pd.NaT

      disclosure_dates = disclosure_dates.fillna(default_disclosure_dates)
      disclosure_dates.columns.name = 'stock_id'

      if detail:
        return disclosure_dates
      return default_disclosure_dates

    def average(self, n):
        return self.rolling(n, min_periods=int(n/2)).mean()

    def is_largest(self, n):
        return self.apply(lambda s: s.nlargest(n), axis=1).notna()

    def is_smallest(self, n):
        return self.apply(lambda s: s.nsmallest(n), axis=1).notna()

    def is_entry(self):
        return (self & ~self.shift(fill_value=False))

    def is_exit(self):
        return (~self & self.shift(fill_value=False))

    def rise(self, n=1):
        return self > self.shift(n)

    def fall(self, n=1):
        return self < self.shift(n)

    def groupby_category(self):
        categories = data.get('security_categories')
        cat = categories.set_index('stock_id').category.to_dict()
        org_set = set(cat.values())
        set_remove_illegal = set(
            o for o in org_set if isinstance(o, str) and o != 'nan')
        set_remove_illegal

        refine_cat = {}
        for s, c in cat.items():
            if c == None or c == 'nan':
                refine_cat[s] = '其他'
                continue

            if c == '電腦及週邊':
                refine_cat[s] = '電腦及週邊設備業'
                continue

            if c[-1] == '業' and c[:-1] in set_remove_illegal:
                refine_cat[s] = c[:-1]
            else:
                refine_cat[s] = c

        col_categories = pd.Series(self.columns.map(
            lambda s: refine_cat[s] if s in cat else '其他'))

        return self.groupby(col_categories.values, axis=1)

    def entry_price(self, trade_at='close'):

        signal = self.is_entry()
        adj = data.get('etl:adj_close') if trade_at == 'close' else data.get(
            'etl:adj_open')
        adj, signal = adj.reshape(
            adj.loc[signal.index[0]: signal.index[-1]], signal)
        return adj.bfill()[signal.shift(fill_value=False)].ffill()

    def sustain(self, nwindow, nsatisfy=None):
        nsatisfy = nsatisfy or nwindow
        return self.rolling(nwindow).sum() >= nsatisfy

    def quantile_row(self, c):
        s = self.quantile(c, axis=1)
        return s

    def exit_when(self, exit):

        df, exit = self.reshape(self, exit)

        df.fillna(False, inplace=True)
        exit.fillna(False, inplace=True)

        entry_signal = df.is_entry()
        exit_signal = df.is_exit()
        exit_signal |= exit

        # build position using entry_signal and exit_signal
        position = pd.DataFrame(np.nan, index=df.index, columns=df.columns)
        position[entry_signal] = 1
        position[exit_signal] = 0

        position.ffill(inplace=True)
        position = position == 1
        position.fillna(False)
        return position

    def hold_until(self, exit, nstocks_limit=100, stoploss=-np.inf, takeprofit=np.inf, trade_at='close', rank=None):

        union_index = self.index.union(exit.index)
        intersect_col = self.columns.intersection(exit.columns)

        if stoploss != -np.inf or takeprofit != np.inf:
            price = data.get(f'etl:adj_{trade_at}')
            union_index = union_index.union(
                price.loc[union_index[0]: union_index[-1]].index)
            intersect_col = intersect_col.intersection(price.columns)
        else:
            price = pd.DataFrame()

        if rank is not None:
            union_index = union_index.union(rank.index)
            intersect_col = intersect_col.intersection(rank.columns)

        entry = self.reindex(union_index, columns=intersect_col,
                             method='ffill').ffill().fillna(False)
        exit = exit.reindex(union_index, columns=intersect_col,
                            method='ffill').ffill().fillna(False)

        if price is not None:
            price = price.reindex(
                union_index, columns=intersect_col, method='ffill')

        if rank is not None:
            rank = rank.reindex(
                union_index, columns=intersect_col, method='ffill')
        else:
            rank = pd.DataFrame(1, index=union_index, columns=intersect_col)

        max_rank = rank.max().max()
        min_rank = rank.min().min()
        rank = (rank - min_rank) / (max_rank - min_rank)
        rank.fillna(0, inplace=True)

        def rotate_stocks(ret, entry, exit, nstocks_limit, stoploss=-np.inf, takeprofit=np.inf, price=None, ranking=None):

            nstocks = 0

            ret[0][np.argsort(entry[0])[-nstocks_limit:]] = 1
            ret[0][exit[0] == 1] = 0
            ret[0][entry[0] == 0] = 0

            entry_price = np.empty(entry.shape[1])
            entry_price[:] = np.nan

            for i in range(1, entry.shape[0]):

                # regitser entry price
                if stoploss != -np.inf or takeprofit != np.inf:
                    is_entry = ((ret[i-2] == 0) if i >
                                1 else (ret[i-1] == 1))

                    is_waiting_for_entry = np.isnan(entry_price) & (ret[i-1] == 1)

                    is_entry |= is_waiting_for_entry

                    entry_price[is_entry == 1] = price[i][is_entry == 1]

                    # check stoploss and takeprofit
                    returns = price[i] / entry_price
                    stop = (returns > 1 + abs(takeprofit)
                            ) | (returns < 1 - abs(stoploss))
                    exit[i] |= stop

                # run signal
                rank = (entry[i] * ranking[i] + ret[i-1] * 3)
                rank[exit[i] == 1] = -1
                rank[(entry[i] == 0) & (ret[i-1] == 0)] = -1

                ret[i][np.argsort(rank)[-nstocks_limit:]] = 1
                ret[i][rank == -1] = 0

            return ret

        ret = pd.DataFrame(0, index=entry.index, columns=entry.columns)
        ret = rotate_stocks(ret.values,
                            entry.astype(int).values,
                            exit.astype(int).values,
                            nstocks_limit,
                            stoploss,
                            takeprofit,
                            price=price.values,
                            ranking=rank.values)

        return pd.DataFrame(ret, index=entry.index, columns=entry.columns)
