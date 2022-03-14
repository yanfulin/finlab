import pandas as pd
import numpy as np
from finlab.shioaji_order_executor.utils import ShioajiApi, logger, get_snapshot
from abc import ABC, abstractmethod


class Base(ABC):
    @abstractmethod
    def calculate_target_positions(self, stock_list, money):
        pass


class Portfolio(ShioajiApi, Base):

    def calculate_target_positions(self, stock_list, money, lowest_fee=20, discount=1, add_cost=10):

        """Calculate the maximum achievable portfolio under the amount limit.
        Args:
         stock_list(list):order list,ex:['1101','1102']
         money(int):invest size
         lowest_fee(int):trading fee
         discount(float):fee
         add_cost(int):additional cost of risk diversification
        Returns:
            portfolio(dict): {stock_id:quantity},ex:{'2543': 4.0, '2841': 4.0, '2897': 4.0, '3011': 4.0, '5345': 4.0}
            portfolio_tatal_value(int): Estimated total amount of investment portfolio
        """
        # Use current price
        stock_list = pd.Series(get_snapshot(self.api, stock_list))
        # print('estimate price according to', price.index[-1])
        nstock = len(stock_list)
        logger.info(f'initial number of stock:{nstock}')
        while (money / len(stock_list)) < (lowest_fee - add_cost) * 1000 / 1.425 / discount:
            stock_list = stock_list[stock_list != stock_list.max()]
        nstock = len(stock_list)
        logger.info(f'after considering fee:{nstock}')

        while True:
            invest_amount = (money / len(stock_list))
            ret = np.floor(invest_amount / stock_list / 1000)
            if (ret == 0).any():
                stock_list = stock_list[stock_list != stock_list.max()]
            else:
                break
        nstock = len(stock_list)
        logger.info(f'after considering 1000 share:{nstock}')
        portfolio = ret.to_dict()
        portfolio_tatal_value = (ret * stock_list * 1000).sum()
        return portfolio, portfolio_tatal_value
