from abc import ABC, abstractmethod
import pandas as pd
import datetime
import numbers
import os
import shioaji as sj
from finlab.online.enums import *

class Order():

  def __init__(self, order_id:str, stock_id: str, action:Action, price:float, quantity:float, status:OrderStatus, order_condition:OrderCondition, time):
    self.order_id = order_id
    self.stock_id = stock_id
    self.action = action
    self.price = price
    self.quantity = quantity
    self.status = status
    self.time = time

  def to_dict(self):
    return {'order_id': self.order_id,
            'stock_id': self.stock_id,
            'action': self.action,
            'price': self.price,
            'quantity': self.quantity,
            'status': self.status,
            'time': self.time,
            }

  @classmethod
  def from_dict(cls, d):
    return cls(**d)

  @classmethod
  def from_shioaji(cls, trade):
    if trade.order.action == 'Buy':
      action = Action.BUY
    elif trade.order.action == 'Sell':
      action = Action.SELL
    else:
      raise Exception('trader order action should be "Buy" or "Sell"')

    status = {
      'PendingSubmit': OrderStatus.NEW,
      'PreSubmitted': OrderStatus.NEW,
      'Submitted': OrderStatus.NEW,
      'Failed': OrderStatus.CANCEL,
      'Cancelled': OrderStatus.CANCEL,
      'Filled': OrderStatus.FILLED,
      'Filling': OrderStatus.PARTIALLY_FILLED,
      'PartFilled': OrderStatus.PARTIALLY_FILLED,
    }[trade.status.status]

    order_condition = {
      'Cash': OrderCondition.CASH,
      'MarginTrading': OrderCondition.MARGIN_TRADING,
      'ShortSelling': OrderCondition.SHORT_SELLING,
    }[trade.order.order_cond]

    return Order.from_dict({
     'order_id': trade.status.id,
     'stock_id': trade.contract.code,
     'action': action,
     'price': trade.order.price if trade.status.modified_price == 0 else trade.status.modified_price,
     'quantity': trade.order.quantity,
     'status': status,
     'order_condition': order_condition,
     'time': trade.status.order_datetime,
    })
  def __repr__(self):
    return str(self.to_dict())

class Stock():

  attrs = {'stock_id':str, 'open':numbers.Number, 'high':numbers.Number,
           'low':numbers.Number, 'close':numbers.Number,
           'bid_price':numbers.Number, 'bid_volume':numbers.Number,
           'ask_price':numbers.Number, 'ask_volume':numbers.Number}

  def __init__(self, **args):
    for key, value in args.items():
      assert isinstance(value, Stock.attrs[key])
      setattr(self, key, value)

  def to_dict(self):
    return {a:getattr(self, a) for a in Stock.attrs}

  def __repr__(self):
    return str(self.to_dict())

  @classmethod
  def from_shioaji(cls, snapshot):
    d = snapshot
    return cls(stock_id=d.code, open=d.open, high=d.high, low=d.low, close=d.close,
        bid_price=d.buy_price, ask_price=d.sell_price, bid_volume=d.buy_volume, ask_volume=d.sell_volume)

class Account(ABC):

  @abstractmethod
  def create_order(self, action, stock_id, quantity, price=None, force=False, wait_for_best_price=False):
    pass

  @abstractmethod
  def update_order(self, order_id, price=None, quantity=None):
    pass

  @abstractmethod
  def cancel_order(self, order_id):
    pass

  @abstractmethod
  def get_orders(self):
    pass

  @abstractmethod
  def get_stocks(self, stock_ids):
    pass

  @abstractmethod
  def get_position(self):
    pass

  @abstractmethod
  def get_total_balance():
    pass

