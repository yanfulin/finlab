from enum import Enum

class OrderStatus(Enum):
  NEW=1
  PARTIALLY_FILLED=2
  FILLED=3
  CANCEL=4

class Action(Enum):
  BUY=1
  SELL=2

class OrderCondition(Enum):
  CASH=1,
  MARGIN_TRADING=2,
  SHORT_SELLING=3,
