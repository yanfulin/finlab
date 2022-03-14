import pandas as pd
from pydantic import validator
from shioaji.base import BaseModel
from collections import abc, Counter
from datetime import datetime
from enum import Enum
from typing import List, Optional
from shioaji.order import Deal


class BaseMethod(BaseModel):
    def get_data(self):
        return [s.get_dict() for s in self.data]

    def get_df(self):
        df = pd.DataFrame(self.get_data())
        df = df.set_index(['code', 'date'])
        return df


class BaseMapping(BaseModel, abc.Mapping):
    def __len__(self):
        return len(self.__values__)

    def __getitem__(self, name):
        return getattr(self, name)

    def get_dict(self):
        dataset = {}
        for k, v in self._iter():
            if isinstance(v, Enum):
                dataset[k] = v.value
            else:
                dataset[k] = v
        return dataset


class AccountName(str, Enum):
    bank_remit = 'bank_remit'
    shioaji_stock = 'shioaji_stock'
    shioaji_future = 'shioaji_future'


class Account(BaseMapping):
    name: AccountName = AccountName.shioaji_stock
    acc_balance: int
    date: datetime
    transfer: int
    available_balance: int
    update_time: datetime = datetime.now()


class AccountPost(BaseMethod):
    data: List[Account]

    def get_df(self):
        df = pd.DataFrame(self.get_data())
        df = df.set_index(['name', 'date'])
        return df


class Strategy(BaseMapping):
    name: str
    account: AccountName = AccountName.shioaji_stock
    enable: bool
    size: int
    schedule: str
    update_time: datetime = datetime.now()

    @validator('schedule')
    def check_crontab_type(cls, v):
        space = ' '
        check = Counter(v)[space]
        if (check != 4) or (space * 2) in v:
            raise ValueError('crontab type error')
        return v


class StrategyPost(BaseMethod):
    data: List[Strategy]

    def get_df(self):
        df = pd.DataFrame(self.get_data())
        df = df.set_index(['name'])
        return df


class Order(BaseMapping):
    strategy: Optional[str]
    action: str
    cancel_quantity: int
    category: str
    code: str
    day_trade: str
    deal_quantity: int
    exchange: str
    modified_price: float
    modified_time: Optional[datetime]
    name: str
    order_datetime: Optional[datetime]
    order_type: str
    ordno: str
    price: float
    price_type: str
    quantity: int
    reference: float
    seqno: str
    status: str
    status_code: str
    deals: List[Deal] = None
    trade_type: str = None


class OrderPost(BaseMethod):
    data: List[Order]

    def get_df(self):
        df = pd.DataFrame(self.get_data())
        df = df.set_index(['seqno', 'order_datetime'])
        return df


class ExecuteOrder(Order):
    strategy: str


class ExecuteOrderPost(OrderPost):
    data: List[ExecuteOrder]


class Position(BaseMapping):
    code: str
    date: datetime = datetime.now().date()
    quantity: float
    price: float
    pnl: int
    update_time: datetime = datetime.now()


class PositionPost(BaseMethod):
    data: List[Position]