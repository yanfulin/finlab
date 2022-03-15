from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql


def get_revenue_from_db():
    rev = pd.DataFrame()
    engine = create_engine('sqlite:///fin_db', echo=False)
    # 將新建的DataFrame儲存為MySQL中的資料表
    rev=pd.read_sql('SELECT "2330" FROM Revenue', engine)
    print(rev.tail())
    print("Write to MySQL successfully!")



if __name__ == '__main__':
    get_revenue_from_db()