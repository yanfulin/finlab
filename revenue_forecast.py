from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql
import datetime
import os
import shutil
import numpy as np
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from fbprophet import Prophet
from datetime import date



def get_revenue_from_db():
    rev = pd.DataFrame()
    engine = create_engine('sqlite:///fin_db', echo=False)
    # 將新建的DataFrame儲存為MySQL中的資料表
    rev=pd.read_sql('SELECT "date", "2330", "2331" FROM Revenue', engine)


    rev.date=rev.date.str.replace("M","")
    rev.date=pd.to_datetime(rev.date)
    rev=rev.set_index("date")
    print(rev)
    print(rev.info())
    quarterly_rev=rev.resample("Q").sum()
    print(quarterly_rev)


    # df.columns = ["ds", "y"]
    # df.ds = pd.to_datetime(df.ds)
    # print(df.head())
    # m1 = Prophet()
    # m1.fit(df)
    # future1 = m1.make_future_dataframe(periods=240, freq='MS')
    # forecast1 = m1.predict(future1)
    # print(forecast1)



if __name__ == '__main__':
    get_revenue_from_db()