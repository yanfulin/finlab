# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql
finlab.login('75b0ztI1TfihNQAeO0UyBJVYckRmF9Rr10E3LhE4JlIDY1uh8NLgUJBCXafSgsJf#free')







def fetch_fin_data():
    # 取得股價淨值比
    rev = data.get("monthly_revenue:當月營收")
    #rev=rev.loc['2018-M01':, ['2317', '2330', '3008']]
    # rrsi=data.indicator('RSI', timeperiod=14)
    # print(rrsi)

    #PER = data.get('price_earning_ratio:本益比')
    #Yield = data.get('price_earning_ratio:殖利率(%)')
    # PBR = data.get('price_earning_ratio:股價淨值比')
    # #print("PER", PER.loc[:,["2330"]])
    # #print("Yeild", Yield.loc[Yield.index[-1],["2330"]])
    # print("PBR", PBR.loc['2022',["2330"]])
    #
    # EPS=data.get('fundamental_features:每股稅後淨利')
    # print("EPS", EPS.loc[:, ["2330"]])
    # rrsi=data.indicator('RSI', timeperiod=14)
    # print(rrsi)

    # # 初始化資料庫連線，使用pymysql模組
    # username = 'neo4jj'     # 資料庫帳號
    # password = 'yan12345'     # 資料庫密碼
    # host = 'db4free.net'    # 資料庫位址
    # port = '3306'         # 資料庫埠號
    # database = 'stock_finlab'   # 資料庫名稱
    #
    # engine = create_engine(
    #     f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
    # )
    engine = create_engine('sqlite:///fin_db', echo=True)
    # 將新建的DataFrame儲存為MySQL中的資料表，不儲存index列
    rev.to_sql('Revenue', engine, if_exists='replace')
    print("Write to MySQL successfully!")

    # db = MetaData()  # 取得類似於 Cursor 的物件
    #
    # demo_table = Table(  # 代表資料表數據的物件
    #     'demo_table', db,
    #     Column('id', Integer, primary_key=True),
    #     Column('name', String),
    #     Column('data', String),
    # )
    #
    # db.create_all(engine)  # 創建資料表

    # conn = sqlite3.connect('fin_db')  # 建立連線
    # cursor = conn.cursor()  # 取得游標物件
    # cursor.execute('SELECT * FROM app_info;')  # 執行 SQL 語法
    #
    # records = cursor.fetchall()  # 取得回傳資料
    # print(records)
    # print("record type => ", type(records))  # 回傳資料型態
    # print("record[0] type => ", type(records[0]))  # 回傳資料內部的元素型態


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fetch_fin_data()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
