from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql
import matplotlib.pyplot as plt
from fbprophet import Prophet
from fbprophet.plot import plot_plotly, plot_components_plotly

#time
import datetime as datetime

#Prophet
from fbprophet import Prophet


from sklearn import metrics

def month_conversion(row):
    row.date=row.date.replace("M","")
    return row

def forecast_revenue(df):
    df.ds = pd.to_datetime(df.ds)
    m = Prophet()
    m.fit(df)
    future = m.make_future_dataframe(periods=240, freq='MS')
    forecast = m.predict(future)
    return forecast

def get_revenue_from_db():
    rev = pd.DataFrame()
    engine = create_engine('sqlite:///fin_db', echo=False)
    # 將新建的DataFrame儲存為MySQL中的資料表
    rev=pd.read_sql('SELECT * FROM Revenue', engine)
    rev=rev.apply(month_conversion, axis=1)
    print(rev.info())

    df=rev.copy()
    df.rename(columns={'date':'ds'}, inplace=True)
    forecast_df=pd.DataFrame()
    for column in df.columns:
        if column != "ds" and df[column].notnull().sum()>5:
            df.rename(columns={column: 'y'}, inplace=True)
            forecast = forecast_revenue(df[["ds",'y']])
            print(column, forecast.yhat[:-10])
            forecast_df["ds"]=forecast.ds
            forecast_df[column]=forecast.yhat
            df.rename(columns={'y':column}, inplace=True)
    # df.columns = ["ds", "y"]

    # forecast=forecast_revenue(rev)
    print(forecast_df)
    forecast_df.to_sql('F_Revenue', engine, if_exists='replace')
    # print (forecast[['ds', 'yhat']])
    #
    # print(forecast)
    # fig1 = m1.plot(forecast)
    # fig2 = m1.plot_components(forecast)
    #
    # plot_plotly(m1, forecast)
    # plot_components_plotly(m1, forecast)
    # plt.show()


if __name__ == '__main__':
    get_revenue_from_db()