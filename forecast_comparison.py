
import datetime
import logging
import os
import shutil
import numpy as np
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from datetime import date
import sqlite3
from prophet.plot import plot_plotly, plot_components_plotly

# 定義輸出格式
FORMAT = '%(asctime)s %(filename)s %(levelname)s:  %(message)s'
# Logging初始設定 + 上定義輸出格式
logging.basicConfig(level=logging.INFO, format=FORMAT)

def drop_sql_table(table_name="EPS_forecast_table"):
    logging.info(f"drop table  {table_name}")
    conn = sqlite3.connect('fin_db')  # 建立連線
    cursor = conn.cursor()
    sql_command=f"DROP TABLE if exists {table_name}"
    cursor.execute(sql_command)
    logging.info(f"drop table  {table_name}")
    conn.commit()
    conn.close()




def make_forecast(stock_id):

    #comment
    logging.info(f"start to make forecast for stock_id = {stock_id}")
    filelist = ["5Y", "10Y", "ALL"]

    # Set Folder Targets for Revenue Info
    # Based on 5/10/All year revenue to do the coming 5 year forecast
    for file in filelist:
        logging.info(f"stock_id is {stock_id}")
        conn = sqlite3.connect('fin_db')  # 建立連線
        sql_query= f'SELECT date, "{stock_id}" FROM Revenue'
        logging.info(f"sql_query is {sql_query}")
        rev_df = pd.read_sql_query(sql_query, conn, parse_dates={"date": {"format":"%Y-M%m"}})  # 取得回傳資料
        conn.close()
        # print(rev_df)

        #rev_df.date = rev_df.date.str.replace("-M", "")
        rev_df.date = pd.to_datetime(rev_df.date)
        rev_df = rev_df.set_index("date")
        #print(rev_df)
        #print(rev_df.info())
        quarterly_rev = rev_df.resample("Q").sum()
        #print("quarlterly revenue", quarterly_rev)

        df=rev_df.reset_index()
        df.columns=["ds", "y"]
        df.ds=pd.to_datetime(df.ds)
        print(df.tail())
        m1=Prophet()
        m1.fit(df)
        future1 = m1.make_future_dataframe(periods=240,freq="MS")
        forecast1 = m1.predict(future1)
        #fig1 = m1.plot(forecast1)
        #fig1.show()
        #plot_plotly(m1, forecast1)
        #print(forecast1.dtypes)
        #print (forecast1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(10))
        if not os.path.exists(f'data/02_intermediate/{stock_id}'):
            os.makedirs(f'data/02_intermediate/{stock_id}')
        if not os.path.exists(f'data/02_intermediate/{stock_id}/forecast/'):
            os.makedirs(f'data/02_intermediate/{stock_id}/forecast/')
        forecast1.to_csv(f'data/02_intermediate/{stock_id}/forecast/{file}.csv')


def get_forecast(stock_id, year=10):

    #TODO 每年的股票股數會變化，所以EPS也會跟著變動，得調整每年古樹，dividend_announcement裡面有過去的股數，company basic info 有最新股數



    # stock_revenue_file = Path.cwd() / stock_id / "ALL.html"
    # EPS_per_quarte_file = Path.cwd() / stock_id / "EPS_per_quarter.csv"
    # Balance_Sheet_file = Path.cwd() / stock_id / "Balance_Sheet_BS_M_QUAR.html"
    # Finance_index_file = Path.cwd() / stock_id / "獲利指標.html"
    # df = pd.read_html(stock_revenue_file)[0]
    # df_EPS = pd.read_csv(EPS_per_quarte_file)
    # df_BS = pd.read_html(Balance_Sheet_file)[0]
    # df_Finance = pd.read_html(Finance_index_file)[0]

    # df_EPS :  Get 稅後淨利率 form database. Quarterly Net Profit Ratio.
    logging.info(f"start to get EPS Ratio for stock_id = {stock_id}")
    conn = sqlite3.connect('fin_db')  # 建立連線
    sql_query = f'SELECT date, "{stock_id}" FROM "fundamental_features-稅後淨利率"'
    logging.info(f"sql_query is {sql_query}")
    df_EPS = pd.read_sql_query(sql_query, conn, parse_dates={"date": {"format": "%Y-Q%q"}})  # 取得回傳資料
    conn.close()
    df_EPS.date = pd.to_datetime(df_EPS.date)
    df_EPS["Quarter"] = df_EPS['date'].dt.quarter
    df_EPS["Year"] = df_EPS['date'].dt.year
    # print(df_EPS.tail())
    # print(df_EPS.info())

    logging.info(f"start to get Market Capital for stock_id = {stock_id}")
    conn = sqlite3.connect('fin_db')  # 建立連線
    sql_query = f'SELECT 已發行普通股數或TDR原發行股數 FROM "company_basic_info" where stock_id={stock_id}'
    logging.info(f"sql_query is {sql_query}")
    df_share_qty = pd.read_sql_query(sql_query, conn)  # 取得回傳資料
    conn.close()
    print(df_share_qty)




    logging.info(f"start to get forecast for stock_id = {stock_id}")
    conn = sqlite3.connect('fin_db')  # 建立連線
    sql_query = f'SELECT date, "{stock_id}" FROM Revenue'
    logging.info(f"sql_query is {sql_query}")
    rev_df = pd.read_sql_query(sql_query, conn, parse_dates={"date": {"format": "%Y-M%m"}})  # 取得回傳資料
    conn.close()
    #print(rev_df)

    # rev_df.date = rev_df.date.str.replace("-M", "")
    rev_df.date = pd.to_datetime(rev_df.date)
    rev_df = rev_df.set_index("date")
    # print(rev_df)
    # print(rev_df.info())
    quarterly_rev = rev_df.resample("Q").sum()
    # print("quarlterly revenue", quarterly_rev)

    df = rev_df.reset_index()
    df.columns = ["ds", "y"]
    df.ds = pd.to_datetime(df.ds)

    # col = ["月別", "單月營收(億)"]
    # df = df[col]
    # df.columns = ["ds", "y"]
    df['forecast'] = 'Actual'
    #print("forecast is actual????")
    #print(df.head())
    #df.ds = pd.to_datetime(df.ds)

    df_5Y = pd.read_csv(f"data/02_intermediate/{stock_id}/forecast/5Y.csv")
    df_10Y = pd.read_csv(f"data/02_intermediate/{stock_id}/forecast/10Y.csv")
    df_ALL = pd.read_csv(f"data/02_intermediate/{stock_id}/forecast/ALL.csv")

    df_5Y = df_5Y[["ds", "yhat"]].rename(columns={"yhat": "5Y_yhat"})
    df_5Y.ds = pd.to_datetime(df_5Y.ds)
    df_10Y = df_10Y[["ds", "yhat"]].rename(columns={"yhat": "10Y_yhat"})
    df_10Y.ds = pd.to_datetime(df_10Y.ds)
    df_ALL = df_ALL[["ds", "yhat"]].rename(columns={"yhat": "ALL_yhat"})
    df_ALL.ds = pd.to_datetime(df_ALL.ds)

    df = pd.merge(df_ALL, df, how='left', on='ds')
    df = df.merge(df_10Y, how="left", on='ds')
    df = df.merge(df_5Y, how="left", on='ds')
    if year == 5:
        df["merged"] = np.where(df.y.isna(), df["5Y_yhat"], df.y)
    elif year == 10:
        df["merged"] = np.where(df.y.isna(), df["10Y_yhat"], df.y)
    else:
        df["merged"] = np.where(df.y.isna(), df["ALL_yhat"], df.y)
    df["forecast"] = np.where(df.forecast.isna(), "forecast", "actual")
    df['Year']=df.ds.dt.year
    df['Month'] = df.ds.dt.month
    df['Quarter'] = df.ds.dt.quarter
    #df['EPS_ratio']=df_EPS.loc[(df_EPS["Year"]==df["Year"]) & (df_EPS["Quarter"]==df["Quarter"])]
    df = pd.merge(df, df_EPS, how="left", on=["Year", "Quarter"])
    df['EPS_ratio']=df[stock_id]
    #print(df.info())
    #print(df.head())

    # add column "Quarter" and "Year" and "EPS estimation"
    Q1_month = [1, 2, 3]
    Q2_month = [4, 5, 6]
    Q3_month = [7, 8, 9]
    Q4_month = [10, 11, 12]
    # add column "Quarter" and "Year"
    df["Year"] = 0
    this_year = date.today().year
    this_month = date.today().month
    # this_quarter = date.today().dt.quarter

    # for index, row in df.iterrows():
    #     year_row = row['ds'].year
    #     month = row['ds'].month
    #     quarter = row['ds'].quarter
    #     df.loc[index, "Year"] = year_row

    for index, row in df.iterrows():
        df.loc[index, "Year"] = row['ds'].year
        # logging.info(f'row is {row}')
        # logging.info(f'year is {row["ds"].year}')
        # logging.info(f'month is {row["ds"].month}')
        # logging.info(f'quarter is {row["ds"].quarter}')

        month = row['ds'].month
        if month in Q1_month:
            Q_number = 1
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number][stock_id].iloc[-1]
        elif month in Q2_month:
            Q_number = 2
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number][stock_id].iloc[-1]
        elif month in Q3_month:
            Q_number = 3
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number][stock_id].iloc[-1]
        else:
            Q_number = 4
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number][stock_id].iloc[-1]
    #print(df[["ds","Quarter",'EPS_ratio']].head())

    df["EPS_ratio"]=np.where(df[stock_id].isnull(),df["EPS_ratio"],df[stock_id])
    #print(df[["ds", "Quarter", 'EPS_ratio']].head())


    ##Todo get the captital for each stock from balance sheet
    # I put TSMC capital below to verify the formula first. This is to be udpated.
    #print(df_share_qty["已發行普通股數或TDR原發行股數"][0])
    df["capital"] = df_share_qty["已發行普通股數或TDR原發行股數"][0]
    # df["capital"] = 2593
    # print("普通股股本", df["capital"])
    df["EPS_forecast"] = df["merged"] * df["EPS_ratio"] / df["capital"] * 10
    df['date'] = df.apply(lambda x: str(x["Year"])+"-Q"+str(x["Quarter"]),axis=1)
    #print("date============", df.date)
    #print(df.head())
    #print(df[(df.ds > "2020-12") & (df.ds < "2022-1")][["ds","date"]])
    forecast_df=df[df.ds>"2019-12-31"].groupby(["date"]).sum()
    #print(forecast_df)
    forecast_df=forecast_df.reset_index()
    #print("df after reset", forecast_df)
    #forecast_df = forecast_df[["date", "EPS_forecast"]].reset_index().apply(lambda x: x)
    forecast_df = forecast_df[["date", "EPS_forecast"]].apply(lambda x: x)
    forecast_df = forecast_df.rename(columns={"EPS_forecast": stock_id})
    #print(forecast_df)
    logging.info(f"start to get forecast for stock_id = {stock_id}")
    conn = sqlite3.connect('fin_db')  # 建立連線
    #forecast_df.to_sql("EPS_forecast_table", conn, if_exists='append',index=True)
    ##Todo 這個方式太慢了，應該接Forecast_df寫出到檔案，然後合併一次寫入to sql
    ##Todo Or we can create the sqlite3 table with correct columns in the beginning

    try:
        # this will fail if there is a new column
        forecast_df.to_sql(name="EPS_forecast_table", con=conn, if_exists='append',index=True)
        logging.info(f'{stock_id} has appended')
    except:
        data = pd.read_sql('SELECT * FROM "EPS_forecast_table"', conn)
        logging.info(f'retrieve data for {stock_id}')
        print("data",data.tail())

        df2 = pd.merge(data, forecast_df, how="left", on="date")
        print("df2...............",df2.tail())
        df2.to_sql(name="EPS_forecast_table", con=conn, if_exists='replace',index=False)
    #sql_query = f'SELECT date, "{stock_id}" FROM Revenue'
    #logging.info(f"sql_query is {sql_query}")
    #rev_df = pd.read_sql_query(sql_query, conn, parse_dates={"date": {"format": "%Y-M%m"}})  # 取得回傳資料

    conn.close()

    return df[["ds", "merged", "forecast", "Quarter", "Year", "capital", "EPS_ratio", "EPS_forecast"]]


def plot_forecast(df,stock_id):
    print("start to plot forecast for stock_id = ", stock_id)
    fig, ax = plt.subplots(figsize=(9, 6))
    # df.plot(kind='line', x='ds', y=["y", "ALL_yhat", "10Y_yhat", "5Y_yhat"], ax=ax)
    df.plot(kind='line', x='ds', y=["merged"], ax=ax)
    fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
    plt.title(f"STOCK ID ={stock_id}")
    plt.show(block=True)
    plt.interactive(False)



#make_forecast("3209")
# make_forecast("2330")
# make_forecast("8213")
drop_sql_table()
get_forecast("3209")
# get_forecast("2330")
# get_forecast("8213")