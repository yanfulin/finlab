
import datetime
import os
import shutil
import numpy as np
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from datetime import date
import sqlite3

def make_forecast(stock_id):

    #comment
    print("start to make forecast for stock_id = ", stock_id)
    filelist = ["5Y", "10Y", "ALL"]

    # Set Folder Targets for Revenue Info
    # Based on 5/10/All year revenue to do the coming 5 year forecast
    for file in filelist:

        conn = sqlite3.connect('fin_db')  # 建立連線
        sql_query= f"SELECT date, "2330" FROM Revenue"
        rev_df = pd.read_sql(sql_query, conn)  # 取得回傳資料
        conn.close()


        #rev_df.date = rev_df.date.str.replace("M", "")
        rev_df.date = pd.to_datetime(rev_df.date)
        rev_df = rev_df.set_index("date")
        print(rev_df)
        print(rev_df.info())
        quarterly_rev = rev_df.resample("Q").sum()
        print("quarlterly revenue", quarterly_rev)

        df=pd.read_html(stock_revenue_file)[0]
        col=["月別", "單月營收(億)"]
        df=df[col]
        df.columns=["ds", "y"]
        df.ds=pd.to_datetime(df.ds)
        #print(df.head())
        m1=Prophet()
        m1.fit(df)
        future1 = m1.make_future_dataframe(periods=240, freq='MS')
        forecast1 = m1.predict(future1)
        #print(forecast1.dtypes)
        #print (forecast1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
        if not os.path.exists(stock_id):
            os.makedirs(stock_id)
        if not os.path.exists(f'{stock_id}/forecast/'):
            os.makedirs(f'{stock_id}/forecast/')
        forecast1.to_csv(f'{stock_id}/forecast/{file}.csv')




    # print("start to make forecast for stock_id = ", stock_id)
    # filelist = ["5Y", "10Y", "ALL"]
    #
    # # Set Folder Targets for Revenue Info
    # # Based on 5/10/All year revenue to do the coming 5 year forecast
    # for file in filelist:
    #     file_html =str(file)+".html"
    #     stock_revenue_file = Path.cwd() / stock_id / file_html
    #     df=pd.read_html(stock_revenue_file)[0]
    #     col=["月別", "單月營收(億)"]
    #     df=df[col]
    #     df.columns=["ds", "y"]
    #     df.ds=pd.to_datetime(df.ds)
    #     #print(df.head())
    #     m1=Prophet()
    #     m1.fit(df)
    #     future1 = m1.make_future_dataframe(periods=240, freq='MS')
    #     forecast1 = m1.predict(future1)
    #     #print(forecast1.dtypes)
    #     #print (forecast1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
    #     if not os.path.exists(stock_id):
    #         os.makedirs(stock_id)
    #     if not os.path.exists(f'{stock_id}/forecast/'):
    #         os.makedirs(f'{stock_id}/forecast/')
    #     forecast1.to_csv(f'{stock_id}/forecast/{file}.csv')


def get_forecast(stock_id, year=10):
    print("start to get forecast for stock_id = ", stock_id)
    stock_revenue_file = Path.cwd() / stock_id / "ALL.html"
    EPS_per_quarte_file = Path.cwd() / stock_id / "EPS_per_quarter.csv"
    Balance_Sheet_file = Path.cwd() / stock_id / "Balance_Sheet_BS_M_QUAR.html"
    Finance_index_file = Path.cwd() / stock_id / "獲利指標.html"
    df = pd.read_html(stock_revenue_file)[0]
    df_EPS = pd.read_csv(EPS_per_quarte_file)
    df_BS = pd.read_html(Balance_Sheet_file)[0]
    df_Finance = pd.read_html(Finance_index_file)[0]

    col = ["月別", "單月營收(億)"]
    df = df[col]
    df.columns = ["ds", "y"]
    df['forecast'] = 'Actual'
    #print("forecast is actual????")
    #print(df.head())
    df.ds = pd.to_datetime(df.ds)

    df_5Y = pd.read_csv(f"{stock_id}/forecast/5Y.csv")
    df_10Y = pd.read_csv(f"{stock_id}/forecast/10Y.csv")
    df_ALL = pd.read_csv(f"{stock_id}/forecast/ALL.csv")

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
        # print (row, row['ds'].year, row['ds'].month)
        month = row['ds'].month
        if month in Q1_month:
            Q_number = 'Q1'
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number]["稅後淨利率(母公司)"].iloc[0]
        elif month in Q2_month:
            Q_number = 'Q2'
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number]["稅後淨利率(母公司)"].iloc[0]
        elif month in Q3_month:
            Q_number = 'Q3'
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number]["稅後淨利率(母公司)"].iloc[0]
        else:
            Q_number = 'Q4'
            df.loc[index, 'Quarter'] = Q_number
            df.loc[index, 'EPS_ratio'] = df_EPS[df_EPS["Quarter"] == Q_number]["稅後淨利率(母公司)"].iloc[0]


    ##Todo get the captital for each stock from balance sheet
    # I put TSMC capital below to verify the formula first. This is to be udpated.

    df["capital"] = df_BS.loc[0, "普通股股本"]
    # df["capital"] = 2593
    # print("普通股股本", df["capital"])
    df["EPS_forecast"] = df["merged"] * df["EPS_ratio"] / df["capital"] * 10

    #print(df.head())
    #print(df[(df.ds > "2020-12") & (df.ds < "2022-1")])
    return df[["ds", "merged", "forecast", "Quarter", "Year", "capital", "EPS_ratio", "EPS_forecast"]]


def get_forecast2(stock_id, year=10):
    print("start to get forecast for stock_id = ", stock_id)
    stock_revenue_file = Path.cwd() / stock_id / "ALL.html"
    EPS_per_quarte_file= Path.cwd() / stock_id / "EPS_per_quarter.csv"
    Balance_Sheet_file = Path.cwd() / stock_id / "Balance_Sheet_BS_M_QUAR.html"
    Finance_index_file = Path.cwd() / stock_id / "獲利指標.html"
    df = pd.read_html(stock_revenue_file)[0]
    df_EPS = pd.read_csv(EPS_per_quarte_file)
    df_BS = pd.read_html(Balance_Sheet_file)[0]
    df_Finance = pd.read_html(Finance_index_file)[0]

    col = ["月別", "單月營收(億)"]
    df = df[col]
    df.columns = ["ds", "y"]
    df['forecast']='Actual'
    #print("forecast is actual????")
    #print(df.head())
    df.ds = pd.to_datetime(df.ds)

    df_5Y = pd.read_csv(f"{stock_id}/forecast/5Y.csv")
    df_10Y = pd.read_csv(f"{stock_id}/forecast/10Y.csv")
    df_ALL = pd.read_csv(f"{stock_id}/forecast/ALL.csv")

    df_5Y = df_5Y[["ds", "yhat"]].rename(columns={"yhat": "5Y_yhat"})
    df_5Y.ds = pd.to_datetime(df_5Y.ds)
    df_10Y = df_10Y[["ds", "yhat"]].rename(columns={"yhat": "10Y_yhat"})
    df_10Y.ds = pd.to_datetime(df_10Y.ds)
    df_ALL = df_ALL[["ds", "yhat"]].rename(columns={"yhat": "ALL_yhat"})
    df_ALL.ds = pd.to_datetime(df_ALL.ds)

    df = pd.merge(df_ALL, df, how='left', on='ds')
    df = df.merge(df_10Y, how="left", on='ds')
    df = df.merge(df_5Y, how="left", on='ds')
    if year==5:
        df["merged"] = np.where(df.y.isna(), df["5Y_yhat"], df.y)
    elif year==10:
        df["merged"] = np.where(df.y.isna(), df["10Y_yhat"], df.y)
    else:
        df["merged"] = np.where(df.y.isna(), df["ALL_yhat"], df.y)
    df["forecast"] = np.where(df.forecast.isna(), "forecast", "actual")

    # add column "Quarter" and "Year" and "EPS estimation"
    Q1_month = [1, 2, 3]
    Q2_month = [4, 5, 6]
    Q3_month = [7, 8, 9]
    Q4_month = [10, 11, 12]
    # add column "Quarter" and "Year"
    df["Year"]=0
    this_year = date.today().year
    this_month = date.today().month
    this_quarter = (this_month-1)//3+1
    #print(date.today, this_year, this_month, this_quarter)

    df_EPS["稅後淨利率(母公司)"] = (df_EPS["稅後淨利率(母公司)"]*1)

    for index, row in df.iterrows():
        year_row = row['ds'].year
        month_row =row['ds'].month
        quarter_row = row['ds'].quarter

        df.loc[index, "Year"]= year_row
        df.loc[index, "Quarter"] = quarter_row
        # print ("Row['ds']", row['ds'],  "Year is", year_row, "Month is", month_row, "Quarter is ", quarter_row)

        if year_row < this_year-1:

            # print(df_Finance["年度"].dtype)
            # print(df_Finance[df_Finance["年度"]== year_row])
            if df_Finance["年度"].dtype == "int64":
                ratio = df_Finance[df_Finance["年度"]== year_row]["稅後淨利(%)"].iloc[0]
            else:
                ratio = df_Finance[df_Finance["年度"] == str(year_row)]["稅後淨利(%)"].iloc[0]
            if ratio == "-":
                ratio = 0.0
            ratio = float(ratio) * 0.01


            #ratio = float(ratio)
            # print(type(ratio), ratio)
            df.loc[index, 'EPS_ratio']= ratio
        elif year_row == this_year-1:
            #print (df_EPS[(df_EPS["Year"] == year_row) & (df_EPS["Quarter"] == "Q"+str(quarter_row))])
            df.loc[index, 'EPS_ratio'] = \
                df_EPS[(df_EPS["Year"] == year_row) & (df_EPS["Quarter"] == "Q"+str(quarter_row))]["稅後淨利率(母公司)"].iloc[0]
        elif year_row >= this_year:
            if quarter_row < this_quarter:
                # print(this_year, type(this_year), quarter_row, this_quarter)

                eps = df_EPS[(df_EPS["Year"] == this_year) & (df_EPS["Quarter"] == "Q"+str(quarter_row))]
                # print("Stock",stock_id, eps,"length is", len(eps))
                if len(eps) == 0:
                    df.loc[index, 'EPS_ratio'] = \
                        df_EPS[(df_EPS["Year"] == this_year - 1) & (df_EPS["Quarter"] == "Q" + str(quarter_row))]["稅後淨利率(母公司)"].iloc[0]
                else:
                    df.loc[index, 'EPS_ratio'] = df_EPS[(df_EPS["Year"] == this_year) & (df_EPS["Quarter"] == "Q"+str(quarter_row))]["稅後淨利率(母公司)"].iloc[0]
            if quarter_row >= this_quarter:
                df.loc[index, 'EPS_ratio'] =\
                    df_EPS[(df_EPS["Year"] == this_year-1) & (df_EPS["Quarter"] == "Q"+str(quarter_row))]["稅後淨利率(母公司)"].iloc[0]
        else:
            if quarter_row < this_quarter:
                df.loc[index, 'EPS_ratio'] = \
                    df_EPS[(df_EPS["Year"] == this_year) & (df_EPS["Quarter"] == "Q" + str(quarter_row))][
                        "稅後淨利率(母公司)"].iloc[0]
            if quarter_row >= this_quarter:
                df.loc[index, 'EPS_ratio'] = \
                    df_EPS[(df_EPS["Year"] == this_year - 1) & (df_EPS["Quarter"] == "Q" + str(quarter_row))][
                        "稅後淨利率(母公司)"].iloc[0]
        print(df.loc[index, 'EPS_ratio'])


    ##Todo get the captital for each stock from balance sheet
    # I put TSMC capital below to verify the formula first. This is to be udpated.

    df["capital"] = df_BS.loc[0,"普通股股本"]
    #df["capital"] = 2593
    #print("普通股股本", df["capital"])
    #print(df.dtypes)
    df["EPS_forecast"]=df["merged"]*df["EPS_ratio"]/df["capital"] * 10


    #print(df.head())
    #print(df[(df.ds > "2020-12") & (df.ds < "2022-1")])
    return df[["ds","merged","forecast","Quarter","Year","capital", "EPS_ratio", "EPS_forecast"]]


def plot_forecast(df,stock_id):
    print("start to plot forecast for stock_id = ", stock_id)
    fig, ax = plt.subplots(figsize=(9, 6))
    # df.plot(kind='line', x='ds', y=["y", "ALL_yhat", "10Y_yhat", "5Y_yhat"], ax=ax)
    df.plot(kind='line', x='ds', y=["merged"], ax=ax)
    fig.autofmt_xdate(bottom=0.2, rotation=30, ha='right')
    plt.title(f"STOCK ID ={stock_id}")
    plt.show(block=True)
    plt.interactive(False)


def Get_Balance_sheet_code(ID):
    # Get the webpage's source html code
    source = 'https://goodinfo.tw/StockInfo/StockFinDetail.asp?'
    url = source + ID
    print (url)

    # Header
    headers = {'accept': '*/*',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-CN;q=0.2',
               'content-length': '0',
               'content-type': 'application/x-www-form-urlencoded;',
               'cookie': '__gads=ID=fe562308f92c2979:T=1511748749:S=ALNI_MZDfWP11BwzcEPLDztntanza0Uc0Q; GOOD%5FINFO%5FSTOCK%5FBROWSE%5FLIST=4%7C2324%7C2385%7C9924%7C2103; _ga=GA1.2.1450427542.1511748749; _gid=GA1.2.614910067.1512647858; SCREEN_SIZE=WIDTH=1680&HEIGHT=1050',
               'dnt': '1',
               'origin': 'http://goodinfo.tw',
               'referer': url,
               'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}


    payload = {
        'STOCK_ID': ID,
        'STEP':  'DATA',
        'RPT_CAT': 'BS_M_QUAR'
        #'QRY_Time':'2020'
        }

    SHEETS = ['BS_M_QUAR', 'BS_M_YEAR']

    # columns = {'5Y': ['月別', '開盤', '收盤', '最高', '最低', '股價漲跌(元)', '股價漲跌(%)', '單月營收(億)', '單月月增(%)', '單月年增(%)',
    #                   '累計營收(億)', '累計月增(%)', '累計年增(%)', '合併營業收入單月月增(%)', '合併營業收入單月年增(%)',
    #                   '合併營業收入累計月增(%)', '合併營業收入累計年增(%)'],
    #
    #            '10Y': ['月別', '開盤', '收盤', '最高', '最低', '股價漲跌(元)', '股價漲跌(%)', '單月營收(億)', '單月月增(%)', '單月年增(%)',
    #                   '累計營收(億)', '累計月增(%)', '累計年增(%)', '合併營業收入單月月增(%)', '合併營業收入單月年增(%)',
    #                   '合併營業收入累計月增(%)', '合併營業收入累計年增(%)'],
    #            'ALL': ['月別', '開盤', '收盤', '最高', '最低', '股價漲跌(元)', '股價漲跌(%)', '單月營收(億)', '單月月增(%)', '單月年增(%)',
    #                   '累計營收(億)', '累計月增(%)', '累計年增(%)', '合併營業收入單月月增(%)', '合併營業收入單月年增(%)',
    #                   '合併營業收入累計月增(%)', '合併營業收入累計年增(%)']}

    HEADER = '''
    <!DOCTYPE html> 
    <html  lang="zh-Hant-TW">
	<head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
	</head>
	<body>
    '''
    FOOTER = '''
	</body>
    </html>
    '''

    for key in SHEETS:
        payload['RPT_CAT'] = key

        res = requests.post('https://goodinfo.tw/StockInfo/StockFinDetail.asp?', headers=headers, verify=False, data=payload)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text.replace('&nbsp;', '').replace('　', ''), 'html.parser')

        [s.find_parent('tr').extract() for s in
        soup.find_all('td', style=lambda value: value and 'color:blue' in value)[1:]]
        [s.find_parent('tr').extract() for s in
         soup.find_all('td', string="金額")]


        df = pd.read_html(str(soup))[1]

        df.iloc[1]=df.iloc[1].shift(-1)
        df.drop(columns=[15], inplace=True, axis=1)
        df=df.T
        df.columns = df.iloc[0]
        df.drop(0, inplace=True, axis=0)
        df.columns.name = ""
        df=df.reset_index(drop=True)

        #print (df)

        key = key.replace('/', '_')

        if not os.path.exists(ID):
            os.makedirs(ID)

        key = str(ID) + '/Balance_Sheet_' + key + '.html'
        df.to_html(str(key))
        with open(str(key), 'w', encoding='utf-8') as f:
            f.write(HEADER)
            f.write(df.to_html())
            f.write(FOOTER)

    return soup


def Get_Income_statement_code(ID):
    # Get the webpage's source html code
    source = 'https://goodinfo.tw/StockInfo/StockFinDetail.asp'
    url = source + ID
    print(url)

    # Header
    headers = {'accept': '*/*',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-CN;q=0.2',
               'content-length': '0',
               'content-type': 'application/x-www-form-urlencoded;',
               'cookie': '__gads=ID=fe562308f92c2979:T=1511748749:S=ALNI_MZDfWP11BwzcEPLDztntanza0Uc0Q; GOOD%5FINFO%5FSTOCK%5FBROWSE%5FLIST=4%7C2324%7C2385%7C9924%7C2103; _ga=GA1.2.1450427542.1511748749; _gid=GA1.2.614910067.1512647858; SCREEN_SIZE=WIDTH=1680&HEIGHT=1050',
               'dnt': '1',
               'origin': 'http://goodinfo.tw',
               'referer': url,
               'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}

    payload = {
        'STOCK_ID': ID,
        'STEP': 'DATA',
        'RPT_CAT': 'IS_M_QUAR'
        # 'QRY_Time':'2020'
    }

    SHEETS = ['IS_M_QUAR','IS_M_QUAR_ACC']


    HEADER = '''
    <!DOCTYPE html> 
    <html  lang="zh-Hant-TW">
	<head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
	</head>
	<body>
    '''
    FOOTER = '''
	</body>
    </html>
    '''

    for key in SHEETS:
        payload['RPT_CAT'] = key

        res = requests.post('https://goodinfo.tw/StockInfo/StockFinDetail.asp?', headers=headers, verify=False,
                            data=payload)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text.replace('&nbsp;', '').replace('　', ''), 'html.parser')

        [s.find_parent('tr').extract() for s in
         soup.find_all('td', style=lambda value: value and 'color:blue' in value)[1:]]
        [s.find_parent('tr').extract() for s in
         soup.find_all('td', string="金額")]

        df = pd.read_html(str(soup))[1]

        df.iloc[1] = df.iloc[1].shift(-1)
        df.drop(columns=[15], inplace=True, axis=1)
        df = df.T
        df.columns = df.iloc[0]
        df.drop(0, inplace=True, axis=0)
        df.columns.name = ""
        df = df.reset_index(drop=True)

        #print(df)

        key = key.replace('/', '_')

        if not os.path.exists(ID):
            os.makedirs(ID)

        key = str(ID) + '/Income_statement_' + key + '.html'
        df.to_html(str(key))
        with open(str(key), 'w', encoding='utf-8') as f:
            f.write(HEADER)
            f.write(df.to_html())
            f.write(FOOTER)

    return soup

make_forecast("2330")