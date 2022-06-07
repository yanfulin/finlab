
from finlab import data
#from finlab.backtest import sim
import finlab
import talib
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import *
import sqlite3
import pymysql
import os, sys
import datetime
import logging
from pathlib import Path
import os
from dotenv import load_dotenv

def configure():
    dotenv_path = Path(Path.home(),'PycharmProjects/finlab/conf/.env')
    # print(Path.cwd(),Path.home())
    # print(dotenv_path)
    # print(os.path.expanduser("~/PycharmProjects"))
    load_dotenv(dotenv_path)
    # finlab_api=os.getenv("finlab_key")
    # print("finlab_api", finlab_api)

def get_data_from_finlab(finlab_data_dict):
    # from conf import secrets
    # /todo move the secret key to conf
    configure()
    finlab.login(os.getenv("finlab_key"))

    for k,v in finlab_data_dict.items():
        date_today = datetime.date.today()
        data_folder = Path.cwd().parent.parent/"data"/"01_raw"/"{}".format(date_today)
        print(data_folder)
        if not os.path.isdir(data_folder):
            os.makedirs(data_folder)
        #print(data_folder)
        fin_df=data.get(k)
        fin_df.to_csv("{}/{}.csv".format(data_folder, v),index=false)
        logging.info('{}____{} is saved to csv'.format(k,v))
    logging.info("All data are saved to CSV")
    return

def save_finlab_data_to_sqlite(finlab_data_dict):
    for k,v in finlab_data_dict.items():
        date_today = datetime.date.today()
        data_folder = Path.cwd().parent.parent /"data"/"01_raw"/"{}".format(date_today)
        fin_df=pd.read_csv("{}/{}.csv".format(data_folder, v))

        db = os.path.join("../../", 'fin_db')
        print(db)
        conn = sqlite3.connect(db)
        # 將新建的DataFrame儲存為MySQL中的資料表，不儲存index列
        fin_df.to_sql(v, conn, if_exists='replace')
        logging.info("Write {} to SQL successfully!".format(k))
    logging.info("All data are saved to SQL database")
    return


# root_dir = os.path.join(os.getcwd(), '..')
# sys.path.append(root_dir)
# print(root_dir)
finlab_data_dict={"monthly_revenue:當月營收":"monthly_revenue", 'dividend_announcement':'dividend_announcement'}
get_data_from_finlab(finlab_data_dict)
save_finlab_data_to_sqlite(finlab_data_dict)