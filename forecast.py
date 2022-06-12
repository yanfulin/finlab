
import forecast_comparison
import Stock_Price_PER

## Set Date Formats
# today_string = datetime.datetime.today().strftime('%m%d%Y_%I%p')
# today_string2 = datetime.datetime.today().strftime('%b %d, %Y')

# Goodinfo provides 5years/10years/ALL monthly revenue

# def fbforecast(stock_id)
# filelist = ["5Y", "10Y", "ALL"]
#
# # Set Folder Targets for Revenue Info
# # Based on 5/10/All year revenue to do the coming 5 year forecast
# for file in filelist:
#     stock_id = "5534"
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
#     future1 = m1.make_future_dataframe(periods=60, freq='MS')
#     forecast1 = m1.predict(future1)
#     #print(forecast1.dtypes)
#     #print (forecast1[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
#     if not os.path.exists(stock_id):
#         os.makedirs(stock_id)
#     if not os.path.exists(f'{stock_id}/forecast/'):
#         os.makedirs(f'{stock_id}/forecast/')
#     forecast1.to_csv(f'{stock_id}/forecast/{file}.csv')

# forecast_5y=forecast_comparison.forecast(stock_id, 5)
# forecast_5y.to_csv(f'{stock_id}/forecast/forecast_actual_merged.csv')
# forecast_comparison.plot_forecast(forecast_5y)

def main():


    fin = open('StockCode', 'r+')
    StockCodeList = [str(i) for i in fin.read().splitlines()]
    fin.close()

    # Need to drop forecast table to avoid misalign of data (add new columns(stock_ids) into database
    forecast_comparison.drop_sql_table()
    for stock_id in StockCodeList:
        print ("ID=", stock_id)
        # goodinfo2.GetHtmlcode(stock_id)
        # goodinfo_revenue_monthly.GetHtmlcode(stock_id)
        # goodinfo_balance_sheet.Get_Balance_sheet_code(stock_id)
        # goodinfo_balance_sheet.Get_Income_statement_code(stock_id)
        # EPS_forecast.GetEPScode(stock_id)
        forecast_comparison.make_forecast(stock_id)
        forecast_10y=forecast_comparison.get_forecast(stock_id, 10)
        print("forecast==>", forecast_10y)

        #forecast_10y.to_csv(f'{stock_id}/forecast/forecast_actual_merged.csv')
        #forecast_comparison.plot_forecast(forecast_10y, stock_id)
        #time.sleep(10)
    Stock_Price_PER.export_to_excel2()


if __name__ == "__main__":
    main()