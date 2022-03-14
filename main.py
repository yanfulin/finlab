# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from finlab import data
#from finlab.backtest import sim
import finlab
import talib
finlab.login('75b0ztI1TfihNQAeO0UyBJVYckRmF9Rr10E3LhE4JlIDY1uh8NLgUJBCXafSgsJf#free')
# 取得股價淨值比
rev = data.get("monthly_revenue:當月營收")
print(rev.loc['2018-M01':, ['2317', '2330', '3008']])
rrsi=data.indicator('RSI', timeperiod=14)
print(rrsi)

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
