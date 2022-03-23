import pandas as pd


def get_stock_db_list():
    #root_dir = os.path.join(os.getcwd(), '..')
    #sys.path.append(root_dir)
    rev=pd.read_csv("../../data/01_raw/monthly_revenue_2022-03-22.csv")
    stock_list = list(rev.columns[1:])

    #Check whether all the favorite stocks are in the list!
    favorite_stock_list = ['2330', '2454','1526', '2347', '2303']
    ETF=['00733']

    check = all(item in stock_list for item in favorite_stock_list)
    if check is True:
        print("The list {} contains all elements of the list {}".format("stock_list", "favorite_stock_list"))
    else:
        print("No, List1 doesn't have all elements of the List2.")

    stock_db_list = sorted(list(set().union(favorite_stock_list, stock_list)))

    return stock_db_list

print("stock_db_list ==>")
print(get_stock_db_list())