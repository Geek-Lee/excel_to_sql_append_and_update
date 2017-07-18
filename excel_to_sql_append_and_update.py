import pandas as pd
import sqlite3
from pandas import DataFrame
from sqlalchemy import create_engine

def df_to_sql_4(filefullpath, sheet, row_name):
    #读取处理文件夹中的excel
    excel_df = pd.read_excel(filefullpath, sheetname=sheet)
    excel_df = excel_df.dropna(how="all")
    #excel_df = excel_df.dropna(axis=1, how="all")
    excel_df[row_name] = excel_df[row_name].ffill()
    excel_df.index = range(len(excel_df))
    print(excel_df)

    #数据库的读取
    con = sqlite3.connect(r"C:\Users\K\Desktop\excel-upload-sqlite3\mins\db.sqlite3")
    sql = "SELECT * FROM fund_nav_data"
    sql_df = pd.read_sql(sql, con)
    name_list = sql_df['fund_name'].tolist()
    date_list = sql_df['statistic_date'].tolist()
    print("name_list")
    #print(type(name_list[0]))
    print(name_list)
    print("date_list")
    #print(type(date_list[0]))
    print(date_list)

    #从fund_info数据表中提取出fund_id，加入fund_nav_data数据表中的fund_id
    for fund_name in sql_df['fund_name'].unique():
        sql = "SELECT * FROM fund_info"
        fund_info_sql_df = pd.read_sql(sql, con)
        fund_id = fund_info_sql_df.loc[fund_info_sql_df.fund_name == fund_name, 'fund_id'].values[0]
        with con:
            cur = con.cursor()
            cur.execute("""UPDATE fund_nav_data SET fund_id=? WHERE fund_name=?""", (fund_id, fund_name))

    #对excel_df进行读取
    excel_name_list = excel_df['基金简称'].tolist()
    excel_name_list = list(set(excel_name_list))
    print("excel_name_list")
    #print(type(excel_name_list[0]))
    print(excel_name_list)

    for name in excel_name_list:
        statistic_date_series = excel_df.loc[excel_df['基金简称'] == name, '净值日期']
        excel_date_list = statistic_date_series.tolist()
        excel_date_list = [str(i) for i in excel_date_list]
        print("excel_date_list")
        #print(type(excel_date_list[0]))
        print(excel_date_list)
        for date in excel_date_list:
            if name in name_list and date in date_list:
                commit_data = excel_df[excel_df['基金简称'] == name]
                commit_data.columns = ["fund_name", "statistic_date", "nav", "added_nav", "total_share", "total_asset", "total_nav", "is_split", "is_open_date", "split_ratio", "after_tax_bonus"]
                commit_data["fund_id"] = str(fund_id)

                fund_name = name
                statistic_date = str(date)
                nav = str(commit_data.loc[commit_data.statistic_date == date, 'nav'].values[0])
                added_nav = str(commit_data.loc[commit_data.statistic_date == date, 'added_nav'].values[0])
                total_share = str(commit_data.loc[commit_data.statistic_date == date, 'total_share'].values[0])
                total_asset = str(commit_data.loc[commit_data.statistic_date == date, 'total_asset'].values[0])
                total_nav = str(commit_data.loc[commit_data.statistic_date == date, 'total_nav'].values[0])
                is_split = str(commit_data.loc[commit_data.statistic_date == date, 'is_split'].values[0])
                is_open_date = str(commit_data.loc[commit_data.statistic_date == date, 'is_open_date'].values[0])
                split_ratio = str(commit_data.loc[commit_data.statistic_date == date, 'split_ratio'].values[0])
                after_tax_bonus = str(commit_data.loc[commit_data.statistic_date == date, 'after_tax_bonus'].values[0])

                with con:
                    cur = con.cursor()
                    sql = """UPDATE fund_nav_data SET nav=?, added_nav=?, total_share=?, total_asset=?, total_nav=?, is_split=?, is_open_date=?, split_ratio=?, after_tax_bonus=? WHERE fund_name=? AND statistic_date=?"""
                    l = (nav, added_nav, total_share, total_asset, total_nav, is_split, is_open_date, split_ratio, after_tax_bonus, fund_name, statistic_date)
                    cur.execute(sql, l)
                print("if")
            else:
                commit_data = excel_df[(excel_df["基金简称"] == name)&(excel_df["净值日期"] == date)]
                commit_data.columns = ["fund_name", "statistic_date", "nav", "added_nav", "total_share", "total_asset", "total_nav", "is_split", "is_open_date", "split_ratio", "after_tax_bonus"]
                commit_data.to_sql("fund_nav_data", con, if_exists="append", index=False)
                print("else")

row_name = "基金简称"
filefullpath = r"C:\Users\K\Desktop\华泰大赛参赛私募基金数据填报模板.xlsx"
sheet = 4
df_to_sql_4(filefullpath, sheet, row_name)
