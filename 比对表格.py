# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 16:52:32 2019

@author: 无敌钢牙小白狼
"""

import os
import pandas as pd
import time
import numpy as np

def get_current_loc():
    current_py = os.path.realpath(__file__)
    current_loc = os.path.dirname(current_py)
    return current_loc

def read_name():
    today = time.strftime("%Y-%m-%d", time.localtime()) 
    df_risk_today = pd.read_excel('risk-'+today+'.xlsx',sheetname = '今收')
    df_risk_trading = pd.read_excel('risk-'+today+'.xlsx',sheetname = '成交')    
    df_risk_trading = df_risk_trading[df_risk_trading.委托状态 == '已成']
    df_today = pd.read_excel('综合信息查询_汇总证券.xls')
    df_trading = pd.read_excel('综合信息查询_成交回报.xls')
    return today,df_risk_today,df_risk_trading,df_today,df_trading

    

def new_file():
    save_path = 'save_path/'
    if not os.path.isdir(save_path):
        os.makedirs(save_path)
    return save_path
        
def handle_risk_today_data(df):
    dic_risk = {}
    temp_list = list(set(df['证券代码']))
    for i in temp_list:
        df_temp = df[df.证券代码 == i]
        df_temp_duo = df_temp[df_temp.持仓方向 == '多仓']
        df_temp_kong = df_temp[df_temp.持仓方向 == '空仓']
        dic_risk[i] = df_temp_duo['持仓量'].sum() - df_temp_kong['持仓量'].sum()
    df_risk = pd.DataFrame.from_dict(dic_risk,orient = 'index',columns = ['持仓量'])
    return df_risk

def handle_risk_trading_data(df):
    dic_risk_trading = {}
    temp_list = list(set(df['证券代码']))
    for i in temp_list:
        if i is np.nan:
            pass
        else:            
            df_temp = df[df.证券代码 == i]
            df_temp_kai = df_temp[df_temp.委托方向 == '开仓']
            df_temp_kai_duo = df_temp_kai[df_temp_kai.买卖方向 == '买入']
            df_temp_kai_kong = df_temp_kai[df_temp_kai.买卖方向 == '卖出']

            df_temp_ping = df_temp[df_temp.委托方向 == '平仓']
            df_temp_ping_duo = df_temp_ping[df_temp_ping.买卖方向 == '买入']
            df_temp_ping_kong = df_temp_ping[df_temp_ping.买卖方向 == '卖出']
            

            dic_risk_trading[i] = df_temp_kai_duo['委托数量'].sum() + df_temp_ping_kong['委托数量'].sum() \
            - df_temp_kai_kong['委托数量'].sum()-df_temp_ping_duo['委托数量'].sum()
    df_risk_trading = pd.DataFrame.from_dict(dic_risk_trading,orient = 'index',columns = ['持仓量'])
    return df_risk_trading 


def handle_today_data(df):
    dic_today = {}
    temp_list = list(set(df['证券代码']))
    for i in temp_list:
        if i is np.nan:
            pass
        else:            
            df_temp = df[df.证券代码 == i]
            df_temp_duo = df_temp[df_temp.持仓多空标志 == '多仓']
            df_temp_kong_1 = df_temp[(df_temp.持仓多空标志 == '空仓' )]
            df_temp_kong_2 = df_temp[(df_temp.持仓多空标志 == '义务仓' )]
            dic_today[i] = df_temp_duo['持仓数量'].sum() - df_temp_kong_1['持仓数量'].sum()-df_temp_kong_2['持仓数量'].sum()
    df_today = pd.DataFrame.from_dict(dic_today,orient = 'index',columns = ['持仓量'])
    return df_today 

def handle_trading_data(df):
    dic_trading = {}
    temp_list = list(set(df['证券代码']))
    for i in temp_list:
        if i is np.nan:
            pass
        else:            
            df_temp = df[df.证券代码 == i]
            df_temp_duo_1 = df_temp[df_temp.委托方向 == '买入开仓']
            df_temp_duo_2 = df_temp[df_temp.委托方向 == '卖出平仓']            
            df_temp_kong_1 = df_temp[(df_temp.委托方向 == '买入平仓' )]
            df_temp_kong_2 = df_temp[(df_temp.委托方向 == '卖出开仓' )]
            dic_trading[i] = df_temp_duo_1['成交数量'].sum() +df_temp_duo_2['成交数量'].sum() \
            - df_temp_kong_1['成交数量'].sum()-df_temp_kong_2['成交数量'].sum()
    df_trading = pd.DataFrame.from_dict(dic_trading,orient = 'index',columns = ['持仓量'])
    return df_trading 

def handle_data(df_risk_today,df_risk_trading,df_today,df_trading):
    df_risk_today = handle_risk_today_data(df_risk_today)
    df_risk_trading = handle_risk_trading_data(df_risk_trading)
    df_today = handle_today_data(df_today)
    df_trading = handle_trading_data(df_trading)
    return df_risk_today,df_risk_trading,df_today,df_trading

def find_diff(df_risk_today,df_today):
    df_temp = pd.concat([df_risk_today,df_today],axis=1)
    df_temp.columns = (['risk','today'])
    df_temp['diff'] = df_temp.risk - df_temp.today
    df_today_diff = df_temp[df_temp['diff'] != 0]
    return df_today_diff



#%%
if __name__ == '__main__':
    current_loc = get_current_loc()
    save_path = new_file()
    today,df_risk_today,df_risk_trading,df_today,df_trading = read_name()
    
    df_risk_today,df_risk_trading,df_today,df_trading  = handle_data(df_risk_today,\
                        df_risk_trading,df_today,df_trading)
    df_today_diff = find_diff(df_risk_today,df_today) 
    df_trading_diff = find_diff(df_risk_trading,df_trading)
    df_trading_diff.columns = ['risk','trading','diff']
    
    writer = pd.ExcelFile(save_path+'结果输出.xls')
    df_today_diff.to_excel(writer,sheet_name='今收')
    df_trading_diff.to_excel(writer,sheet_name='成交')
    writer.save()
    
    
    
    







    




