# coding: utf-8
# Author: Alvin Du
# Project: 每日期货指数更新脚本
# Contact: 63265849@qq.com

import numpy as np
import pandas as pd
import os
from WindPy import *
import time
import m3_20181009_daily_instrument_index as m3
import datetime as dt

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)

w.start()

today = dt.date.today()
month_ago = today - dt.timedelta(days=30)
trade_calendar_list = w.tdays(month_ago, today).Data[0]
yesterday_str = datetime.strftime(trade_calendar_list[-2], '%Y-%m-%d')


def get_position(windcode):
    position_num = 0
    contract_data = w.wset("futurecc",
                           "startdate=%s; enddate=%s; wind_code=%s" % (today, today, windcode))
    contract_df = pd.DataFrame(data=contract_data.Data, index=contract_data.Fields)
    contract_df = contract_df.T
    del contract_df['sec_name'], contract_df['change_limit'], contract_df['target_margin']
    if windcode in ['FG.CZC', 'ZC.CZC', 'OI.CZC', 'CF.CZC', 'RM.CZC', 'SR.CZC', 'MA.CZC', 'TA.CZC', 'AP.CZC', 'A.DCE',
                    'JD.DCE', 'NI.SHF', 'I.DCE', 'J.DCE', 'JM.DCE', 'C.DCE', 'CS.DCE', 'L.DCE', 'PP.DCE', 'P.DCE',
                    'RU.SHF', 'Y.DCE', 'V.DCE', 'M.DCE']:
        contract_df = contract_df[contract_df['code'].str.contains('01') | contract_df['code'].str.contains('05') |
                                  contract_df['code'].str.contains('09')]
    elif windcode in ['RB.SHF', 'HC.SHF', 'AP.CZC']:
        contract_df = contract_df[contract_df['code'].str.contains('01') | contract_df['code'].str.contains('05') |
                                  contract_df['code'].str.contains('10')]
    elif windcode in ['AG.SHF', 'AU.SHF']:
        contract_df = contract_df[contract_df['code'].str.contains('06') | contract_df['code'].str.contains('12')]
    elif windcode in ['BU.SHF']:
        contract_df = contract_df[contract_df['code'].str.contains('02') | contract_df['code'].str.contains('06') |
                                  contract_df['code'].str.contains('09') | contract_df['code'].str.contains('12')]
    else:
        pass
    for contract in contract_df['wind_code'][:3]:
        position = w.wsd(contract, 'WINDCODE, OI', yesterday_str, yesterday_str)
        if position.Data[1][0] is None:
            pass
        else:
            position_num += position.Data[1][0]
    return position_num


def calculate_weight(index_name):
    index_info = pd.read_csv('E:\my_files_du\my_data\index_info.csv')
    weight_data = pd.DataFrame()
    index = list(index_info['index']).index(index_name)
    codes = index_info.ix[index_info['index'] == index_name]['codes'][index]
    for windcode in codes.split(','):
        print windcode
        code = str.lower(windcode.split('.')[0])
        weight_data['%s_volume' % code] = [get_position(windcode)]
    weight_data['total_volume'] = sum(np.array(weight_data)[0])
    for code_str in list(weight_data)[:-1]:
        code_str2 = str.lower(code_str.split('_')[0])
        weight_data['%s_weight' % code_str2] = weight_data[code_str] / weight_data['total_volume']
    return weight_data


def calculate_min_data(min_dir, index_dir):
    index_info = pd.read_csv('E:\my_files_du\my_data\index_info.csv')
    time_data = pd.read_csv(r'E:\my_files_du\my_data\time.csv')
    today_date_num = str(today)[:4] + str(today)[5:7] + str(today)[8:]
    time_data['TIME'] = time_data['TIME'].apply(lambda x: x[10:])
    time_data1 = time_data[:181]
    time_data1['TIME'] = time_data['TIME'].apply(lambda x: yesterday_str + x)
    time_data2 = time_data[181:]
    time_data2['TIME'] = time_data['TIME'].apply(lambda x: str(today) + x)
    dir_list = os.listdir(index_dir)
    for index in dir_list:
        c = 1
        today_data = time_data1.append(time_data2)
        today_data['TRADING_DAY'] = today_date_num
        index_name = index.split('.')[0]
        weight_data = calculate_weight(index_name)
        c_index = list(index_info['index']).index(index_name)
        codes = index_info.ix[index_info['index'] == index_name]['codes'][c_index]
        for windcode in codes.split(','):
            code = str.lower(windcode.split('.')[0])
            min_data = pd.read_csv('E:\my_files_du\my_data\index_by_underlying\%s\%s_%s.csv' % (code, code,
                                                                                                    today_date_num))
            min_data['PRE_CLOSE'] = min_data['CLOSE'].shift(1)
            min_data['ret'] = (min_data['CLOSE'] - min_data['PRE_CLOSE']) / min_data['CLOSE']
            min_data.fillna(0, inplace=True)
            min_dict = min_data.set_index('TIME').T.to_dict()
            rt_list = []
            for time_str in today_data['TIME']:
                try:
                    rt = min_dict[time_str]['ret']
                    rt_list.append(rt)
                except:
                    rt = 0
                    rt_list.append(rt)
            today_data.insert(c, '%s_volume' % code, len(today_data) * [weight_data['%s_volume' % code][0]])
            today_data.insert(c+1, '%s_ret' % code, rt_list)
            today_data.insert(c+2, '%s_weight' % code, len(today_data)*[weight_data['%s_weight' % code][0]])
            c += 3
        today_data['total_volume'] = [weight_data['total_volume'][0]] * len(today_data)
        today_data['hour'] = today_data['TIME'].apply(lambda x: int(x[10:13] + x[14:16]))
        today_data['equity_change'] = [0] * len(today_data)
        start = time.time()
        pre_index = pd.read_csv(index_dir + '\%s' % index)
        history_data = pd.read_csv(u'Z:\Personal\李越嘉\history_industry_index\%s' % index)
        last_price = list(pre_index['equity'])[-1]
        for columns in list(today_data)[2:len(list(today_data)) - 3:3]:
            today_data['equity_change'] += today_data[columns] * today_data[
                list(today_data)[list(today_data).index(columns) + 1]]
        index_price_list = [last_price]
        for i in range(len(today_data)):
            trade_hour = today_data['hour'][i]
            percentage = today_data['equity_change'][i]
            if index_name in ['CIFI', 'APFI', 'CRFI', 'NMBM']:
                if trade_hour >= 2059 or trade_hour < 859:
                    index_price = index_price_list[-1]
                    index_price_list.append(index_price)
                else:
                    index_price = (1 + percentage) * index_price_list[-1]
                    index_price_list.append(index_price)
            elif index_name in ['JJRI']:
                if trade_hour > 2300 or trade_hour < 859:
                    index_price = index_price_list[-1]
                    index_price_list.append(index_price)
                else:
                    index_price = (1 + percentage) * index_price_list[-1]
                    index_price_list.append(index_price)
            else:
                index_price = (1 + percentage) * index_price_list[-1]
                index_price_list.append(index_price)
        today_data['equity'] = index_price_list[1:]
        del today_data['equity_change']
        for del_columns in list(today_data)[3:len(list(today_data)) - 3:3]:
            del today_data[del_columns]
        del today_data['hour']
        col_list = list(today_data)
        col_list.pop(col_list.index('TRADING_DAY'))
        col_list.append('TRADING_DAY')
        today_data = today_data[col_list]
        pre_index['TIME'] = pd.to_datetime(pre_index['TIME'])
        pre_index.set_index(['TIME'], inplace=True)
        this_year_data = pre_index['2018']
        this_year_data['TIME'] = this_year_data.index
        t_list = list(this_year_data)
        t_list.pop(t_list.index('TIME'))
        t_list.insert(0, 'TIME')
        this_year_data = this_year_data[t_list]
        this_year_data = this_year_data.append(today_data)
        this_year_data.to_csv(index_dir + '/%s' % index, index=False)
        history_data = history_data.append(today_data)
        history_data.to_csv(u'Z:\Personal\李越嘉\history_industry_index\%s' % index, index=False)
        print index_name + ' completed'


if __name__ == '__main__':
    # m3.daily_instrument_index()
    calculate_min_data(r'E:\my_files_du\my_data\index_by_underlying', r'E:\my_files_du\my_data\total_industry_index')