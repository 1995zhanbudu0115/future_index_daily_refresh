# coding: utf-8
# Author: Alvin Du
# Contact: 63265849@qq.com

import numpy as np
import pandas as pd
from WindPy import *
import datetime as dt
import os

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)


w.start()


today = dt.date.today()
month_ago = today - dt.timedelta(days=30)
trade_calendar_list = w.tdays(month_ago, today).Data[0]
yesterday_str = datetime.strftime(trade_calendar_list[-2], '%Y-%m-%d')

trade_date_num = str(today)[:4] + str(today)[5:7] + str(today)[8:10]


def get_future_min_data(windcode):
    total_min_data = pd.DataFrame()
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
    c = 0
    oi_data = pd.DataFrame()
    for contract in contract_df['wind_code'][:3]:
        print contract
        min_data = w.wsi(contract, 'CLOSE', yesterday_str + ' 21:00:00', str(today) + ' 15:00:00', 'FILL=PREVIOUS')
        last_position = w.wsd(contract, 'WINDCODE, OI', yesterday_str, yesterday_str)
        total_min_data['TIME'] = min_data.Times
        total_min_data['CONTRACT%s' % c] = [contract] * len(total_min_data)
        total_min_data['CONTRACT%s_CLOSE' % c] = min_data.Data[0]
        na_index = list(total_min_data['CONTRACT%s_CLOSE' % c].isna()).index(False)
        if na_index > 0:
            total_min_data['CONTRACT%s_CLOSE' % c] = total_min_data['CONTRACT%s_CLOSE' % c].\
                fillna(total_min_data['CONTRACT%s_CLOSE' % c][na_index])
        if last_position.Data[1][0] is None:
            total_min_data['CONTRACT%s_OI' % c] = [0] * len(total_min_data)
            oi_data[str(c)] = [0] * len(total_min_data)
        else:
            total_min_data['CONTRACT%s_OI' % c] = [last_position.Data[1][0]] * len(total_min_data)
            oi_data[str(c)] = [last_position.Data[1][0]] * len(total_min_data)
        c += 1
    total_min_data['TOTAL_OI'] = sum(np.array(oi_data).T)
    total_min_data['CLOSE'] = [0]*len(total_min_data)
    for position in list(total_min_data)[3:len(list(total_min_data))-1:3]:
        contract_num = position.split('_')[0]
        total_min_data[contract_num + '_WEIGHT'] = total_min_data[position]/total_min_data['TOTAL_OI']
        total_min_data['CLOSE'] += total_min_data[contract_num + '_WEIGHT'] * total_min_data[contract_num + '_CLOSE']
        del total_min_data[contract_num + '_WEIGHT'], total_min_data[contract_num + '_CLOSE']
    del total_min_data['TOTAL_OI']
    return total_min_data


def daily_instrument_index():
    for wind_code in ['V.DCE', 'FG.CZC', 'AU.SHF', 'AG.SHF', 'C.DCE', 'TA.CZC', 'MA.CZC', 'RU.SHF', 'BU.SHF', 'L.DCE',
                      'PP.DCE', 'J.DCE', 'JM.DCE', 'HC.SHF', 'I.DCE', 'RB.SHF', 'ZC.CZC', 'JD.DCE', 'CS.DCE', 'AP.CZC',
                      'SR.CZC', 'CF.CZC', 'CU.SHF', 'AL.SHF', 'ZN.SHF', 'NI.SHF', 'A.DCE', 'M.DCE', 'RM.CZC', 'P.DCE',
                      'OI.CZC', 'Y.DCE']:
        instrument_code = wind_code.split('.')[0]
        min_data = get_future_min_data(wind_code)
        if not os.path.exists('E:\my_files_du\my_data\index_by_underlying\%s' % instrument_code):
            os.mkdir('E:\my_files_du\my_data\index_by_underlying\%s' % instrument_code)
        min_data.to_csv('E:\my_files_du\my_data\index_by_underlying\%s\%s_%s.csv' % (instrument_code,
                                                                                     instrument_code, trade_date_num),
                        index=False)


if __name__ == '__main__':
    daily_instrument_index()
