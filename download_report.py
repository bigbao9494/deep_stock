#-*- coding: UTF-8 -*-
'''数据无效：目标买入日不是交易日：002001: 2017-04-01
数据无效：找不到同比增长数据：002001: 2017-03-31
数据无效：找不到同比增长数据：002001: 2017-03-30
数据无效：找不到同比增长数据：002001: 2017-03-29
'''
from datetime import date
from datetime import timedelta
import tushare as ts
import pickle
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import logging
from common import *

def main():
    year=2004
    season=1
    while year<2017:
        while season<5:
            try:#本地有记录
                with open('report_'+str(year)+'_'+str(season)+'.pkl', "rb") as f:
                    report = pickle.load(f)
            except:#本地没有存过
                with open('report_' + str(year) + '_' + str(season) + '.pkl', "wb") as f:
                    report_1st = ts.get_report_data(year, season)  # 获取业绩报表
                    report_2nd = ts.get_report_data(year, season)  # 获取业绩报表
                    report_3rd = ts.get_report_data(year, season)  # 获取业绩报表
                    #把三次合并:
                    report_all3 = pd.concat((report_1st,report_2nd,report_3rd),axis=0,join='outer')
                    #report_all3.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_all3.xlsx')
                    report =report_all3.drop_duplicates()
                    #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
                    pickle.dump(report, f)
            #report =report.drop_duplicates()
            #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
            engine = create_engine(
                        'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
            #存入数据库
            report.to_sql('report_'+str(year)+'_'+str(season),engine,if_exists='replace')
            season=season+1
        season =1
        year += 1
def download_1(year,season):
    #year=2014
    #season=1
    try:#本地有记录
        with open('report_'+str(year)+'_'+str(season)+'.pkl', "rb") as f:
            report = pickle.load(f)
    except:#本地没有存过
        with open('report_' + str(year) + '_' + str(season) + '.pkl', "wb") as f:
            report_1st = ts.get_report_data(year, season)  # 获取业绩报表
            report_2nd = ts.get_report_data(year, season)  # 获取业绩报表
            report_3rd = ts.get_report_data(year, season)  # 获取业绩报表
            #把三次合并:
            report_all3 = pd.concat((report_1st,report_2nd,report_3rd),axis=0,join='outer')
            #report_all3.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_all3.xlsx')
            report =report_all3.drop_duplicates()
            #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
            pickle.dump(report, f)
    #report =report.drop_duplicates()
    #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
    engine = create_engine(
            'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
    #存入数据库
    report.to_sql('report_'+str(year)+'_'+str(season),engine,if_exists='replace')
def re_download(year,season):
    '''重新下载一个季度的报表，在原来的基础上，补上缺少的部分
    :return:
    '''

    with open('report_'+str(year)+'_'+str(season)+'.pkl', "rb") as f:
        report = pickle.load(f)

    with open('report_' + str(year) + '_' + str(season) + '.pkl', "wb") as f:
        report_1st = ts.get_report_data(year, season)  # 获取业绩报表
        #把两次合并:
        report_all3 = pd.concat((report_1st,report),axis=0,join='outer')
        #report_all3.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_all3.xlsx')
        report =report_all3.drop_duplicates()
        #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
        pickle.dump(report, f)
    #report =report.drop_duplicates()
    #report.to_excel(r'D:\work_python\DeepData\tushare_download\201701_01_quchong.xlsx')
    engine = create_engine(
            'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
    #存入数据库
    report.to_sql('report_'+str(year)+'_'+str(season),engine,if_exists='replace')

if __name__ == '__main__':
    #main()

    #download_1(2016,3)

    #year=2016
    #season=3
    #re_download(year,season)

    year=2014
    season=1
    while year<2017:
        while season<5:
            re_download(year,season)
            season=season+1
        season =1
        year += 1