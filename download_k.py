#-*- coding: UTF-8 -*-
#下载历年的k线
from datetime import date
from datetime import timedelta
import tushare as ts
import pickle
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import logging
from common import *
import pymysql
import mpi4py.MPI as MPI
import sys
CHECK_DATABASE = True
pymysql_config = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': 'm7227510',
        'db': 'tushare',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
conn = pymysql.connect(**pymysql_config)

def download_day_k():
    #engine = create_engine('mysql+pymysql://root:m7227510@127.0.0.1/tushare?charset=utf8')
    pass
def download_week_k():
    pass
def check_already_downloaded(conn,code,year,day_or_week):
    '''
    :param code: string
    :param year: int
    :param day_or_week: string, "day" or "week"
    :return:
    '''
    query = "SELECT * from %s" % (code + "_"+day_or_week+"_" + str(year)) + \
            " LIMIT 1"
    try:
        result = db_execute(query, conn)
        if (result is None):
            return False
        else:
            return True
    except:
        return False


def main():
    '''
    把中小板的股票历年日线、周线全下载，存入mysql中。
    如果用control+c打断，可以记录下载进度，下次继续下载。
    数据库中已经有的全年记录，不会重复下载。
    最早到下载到2005年
    :return:
    '''
    with open('download_k.pkl', "wb") as pkf:
        pickle.dump(26, pkf)
    zxb_tickers = ts.get_sme_classified()
    engine = create_engine('mysql+pymysql://root:m7227510@127.0.0.1/tushare?charset=utf8')
    try:#载入进度
        with open('download_k.pkl',"rb") as pkf:
            i_code = pickle.load(pkf)
    except:
        i_code=0
    try:
        for i in range(i_code,zxb_tickers.index.size):
            one_ticker = zxb_tickers.iloc[i, 0]
            year = 2017
            while year > 2004:
                #日线
                if(not check_already_downloaded(conn,one_ticker,year,"day")):
                    k_data = ts.get_k_data(one_ticker,start=str(year)+"-01-01",end=str(year)+"-12-31",ktype="D",autype="qfq")
                    if(k_data.empty):
                        break
                    k_data.to_sql(one_ticker+'_day_' + str(year) , engine, if_exists='replace')
                    if (CHECK_DATABASE):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
                #周线
                if (not check_already_downloaded(conn, one_ticker, year, "week  ")):
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                           autype="qfq")
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE):
                        print("downloaded week code: %s year:%s" % (one_ticker, year))
                year-=1
                if (not CHECK_DATABASE):
                    print("finished code: %s year:%s" %(one_ticker,year))
    except Exception as ex:
        print("Need debug. code: %s year:%s" %(one_ticker,year))
    except KeyboardInterrupt as ex:
        with open('download_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)

def download_oneyear(year):
    zxb_tickers = ts.get_sme_classified()
    engine = create_engine('mysql+pymysql://root:m7227510@127.0.0.1/tushare?charset=utf8')
    try:#载入进度
        with open(str(year)+'download_k.pkl',"rb") as pkf:
            i_code = pickle.load(pkf)
    except:
        i_code=0
    try:
        for i in range(i_code,zxb_tickers.index.size):
            one_ticker = zxb_tickers.iloc[i, 0]
            # 日线
            if (not check_already_downloaded(conn, one_ticker, year, "day")):
                k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="D",
                                       autype="qfq")
                if (not k_data.empty):
                    k_data.to_sql(one_ticker + '_day_' + str(year), engine, if_exists='replace')
                    if(CHECK_DATABASE):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
            # 周线
            if (not check_already_downloaded(conn, one_ticker, year, "week")):
                k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                       autype="qfq")
                if (not k_data.empty):
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if(CHECK_DATABASE):
                        print("downloaded week code: %s year:%s" %(one_ticker,year))
            if(not CHECK_DATABASE):
                print("downloaded code: %s year:%s" % (one_ticker, year))
    except Exception as ex:
        print("Need debug. code: %s year:%s" %(one_ticker,year))
        with open(str(year)+'download_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
        print("year %s process saved." %year)
    except KeyboardInterrupt as ex:
        with open(str(year)+'download_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
        print("year %s process saved." %year)


if __name__=="__main__":
    main()
    #windows下多进程运行：mpiexec -n 14 python download_k.py
    #comm = MPI.COMM_WORLD
    #comm_rank = comm.Get_rank()  # my rank()my position)
    #comm_size = comm.Get_size()  # number of total work items
    #print("process: download year %s" %(2004+comm_rank))
    #download_oneyear(2004+comm_rank)

    #download_oneyear(sys.argv[1])

    #download_oneyear(2007)

