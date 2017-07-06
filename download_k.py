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
CHECK_DATABASE_PRINT = False
pymysql_config = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': DATABASS_USER_NAME,
        'password': DATABASS_PASSWORD,
        'db': DATABASS_NAME,
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
conn = pymysql.connect(**pymysql_config)

def check_already_downloaded(conn,code,year,day_or_week):
    '''
    :param code: string
    :param year: int or str
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
def download_all_stock():
    '''下载所有股票（不含指数）的日线与周线
    :return:
    '''
    #with open('download_all_stock_k.pkl', "wb") as pkf:
        #pickle.dump(1721, pkf)
    # zxb_tickers = ts.get_sme_classified()#下载中小板的
    codes = ts.get_stock_basics()  # 下载全部股票
    #engine = create_engine('mysql+pymysql://root:m7227510@127.0.0.1/tushare?charset=utf8')
    engine = create_engine('mysql+pymysql://'+DATABASS_USER_NAME+':'+DATABASS_PASSWORD+'@127.0.0.1/tushare?charset=utf8')
    try:  # 载入进度
        with open('download_all_stock_k.pkl', "rb") as pkf:
            i_code = pickle.load(pkf)
    except:
        i_code = 0
    try:
        for i in range(i_code, codes.index.size):
            one_ticker = codes.index[i]
            year = 2017
            print("process: %s:%s" % (codes.index.size, i))
            while year > 2004:
                # 日线
                if (not check_already_downloaded(conn, one_ticker, year, "day")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download day code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="D",
                                           autype="qfq")
                    if (k_data.empty):
                        break
                    k_data.to_sql(one_ticker + '_day_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
                # 周线
                if (not check_already_downloaded(conn, one_ticker, year, "week")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download week code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                           autype="qfq")
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded week code: %s year:%s" % (one_ticker, year))
                if (not CHECK_DATABASE_PRINT):
                    print("finished code: %s year:%s" % (one_ticker, year))
                year -= 1
    except Exception as ex:
        print("Need debug. code: %s year:%s" % (one_ticker, year))
        with open('download_all_stock_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
    except KeyboardInterrupt as ex:
        with open('download_all_stock_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
def download_all_index():
    '''
    下载所有指数的日线与周线
    :return:
    '''
    # with open('download_k.pkl', "wb") as pkf:
    # pickle.dump(949, pkf)
    codes = ts.get_index()  # 下载全部指数列表
    engine = create_engine(
        'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
    try:  # 载入进度
        with open('download_all_index.pkl', "rb") as pkf:
            i_code = pickle.load(pkf)
    except:
        i_code = 0
    try:
        for i in range(i_code, codes.index.size):
            one_ticker = codes.iloc[i,0]
            year = 2017
            print("process: %s:%s" % (codes.index.size, i))
            while year > 2004:
                # 日线
                if (not check_already_downloaded(conn, one_ticker, year, "day")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download day code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="D",
                                           autype="qfq")
                    if (k_data.empty):
                        break
                    k_data.to_sql(one_ticker + '_day_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
                # 周线
                if (not check_already_downloaded(conn, one_ticker, year, "week")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download week code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                           autype="qfq")
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded week code: %s year:%s" % (one_ticker, year))
                year -= 1
                if (not CHECK_DATABASE_PRINT):
                    print("finished code: %s year:%s" % (one_ticker, year))
    except Exception as ex:
        print("Need debug. code: %s year:%s" % (one_ticker, year))
        with open('download_all_index.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
    except KeyboardInterrupt as ex:
        with open('download_all_index.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
def main():
    '''
    把中小板的股票历年日线、周线全下载，存入mysql中。
    如果用control+c打断，可以记录下载进度，下次继续下载。
    数据库中已经有的全年记录，不会重复下载。
    最早到下载到2005年
    :return:
    '''
    #with open('download_k.pkl', "wb") as pkf:
        #pickle.dump(949, pkf)
    #zxb_tickers = ts.get_sme_classified()#下载中小板的
    codes = ts.get_stock_basics()#下载全部股票
    engine = create_engine(
        'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
    try:#载入进度
        with open('download_k.pkl',"rb") as pkf:
            i_code = pickle.load(pkf)
    except:
        i_code=0
    try:
        for i in range(i_code,codes.index.size):
            one_ticker = codes.index[i]
            year = 2017
            print("process: %s:%s" %(codes.index.size,i))
            while year > 2004:
                #日线
                if(not check_already_downloaded(conn,one_ticker,year,"day")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download day code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker,start=str(year)+"-01-01",end=str(year)+"-12-31",ktype="D",autype="qfq")
                    if(k_data.empty):
                        break
                    k_data.to_sql(one_ticker+'_day_' + str(year) , engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
                #周线
                if (not check_already_downloaded(conn, one_ticker, year, "week")):
                    if (CHECK_DATABASE_PRINT):
                        print("Will download week code: %s year:%s" % (one_ticker, year))
                    k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                           autype="qfq")
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if (CHECK_DATABASE_PRINT):
                        print("downloaded week code: %s year:%s" % (one_ticker, year))
                year-=1
                if (not CHECK_DATABASE_PRINT):
                    print("finished code: %s year:%s" %(one_ticker,year))
    except Exception as ex:
        print("Need debug. code: %s year:%s" %(one_ticker,year))
        with open('download_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)
    except KeyboardInterrupt as ex:
        with open('download_k.pkl', "wb") as pkf:
            pickle.dump(i, pkf)

def download_oneyear_stock(year):
    '''单单下载某一年的全部股票的日线和周线数据
    :param year: int or string
    :return:
    '''
    zxb_tickers = ts.get_sme_classified()
    engine = create_engine(
        'mysql+pymysql://' + DATABASS_USER_NAME + ':' + DATABASS_PASSWORD + '@127.0.0.1/'+DATABASS_NAME+'?charset=utf8')
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
                    if(CHECK_DATABASE_PRINT):
                        print("downloaded day code: %s year:%s" % (one_ticker, year))
            # 周线
            if (not check_already_downloaded(conn, one_ticker, year, "week")):
                k_data = ts.get_k_data(one_ticker, start=str(year) + "-01-01", end=str(year) + "-12-31", ktype="W",
                                       autype="qfq")
                if (not k_data.empty):
                    k_data.to_sql(one_ticker + '_week_' + str(year), engine, if_exists='replace')
                    if(CHECK_DATABASE_PRINT):
                        print("downloaded week code: %s year:%s" %(one_ticker,year))
            if(not CHECK_DATABASE_PRINT):
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
    #main()
    #windows下多进程运行：mpiexec -n 14 python download_k.py
    #comm = MPI.COMM_WORLD
    #comm_rank = comm.Get_rank()  # my rank()my position)
    #comm_size = comm.Get_size()  # number of total work items
    #print("process: download year %s" %(2004+comm_rank))
    #download_oneyear(2004+comm_rank)

    #download_oneyear(sys.argv[1])

    #download_oneyear(2007)

    #download_all_index()
    if(sys.argv[1]=="ai"):
        download_all_index()
    elif(sys.argv[1]=="as"):
        download_all_stock()
    elif(sys.argv[1][0:2] == "20"):
        download_oneyear_stock(sys.argv[1])
    else:
        print("Parameters:")
        print("Download all index: 'python download_k.py ai'")
        print("Download all stock: 'python download_k.py as'")
        print("Download all stock of specific year : 'python download_k.py 2017' , you can change 2017 to any year,(not earlier than 2000)")
