#-*- coding: UTF-8 -*-
from tushare_download.common import *
import pymysql
import numpy as np
from download_report import *
from datetime import date
class get_report():
    '''取出指定股票、指定season的数据
    '''
    def __init__(self, conn, code="",year="", season=""):
        self.code = code
        self.year = year
        self.season = season #
        self.conn = conn
        self.result = None #数据库中取出的原始数据。key是field的名字，value是值。None补0.
        self.nm_result=None
        self.data_valid=True
        self.report_date=None
    def get_report(self):
        pass
class get_report_db(get_report):
    '''从数据库中取数据
    '''
    def _get_data_from_db(self,fields):
        self.result={}
        query =  "SELECT * from %s" %('report_'+self.year+'_'+self.season) +\
                 " WHERE code = '%s'" %self.code
        try:
            result = db_execute(query,self.conn)
            if(result is None):
                self.data_valid=False
                #result={}
                #for field in fields:
                    #result[field]= 0.0

        except:
            self.data_valid=False
            #result={}
            #for field in fields:
                #result[field]= 0.0
        if(self.data_valid):#数据有效
            for field in fields:
                self.result[field]=result[field]
            #记录报告日期
            if(self.season==4):#如果是4季报，发布年份是下一年
                report_year=int(self.year)+1
            else:
                report_year=int(self.year)
            report_date = date(report_year,int(result['report_date'][0:2]),int(result['report_date'][3:5]))
        else:
            self.result=None

        return self.result # 成功了，result是真实数；失败了，self.result是None

    def get_data_origin(self,*fields):
        if((self.result is None) and (self.data_valid)):
            self._get_data_from_db(fields)
        return self.result #成功了是数据，失败了self.result是None
    def get_data_normalized(self,*fields):
        if((self.result is None) and (self.data_valid)):#还没下载
            self._get_data_from_db(fields)
        elif(not self.data_valid):#下载失败
            return None

        self.nm_input =[]
        for one_data in self.result:
            if self.result[one_data] is None:
                self.nm_input.append(0.0)
            else:
                self.nm_input.append(float(self.result[one_data]))
        self.nm_result = normalize(np.array(self.nm_input))
        return self.nm_result
pymysql_config = {
          'host':'127.0.0.1',
          'port':3306,
          'user':'root',
          'password':'m7227510',
          'db':'tushare',
          'charset':'utf8mb4',
          'cursorclass':pymysql.cursors.DictCursor,
          }
def test():
    conn = pymysql.connect(**pymysql_config)
    '''year=2017
    season=1
    rp = get_report_db(conn,code='002001',year="2017",season="1")
    result = rp.get_data_normalized("eps_yoy","roe","profits_yoy")
    print(result)'''

    year=2009
    season=1
    while year<2017:
        while season<5:
            print("year:%s_season:%s" %(year,season))
            rp = get_report_db(conn,code='002001',year=str(year),season=str(season))
            result=None
            result = rp.get_data_normalized("eps_yoy","roe","profits_yoy")
            while(result is None):#如果没成功
                re_download(year,season)
                result = rp.get_data_normalized("eps_yoy","roe","profits_yoy")
            print(result)
            season=season+1
        season =1
        year += 1
if __name__ == '__main__':
    main()