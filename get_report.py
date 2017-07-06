#-*- coding: UTF-8 -*-
from common import *
import pymysql
import numpy as np
from download_report import *
from datetime import date
class get_report():
    '''取出指定股票、指定season的数据
    '''
    def __init__(self, conn, code="",year=1999, season=1):
        self.code = code
        self.year = year #int
        self.season = season #int
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
        query =  "SELECT * from %s" %('report_'+str(self.year)+'_'+str(self.season)) +\
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
                if(result[field] is None):
                    self.result[field] = 0.0
                else:
                    self.result[field]=result[field]
            #记录报告日期
            if(self.season==4):#如果是4季报，发布年份是下一年
                report_year=self.year+1
            else:
                report_year=self.year
            self.report_date = date(report_year,int(result['report_date'][0:2]),int(result['report_date'][3:5]))
        else:
            self.result=None

        return self.result # 成功了，result是真实数；失败了，self.result是None

    def get_data_origin(self,*fields):
        #if((self.result is None) and (self.data_valid)):
        self._get_data_from_db(fields)
        return self.result #成功了是数据，失败了self.result是None
    def get_data_normalized(self,*fields):
        if((self.result is None) and (self.data_valid)):#还没下载
            self._get_data_from_db(fields)
        if(not self.data_valid):#下载失败
            return None

        self.nm_input =[]
        for one_data in self.result:
            if self.result[one_data] is None:
                self.nm_input.append(0.0)
            else:
                self.nm_input.append(float(self.result[one_data]))
        self.nm_result = normalize(np.array(self.nm_input))
        return self.nm_result

class get_report_db_by_date(get_report_db):
    def __init__(self,cnn,code,date):
        self.date = date
        year,season = get_season_int(date)
        super(get_report_db_by_date,self).__init__(cnn,code,year,season)
    def get_data_by_date(self,*fields):
        self.get_data_origin(*fields)
        if(self.data_valid): #数据有效
            if(self.report_date<self.date):#发布日期早于date
                return self.result
            else:#发布日期晚于date，则应该取上一个季度的季报
                self.season = self.season-1
                if(self.season==0):
                    self.season = 4
                    self.year -= 1
                self.get_data_origin(*fields)
                if(self.data_valid):
                    return self.result
                else:
                    return None
        else:
            return None
    def get_data_by_date_normalized(self,*fields):
        self.get_data_by_date(*fields)
        if(self.data_valid):
            self.nm_input =[]
            for one_data in self.result:
                self.nm_input.append(float(self.result[one_data]))
            self.nm_result = normalize(np.array(self.nm_input))
            return self.nm_result
        else:
            return None

pymysql_config = {
          'host':'127.0.0.1',
          'port':3306,
          'user':DATABASS_USER_NAME,
          'password':DATABASS_PASSWORD,
          'db': DATABASS_NAME,
          'charset':'utf8mb4',
          'cursorclass':pymysql.cursors.DictCursor,
          }
def test_get_report_db():
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
            rp = get_report_db(conn,code='002001',year=year,season=season)
            result=None
            result = rp.get_data_normalized("eps_yoy","roe","profits_yoy")
            while(result is None):#如果没成功
                re_download(year,season)
                result = rp.get_data_normalized("eps_yoy","roe","profits_yoy")
            print(result)
            season=season+1
        season =1
        year += 1

def test_get_report_db_by_date():
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    conn = pymysql.connect(**pymysql_config)
    zxb_tickers = ts.get_sme_classified()
    #one_dateee = date(2005,month=3,day=7)
    for i in range(zxb_tickers.index.size):
        one_dateee = date(2005,month=4,day=30)
        one_ticker = zxb_tickers.iloc[i,0]
        for j in range(3650):
            rp_obj = get_report_db_by_date(conn,one_ticker,one_dateee)
            report = rp_obj.get_data_by_date_normalized("eps_yoy","roe","profits_yoy")
            if(rp_obj.data_valid):
                print('valid: '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(report))
            else:
                logger.debug('invalid:  '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(report))
            one_dateee = one_dateee+timedelta(1)
    pass
if __name__ == '__main__':
    test_get_report_db_by_date()