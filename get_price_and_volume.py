#-*- coding: UTF-8 -*-
#class price_and_volume_ts():获取指定日期、编号的股票的输入向量。具有以下特点：
#1. 如果指定日期不是交易日，return  None
#2. 返回的价格和均线都是指定日期的close价格
#已知问题：
#1. 中小板指数从2007年09月04日再往前就没有了。可能是tushare的数据源没有那之前的数据

#调试记录：

from datetime import date
from datetime import timedelta
import tushare as ts
import pickle
import numpy as np
import logging
from common import *
import pymysql
#输入指定股票的指定日期，得到用来训练的vector（numpy类型）
class price_and_volume_ts():
    def __init__(self, code,date,index=False):
        self.code = code
        self.test_date = date
        self.buy_date_str = date_to_str(self.test_date)
        self.index = index

        #最终输出的结果
        self.price={} #np类型
        self.volume=None #np类型
        self.nm_price = None
        self.nm_volume=None

        #指示数据有效性的
        self.data_valid = True
        self.no_week_ma = False #没有日线的20日均线，说明再往前的日子数据都不用找了.
    def get_normalized_price_and_volume(self):
        '''
        获得归一化的price和volume。
        如果已经下载过，这里不再下载，沿用之前的数据；如果没下载过，先下载，再归一化。
        :return: numpy array：[当日价格，5日均价，10日均价，20日均价，5周均价，10周均价，20周均价，当日成交，5日均量，10日均量，20日均量，5周均量，10周均量，20周均量]
        '''
        if(self.data_valid==True and (self.price is None)):#没下载过
            self.get_price_and_volume()
        #将以上数据归一化
        if(self.data_valid):
            self.nm_price = normalize(np.array(self.price))
            self.nm_volume = normalize(np.array(self.volume))
            return np.append(self.nm_volume,self.nm_volume)
        if(self.data_valid is False):
            return None
    def get_price_and_volume(self):
        '''
        return: numpy array：[当日价格，5日均价，10日均价，20日均价，5周均价，10周均价，20周均价，当日成交，5日均量，10日均量，20日均量，5周均量，10周均量，20周均量]
        '''
        #取日线行情:
        tmp_price, tmp_volume = self._download_data(self.code,index=self.index)#不成功返回None,None
        if(self.data_valid is False):
            return None
        self.price = np.array(tmp_price)
        self.volume = np.array(tmp_volume)
        #整合以上数据
        return np.append(self.price,self.volume)

    def _check_data(self,data):
        for d in data:
            if d<1.0:
                return False # 异常
        return True # 正常

    def _download_data(self,code,index):
        start_day_date = date_to_str(self.test_date-timedelta(40))
        start_week_date = date_to_str(self.test_date-timedelta(25)*7)
        #取日线行情:
        data_day = ts.get_k_data(code, ktype='d', autype='qfq',index=index,start=start_day_date, end=self.buy_date_str)
        #检查够不够20个交易日
        if(data_day.index.size<20):
            data_day = ts.get_k_data(code, ktype='d', autype='qfq',index=index,start=date_to_str(self.test_date-timedelta(180)), end=self.buy_date_str)#考虑有停牌的情况，祖国不够20个，再往前追加3个月
        #检查是否仍然空。如果还空，要么停牌超过半年，要么服务器出错。
        if(data_day.empty):
            print("数据无效：停牌超过半年，或者服务器出错：" + code + ": " + self.buy_date_str)
            self.data_valid = False
            return None, None
        # 检查buydate是否存在
        if (data_day.iloc[-1, 0] != self.buy_date_str):
            print("数据无效：目标买入日不是交易日：" + code + ": " + self.buy_date_str)
            self.data_valid = False
            return None, None
        if(data_day.index.size<20):
            print("数据无效：股票上市日子不够，计算不出日MA20："+code+": "+self.buy_date_str)
            self.no_week_ma = True
            self.data_valid = False
            return None,None
        #取周线行情:
        data_week= ts.get_k_data(code, ktype='w', autype='qfq',index=index,start=start_week_date, end=self.buy_date_str)
        #检查够不够20个交易日
        if(data_week.index.size<20):
            data_week= ts.get_k_data(code, ktype='w', autype='qfq',index=index,start=date_to_str(self.test_date-timedelta(50)*7), end=self.buy_date_str)
        if(data_week.index.size<20):
            print("数据无效：股票上市日子不够，计算不出周MA20："+code+": "+self.buy_date_str)
            self.no_week_ma = True
            self.data_valid = False
            return None,None
        #当日price
        price = [data_day.iloc[-1,2]]
        #日均值price
        price += compute_ma(data_day['close'])
        #周均值price
        price += compute_ma(data_week['close'])
        if(self._check_data(price) is False):
            print("数据无效：股价太小"+code+": "+self.buy_date_str)
            self.data_valid = False
            return None,None
        #当日volume
        volume = [data_day.iloc[-1,5]]
        #日均值volume
        volume += compute_ma(data_day['volume'])
        #周均值volume
        volume += compute_ma(data_week['volume'])
        if(self._check_data(volume) is False):
            print("数据无效：成交量太小"+code+": "+self.buy_date_str)
            self.data_valid = False
            return None,None
        return price,volume

#class price_and_volume_db:
#返回指定日期的价格。如果指定的日期不是交易日，返回{}或[]。
class price_and_volume_db():
    def __init__(self,conn,code,date,index=False):
        self.code = code
        self.test_date = date
        self.buy_date_str = date_to_str(self.test_date)
        self.year = date.year
        self.index = index#是否是指数,True:指数
        self.current_index = None #当前日期的价格/成交如果被找到了，记录下来它在数据库中的index。用于取多个数据.

        # 最终输出的结果
        self.price = {}  # np类型
        self.prices = [] #多天的价格。按照时间由过去到将来的顺序，即序号0是最早的价格，序号-1是date当天的价格。
        self.volume = None  # np类型
        self.volumes = []
        self.nm_price = None
        self.nm_volume = None
        self.fail_message = None
        # 指示数据有效性的
        self.data_valid = True
        self.no_week_ma = False  # 没有日线的20日均线，说明再往前的日子数据都不用找了.
        self.conn = conn
    def get_price(self,day_or_week,fields=("open","close","high","low")):
        '''
        :param day_or_week: string, "day" or "week"
        :fields: tuple, 列出想提取的数据，例如：("open","close","high","low")。目前仅限于这4项之中。
        :return: numpy.array []
        '''
        query = "SELECT * from %s" % (self.code + "_"+day_or_week+"_" + str(self.year)) + \
                " WHERE date = '%s'" % self.buy_date_str
        self._do_the_query(query)
        if(self.data_valid):#数据有效
            self.current_index = self.tmp["index"]
            for field in fields:
                if(self.tmp[field] is None):
                    self.price[field] = 0.0
                else:
                    self.price[field]=self.tmp[field]
        return self.price # 成功了，result是真实数；失败了，return {}
    def get_prices(self,num,day_or_week,fields=("open","close","high","low")):
        '''
        :param num: int, 取的总天数
        :param day_or_week: str,"day" or "week"
        :param fields: tuple, 列出想提取的数据，例如：("open","close","high","low")。目前仅限于这4项之中。
        :return:
        '''
        #检查当天价格是否存在，如果不存在，结束返回
        self.get_price(day_or_week,fields)
        if(not self.data_valid):
            return None
        #从当天往前取num个数
        #for i in range(num):
        if(self.current_index>=num-1): #则当前table就有我们需要的所有数据
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year)) + \
                    " LIMIT %s,%s" %(self.current_index-num+1,num)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(fields)
        else:#当前table数据不够用,分两个table取
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year)) + \
                    " LIMIT %s,%s" % (0, self.current_index+1)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(fields)
            tmp1 = self.tmp
            query = "SELECT COUNT(*) from %s" % (self.code + "_" + day_or_week + "_" + str(self.year-1))
            self._do_the_query(query) # 得到表长
            table_length = self.tmp["COUNT(*)"]
            residue_num = num - self.current_index - 1
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year-1)) + \
                    " LIMIT %s,%s" %(table_length-residue_num,residue_num)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(fields)
            tmp2 = self.tmp
            self.prices = tmp2+tmp1
        return self.prices
    def get_volume(self,day_or_week):
        '''
        :param day_or_week: string, "day" or "week"
        :fields: tuple, 列出想提取的数据，例如：("open","close","high","low")。目前仅限于这4项之中。
        :return: numpy.array []
        '''
        query = "SELECT * from %s" % (self.code + "_"+day_or_week+"_" + str(self.year)) + \
                " WHERE date = '%s'" % self.buy_date_str
        self._do_the_query(query,field="volume")
        if(self.data_valid):#数据有效
            self.current_index = result["index"]
            if(result["volume"] is None):
                self.volume = 0.0
            else:
                self.volume = result["volume"]
        return self.volume # 成功了，return真实数；失败了，return None
    def get_volumes(self,num,day_or_week):
        #检查当天价格是否存在，如果不存在，结束返回
        self.get_volume(day_or_week)
        if(not self.data_valid):
            return None
        #从当天往前取num个数
        if(self.current_index>=num-1): #则当前table就有我们需要的所有数据
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year)) + \
                    " LIMIT %s,%s" %(self.current_index-num+1,num)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(("volume"))
        else:#当前table数据不够用,分两个table取
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year)) + \
                    " LIMIT %s,%s" % (0, self.current_index+1)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(("volume"))
            tmp1 = self.tmp
            query = "SELECT COUNT(*) from %s" % (self.code + "_" + day_or_week + "_" + str(self.year-1))
            self._do_the_query(query) # 得到表长
            table_length = self.tmp["COUNT(*)"]
            residue_num = num - self.current_index - 1
            query = "SELECT * from %s" % (self.code + "_" + day_or_week + "_" + str(self.year-1)) + \
                    " LIMIT %s,%s" %(table_length-residue_num,residue_num)
            self._do_the_query(query,multi=True)
            self._filter_result_by_fields(("volume"))
            tmp2 = self.tmp
            self.volumes = tmp2+tmp1
        return self.volumes
    def _do_the_query(self,query,field=None, multi=False):
        '''
        field: string, only one field can be used. Ex:"open", or "close"
        '''
        try:
            self.tmp = db_execute(query, self.conn,field=field, multi=multi)
            if (self.tmp is None):
                self.data_valid = False
                self.fail_message = "No this data in databass."
        except Exception as ex:
            self.data_valid = False
            self.fail_message = "No this data in databass."
    def _filter_result_by_fields(self,fields):
        filter_tmp = []
        if(self.data_valid):
            for rs in self.tmp:
                tmp_one = {}
                for field in fields:
                    if(rs[field] is None):
                        tmp_one[field] = 0.0
                    else:
                        tmp_one[field]=rs[field]
                filter_tmp.append(tmp_one)
        self.tmp = filter_tmp

def test_ts():
    one_date = date(2017,month=5,day=2)
    one_ticker = '601600'
    one_data = price_and_volume_ts(one_ticker,one_date)
    x = one_data.get_input_data()
def test_db_stock_days():
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
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    codes = ts.get_stock_basics()  # 下载全部股票代码
    for i in range(codes.index.size):
        one_dateee = date(2017, month=1, day=6)
        code = codes.index[i]
        code = "002001" #just for debug
        for j in range(3650):
            one_data = price_and_volume_db(conn,code,one_dateee)
            x = one_data.get_volumes(20,"day")
            #检查数据合理性
            if(one_data.data_valid):
                logger.debug('valid: '+str(code)+'_'+date_to_str(one_dateee)+"_"+str(x)) #数据有效
            else:#数据无效
                logger.debug('invalid: '+str(code)+'_'+date_to_str(one_dateee)+"_"+str(x))
            one_dateee = one_dateee-timedelta(1)
def test_db_stock():
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
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    codes = ts.get_stock_basics()  # 下载全部股票代码
    for i in range(codes.index.size):
        one_dateee = date(2017, month=7, day=1)
        code = codes.index[i]
        for j in range(3650):
            one_data = price_and_volume_db(conn,code,one_dateee)
            x = one_data.get_price("day")
            #检查数据合理性
            if(one_data.data_valid):
                logger.debug('valid: '+str(code)+'_'+date_to_str(one_dateee)+"_"+str(x)) #数据有效
            else:#数据无效
                logger.debug('invalid: '+str(code)+'_'+date_to_str(one_dateee)+"_"+str(x))
            one_dateee = one_dateee-timedelta(1)
def test_ts_index():
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    one_dateee = date(2015,month=7,day=30)
    one_ticker = "399005" #中小板指数
    for j in range(3650):
        one_data = price_and_volume_ts(one_ticker,one_dateee)
        x = one_data.get_price_and_volume()
        #检查数据合理性
        if(one_data.data_valid):
            logger.debug('valid: '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(x)) #数据有效
        elif(one_data.no_week_ma):
            logger.debug('invalid: no week MA20 '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(x))
            break  #数据无效，而且再往前的日子的数据都不会有效了
        else:#数据无效
            logger.debug('invalid: '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(x))
        one_dateee = one_dateee+timedelta(1)
def test_ts_stock():
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S', )
    file_handler = logging.FileHandler("./log/test_log.txt", encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    zxb_tickers = ts.get_sme_classified()
    one_dateee = date(2015, month=7, day=30)
    for i in range(zxb_tickers.index.size):
        one_ticker = zxb_tickers.iloc[i, 0]
        for j in range(3650):
            one_data = price_and_volume_ts(one_ticker, one_dateee)
            x = one_data.get_normalized_price_and_volume()

            # 检查数据合理性
            if (one_data.data_valid):
                logger.debug('valid: ' + str(one_ticker) + '_' + date_to_str(one_dateee) + "_" + str(x))  # 数据有效
            elif (one_data.no_week_ma):
                logger.debug('invalid: no week MA20 ' + str(one_ticker) + '_' + date_to_str(one_dateee) + "_" + str(x))
                break  # 数据无效，而且再往前的日子的数据都不会有效了
            else:  # 数据无效
                logger.debug('invalid: ' + str(one_ticker) + '_' + date_to_str(one_dateee) + "_" + str(x))
            one_dateee = one_dateee - timedelta(1)
if __name__ == '__main__':
    #main()
    #test_stock()
    #test_ts_index()
    #test_db_stock_days()
    test_db_stock()

#tips:
#判断未来的涨幅，应该使用后复权的未来股价与设定日的股价相减。
#即：设定日-》-》-》未来股价（后复权）
#后复权：就是把除权后的价格按以前的价格换算过来。复权后以前的价格不变，现在的价格增加，所以为了利于分析一般推荐前复权。
