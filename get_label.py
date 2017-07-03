#-*- coding: UTF-8 -*-
#遗留问题：这里没有考虑未来有停牌，交易日很少怎么办。由于这种情况比较少，暂时不理会
#买入：考察日date的下一个交易日的收盘价买入，有一天的时间买
from common import *
from datetime import timedelta
from datetime import date
import tushare as ts
import logging
class input_data_label():
    def __init__(self,code,date,hold_date,profit_rate):
        '''
        :param code: string
        :param date: date类型，被测的日期，即买入日期的前一天
        :param hold_date: int，持有的天数
        :param profit_rate: 目标收益率，在buy_date与sold_date期间，达到过该收益率则成功，label=1.
        :return:
        '''
        self.code = code
        self.buy_date = date+timedelta(1)#如果buy_date不是交易日，继续往后找
        self.buy_date_str = date_to_str(self.buy_date)
        self.hold_date = hold_date
        self.label=None #  1：盈利目标达到；0：没达到目标，到sold_date时盈利在（0,profit_rate）范围; -1:亏钱
        self.sold_date = self.buy_date+timedelta(hold_date)
        self.sold_date_str = date_to_str(self.sold_date)
        self.profit_rate=profit_rate
        self.data_valid=True
    def get_label(self):
        #检查目标买入日是否是交易日
        data_day_0 = ts.get_k_data(self.code, ktype='d', autype='hfq',index=False,start=self.buy_date_str, end=self.sold_date_str)
        if(data_day_0.iloc[0,0]!=self.buy_date_str):
            #说明原定buy_date不是交易日，重新选定buy_date
            self.buy_date = date(year=int(data_day_0.iloc[0,0][0:4]),month=int(data_day_0.iloc[0,0][5:7]),day=int(data_day_0.iloc[0,0][8:10]))
            self.buy_date_str = data_day_0.iloc[0,0]
            self.sold_date = self.buy_date+timedelta(self.hold_date)
            self.sold_date_str = date_to_str(self.sold_date)
        #检查目标卖出日是否大于训练的日子
        if(self.sold_date>date.today()):
            self.input_data=[]
            print("数据无效：目标卖出日还没到，不能用来训练："+self.code+": "+self.buy_date_str+": "+self.sold_date_str)
            self.data_valid=False
            return self.label #返回None
        #获取未来的价格
        data_day_0 = ts.get_k_data(self.code, ktype='d', autype='hfq',index=False,start=self.buy_date_str, end=self.sold_date_str)
        #这里没有考虑未来有停牌，交易日很少怎么办
        for i in range(data_day_0.index.size):
            if((data_day_0.iloc[i,2]-data_day_0.iloc[0,2])/data_day_0.iloc[0,2]>self.profit_rate):
                self.label=1
                return self.label
        if(self.label is None):#没达成目标
            if((data_day_0.iloc[data_day_0.index.size-1,2]-data_day_0.iloc[0,2])/data_day_0.iloc[0,2]>0):#盈利了
                self.label=0
                return self.label
            else:#赔钱
                self.label=-1
                return self.label
def main():
    logger = logging.getLogger("mylog")
    formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
    file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    zxb_tickers = ts.get_sme_classified()
    one_dateee = date(2015,month=7,day=30)
    for i in range(zxb_tickers.index.size):
        one_ticker = zxb_tickers.iloc[i,0]
        for j in range(3650):
            one_data = input_data_label(one_ticker,one_dateee,30,0.1)
            x = one_data.get_label()

            #检查数据合理性
            if(not one_data.data_valid):
                logger.debug('invalid: '+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(x))
            #发现买入点
            if(x==1):
                logger.debug('valid: 买入'+str(one_ticker)+'_'+date_to_str(one_dateee)+"_"+str(x))

            one_dateee = one_dateee-timedelta(1)
if __name__ == '__main__':
    main()
    #test()