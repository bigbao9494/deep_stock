from common import *
from datetime import timedelta
from datetime import date
import tushare as ts
class input_data_label():
    def __init__(self,code,date,hold_date,profit_rate):
        '''
        :param code: string
        :param date: date类型，被测的日期，即买入日期
        :param hold_date: int，持有的天数
        :param profit_rate: 目标收益率，在buy_date与sold_date期间，达到过该收益率则成功，label=1.
        :return:
        '''
        super(input_data_label,self).__init__(code,date)
        self.code = code
        self.buy_date = date
        self.buy_date_str = date_to_str(self.buy_date)

        self.label=None #  1：盈利目标达到；0：没达到目标，到sold_date时盈利在（0,profit_rate）范围; -1:亏钱
        self.sold_date = date+timedelta(hold_date)
        self.sold_date_str = date_to_str(self.sold_date)
        self.profit_rate=profit_rate
    def get_label(self):
        #检查目标卖出日是否大于训练的日子
        if(self.sold_date>date.today()):
            self.input_data=[]
            print("数据无效：目标卖出日还没到，不能用来训练："+self.code+": "+self.buy_date_str+": "+self.sold_date_str)
            self.data_valid=False
            return self.label #返回None
        #获取未来的价格
        data_day_0 = ts.get_k_data(self.code, ktype='d', autype='hfq',index=False,start=self.buy_date_str, end=self.sold_date_str)
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

if __name__ == '__main__':
    #main()
    test()