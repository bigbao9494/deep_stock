from common import *
from datetime import timedelta
from datetime import date
import tushare as ts
class input_data_label():
    def __init__(self,code,date,hold_date,profit_rate):
        '''
        :param code: string
        :param date: date���ͣ���������ڣ�����������
        :param hold_date: int�����е�����
        :param profit_rate: Ŀ�������ʣ���buy_date��sold_date�ڼ䣬�ﵽ������������ɹ���label=1.
        :return:
        '''
        super(input_data_label,self).__init__(code,date)
        self.code = code
        self.buy_date = date
        self.buy_date_str = date_to_str(self.buy_date)

        self.label=None #  1��ӯ��Ŀ��ﵽ��0��û�ﵽĿ�꣬��sold_dateʱӯ���ڣ�0,profit_rate����Χ; -1:��Ǯ
        self.sold_date = date+timedelta(hold_date)
        self.sold_date_str = date_to_str(self.sold_date)
        self.profit_rate=profit_rate
    def get_label(self):
        #���Ŀ���������Ƿ����ѵ��������
        if(self.sold_date>date.today()):
            self.input_data=[]
            print("������Ч��Ŀ�������ջ�û������������ѵ����"+self.code+": "+self.buy_date_str+": "+self.sold_date_str)
            self.data_valid=False
            return self.label #����None
        #��ȡδ���ļ۸�
        data_day_0 = ts.get_k_data(self.code, ktype='d', autype='hfq',index=False,start=self.buy_date_str, end=self.sold_date_str)
        for i in range(data_day_0.index.size):
            if((data_day_0.iloc[i,2]-data_day_0.iloc[0,2])/data_day_0.iloc[0,2]>self.profit_rate):
                self.label=1
                return self.label
        if(self.label is None):#û���Ŀ��
            if((data_day_0.iloc[data_day_0.index.size-1,2]-data_day_0.iloc[0,2])/data_day_0.iloc[0,2]>0):#ӯ����
                self.label=0
                return self.label
            else:#��Ǯ
                self.label=-1
                return self.label
def main():

if __name__ == '__main__':
    #main()
    test()