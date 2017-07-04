#-*- coding: UTF-8 -*-
from get_report import *
from get_label import *
from get_price_and_volume import *
from common import *
import pickle

logger = logging.getLogger("mylog")
formatter = logging.Formatter('%(asctime)s %(message)s', '%a, %d %b %Y %H:%M:%S',)
file_handler = logging.FileHandler("./log/test_log.txt",encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

def input_data(code,index_code,date,hold_date,profit_rate ):
    '''
    :param code: string
    :param index_code: string
    :param date: date
    :param hold_date: int 持股天数。从date的下一个交易日的close价格买入，date+30天自然日（交易日不足30个）卖出.
    :param profit_rate: float,目标收益率。例如0.1是10%的收益
    :return: input_data:
    '''
    #get price
    stock_obj = price_and_volume(code,date)
    stock_data = stock_obj.get_normalized_price_and_volume()
    if(stock_obj.data_valid):
        logger.debug('price and volume valid: '+str(code)+'_'+date_to_str(date)+"_"+str(stock_data)) #数据有效
    else:
        return None,None
    #get index data
    index_obj = price_and_volume(index_code,date,index=True)
    index_data = index_obj.get_normalized_price_and_volume()
    if(index_obj.data_valid):
        logger.debug('index valid: '+str(index_code)+'_'+date_to_str(date)+"_"+str(index_data)) #数据有效
    else:
        return None,None
    #get report
    conn = pymysql.connect(**pymysql_config)
    rp_obj = get_report_db_by_date(conn,code,date)
    report = rp_obj.get_data_by_date_normalized("eps_yoy","roe","profits_yoy")
    if(rp_obj.data_valid):
        logger.debug('report valid: '+str(code)+'_'+date_to_str(date)+"_"+str(report))
    else:
        return None,None
    #get label
    label_obj = input_data_label(code,date,hold_date,profit_rate)
    label_data = label_obj.get_label()
    if(label_obj.data_valid):
        logger.debug('label valid: '+str(code)+'_'+date_to_str(date)+"_"+str(label_data)) #数据有效
    else:
        return None,None

    #成功了
    return np.append(np.append(stock_data,index_data),report),label_data

def test():
    '''log记录下了所有成功的数据。print了所有不成功的数据。
    暂时发现不成功的原因包括：
    1. 当天不是交易日；
    2.
    :return:
    '''
    hold_date = 30
    profit_rate = 0.1
    zxb_tickers = ts.get_sme_classified()
    one_dateee = date(2017,month=5,day=3)
    try:#载入进度
        with open('input_data_test.pkl',"rb") as pkf:
            result = pickle.load(pkf)
            i_day = pickle.load(pkf)
            i_ticker = pickle.load(pkf)
    except:
        i_ticker=0
        i_day=0
        result = []
    one_dateee = one_dateee-timedelta(i_day)
    try:
        for i in range(i_ticker,zxb_tickers.index.size):
            one_ticker = zxb_tickers.iloc[i,0]
            for j in range(i_day,3525):
                x,y = input_data(one_ticker,'399005',one_dateee,hold_date=hold_date,profit_rate=profit_rate)
                if(x is None):
                    print('invalid: '+str(one_ticker)+'_'+date_to_str(one_dateee))
                else:
                    result.append((x,y))
                one_dateee = one_dateee-timedelta(1)
            one_dateee = date(2017,month=5,day=3)
            i_day = 0
    except KeyboardInterrupt as ex:
        with open('input_data_test.pkl',"wb") as pkf:
            pickle.dump(result,pkf)
            pickle.dump(j,pkf)
            pickle.dump(i,pkf)
def load_input_from_disk():
    with open('input_data_test.pkl',"rb") as pkf:
            result = pickle.load(pkf)
    return result

def debug():
    hold_date = 30
    profit_rate = 0.1
    zxb_tickers = ts.get_sme_classified()
    one_dateee = date(2007, month=9, day=4)

    i_ticker = 0
    i_day = 0
    result = []
    one_dateee = one_dateee - timedelta(i_day)
    try:
        for i in range(i_ticker, zxb_tickers.index.size):
            one_ticker = zxb_tickers.iloc[i, 0]
            for j in range(i_day, 3590):
                x, y = input_data(one_ticker, '399005', one_dateee, hold_date=hold_date, profit_rate=profit_rate)
                if (x is None):
                    print('invalid: ' + str(one_ticker) + '_' + date_to_str(one_dateee))
                else:
                    result.append((x, y))
                one_dateee = one_dateee - timedelta(1)
    except KeyboardInterrupt as ex:
        pass
if __name__ == '__main__':
    test()
    #debug()
