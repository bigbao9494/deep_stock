#-*- coding: UTF-8 -*-
def db_execute(query, conn, field=None):

    u"""Helper method for executing the given MySQL non-query.
    :param query: MySQL query to be executed.
    :param conn: MySQL connection.
    """
    cursor = conn.cursor()

    cursor.execute(query)
    if(field is None):
        result = cursor.fetchone()
    else:
        result = cursor.fetchone()[field]
    cursor.close()
    return result

def normalize(input_list):
    #input_list: np.array
    mean = input_list.mean()
    std = input_list.std()
    if(std==0):
        return [0,0,0]
    for i in range(len(input_list)):
        input_list[i] = (input_list[i] - mean) / std
    return input_list
def date_to_str(date_python):#
    return date_python.strftime('%Y-%m-%d')
def compute_ma(input_data):
    #计算均值，输出list: [ma5,ma10,ma20]
    #以input_data的最后一个值为起点，往左找5天计算ma5，往左找10天计算ma10......
    tmp_sum=0.0
    for i in range(20):
        tmp_sum += input_data[input_data.index[-i-1]]
        if(i==4):
            ma5=tmp_sum/5.0
        if(i==9):
            ma10=tmp_sum/10.0
        if(i==19):
            ma20=tmp_sum/20.0
    return [ma5,ma10,ma20]

def get_season_int(date):
    '''
    :param date: date类型
    :return:(year,season), 其中year和season都是int类型
    '''
    str_date = date_to_str(date)
    year = int(str_date[0:4])
    month = int(str_date[5:7])
    season = int((month - 1) / 3)  # 4,5,6看一季报,789看二季报，10\11\12看三季报，1\2\3

    if season == 0:
        season = 4
        year = year-1#看上一年的4季报
    return year,season