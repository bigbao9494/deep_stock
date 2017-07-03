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