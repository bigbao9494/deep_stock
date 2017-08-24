# deep_stock
基本功能：
1. 从网络下载任意股票的价格、财务数据等信息，保存在mysql数据库中。

高层功能：
1. 从网络/本地数据库获取指定股票的股价、成交量，5日均值，10日均值等（参考get_price_and_volume.py）；
2. 从网络/本地数据库获取指定日期的指数、成交量、5日均值，10日均值等（参考get_price_and_volume.py）；
3. 从网络/本地数据库获取季度报告（包括半年报和年报），并存储在mysql数据库中（download_report.py）。
4. 指定股票、买入日期、持股天数、目标收益率，然后根据“买入日期”之后的股市实际情况，得到是否达成目标的label，用于训练神经网络（get_label.py）。
4. 基于以上功能，实现了一个类，用来获取以上所有信息和label，即用于训练神经网络的x与y。由于网络速度较慢，该信息保存在本地文件夹中（项目目录下的“input_data_test.pkl”文件）（参考input_data.py）。

install：
1. pip install -r requirements.txt




