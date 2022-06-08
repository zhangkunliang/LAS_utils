# -*- coding: utf-8 -*-
import time
from Log_collection import monitor, cursor
import datetime


# 设置多长时间监控一次
def collection_SetTimeinterval(time_interval):
    while True:
        monitor()
        time.sleep(time_interval)


# 给定时间段，监控该时间段中的所有数据
def collection_BySETime(start_time, end_time):
    sql = "select * from monitor_info where time >" + start_time + "and time < " + end_time
    cursor.execute(sql)
    queryData = cursor.fetchall()
    return queryData


# 监控一定时间内的日志数据,时间默认为一小时
def collection_ByTimeinterval(time_interval):
    cur_time = datetime.datetime.now()
    end_time = (cur_time + datetime.timedelta(hours=-time_interval)).strftime('%Y-%m-%d %H:%M:%S')
    sql = "select * from monitor_info where time >" + cur_time.strftime('%Y-%m-%d %H:%M:%S') + "and time < " + end_time
    cursor.execute(sql)
    queryData = cursor.fetchall()
    return queryData

collection_SetTimeinterval(30)