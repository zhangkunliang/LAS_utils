# -*- coding: utf-8 -*-
import psycopg2


# 通过connect方法创建数据库连接
def connect(dbname, user, password, host, port):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    # 创建cursor以访问数据库
    return conn



