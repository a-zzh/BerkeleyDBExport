#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb as mdb
# 连接数据库
#conn = mdb.connect('localhost', 'root', 'root')

# 也可以使用关键字参数
conn = mdb.connect(host='192.168.1.109', port=3306, user='root', passwd='root', db='maimai', charset='utf8')

# 使用cursor()方法获取操作游标
cursor = conn.cursor()

# 不建议直接拼接sql，占位符方面可能会出问题，execute提供了直接传值
#value = [2, 'John','John','John','John', 'John','John','John','John', 'John','John','John','John']
value = [3, None]
#cursor.execute('INSERT INTO renren values(%s,%s,%s%,%s,%s,%s,%s,%s,%s,%s,%s,%s)', value)
cursor.execute("insert into renren(uid,name) values(%s,%s)",value)
conn.commit()# Commit the transaction


