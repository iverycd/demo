# coding=utf-8
import pymysql
import sys
import traceback
import re


class Logger(object):
    def __init__(self, filename='/tmp/test.log', stream=sys.stdout):
        self.terminal = stream
        self.log = open(filename, 'w')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


sys.stdout = Logger(stream=sys.stdout)

print('hello')

conn = pymysql.connect(
    host="192.168.189.208",
    port=3306,
    user="root",
    passwd="Gepoint",
    db="test",
    charset="utf8mb4")
# 使用cursor()方法获取数据库的操作游标
cursor = conn.cursor()

# 插入一条数据
try:
    insert = cursor.execute("insert into test3(id,name) values(1,'123456')")
# except pymysql.err.DataError:
except Exception as e:
    # print(e)
    print(traceback.format_exc())

    # traceback.print_exc(file=open('/tmp/test.log', 'a'))

#    print(pymysql.err.DataError)

# 关闭游标
cursor.close()
# 提交事务
conn.commit()  # 提交事务后，不能回滚了
# 关闭数据库连接
conn.close()
