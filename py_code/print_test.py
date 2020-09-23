import cx_Oracle
import sys
import os
import pymysql

# source_db = cx_Oracle.connect('NJJBXQ_DJGBZ/11111@192.168.189.208:1522/orcl11g')  # 源库
source_db = pymysql.connect("192.168.189.208", "root", "Gepoint", "cnsbzb_v8_test")  # 源库
dbname='cnsbzb_v8_test'

def print_table():
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    # tableoutput_sql = 'select table_name from user_tables where table_name=\'FRAME_USER\''
    tableoutput_sql = 'select table_name from information_schema.tables where table_schema=' + "'" +dbname + "'" + 'and TABLE_TYPE=\'BASE TABLE\'' + ' and table_name=\'test\''  # 查询需要导出的表
    # tableoutput_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        print(table_name)
        f.write(table_name + '\n')
    f.close()


if __name__ == '__main__':
    print_table()
source_db.close()

'''
source_db = cx_Oracle.connect('NJJBXQ_DJGBZ/11111@192.168.189.208:1522/orcl11g')  # 源库
cursor = source_db.cursor()
sql = "select table_name from user_tables"
cursor.execute(sql)
number = cursor.fetchall()
fp = open(file_txt, "w")
loan_count = 0
for loanNumber in number:
    loan_count += 1
    fp.write(str(loanNumber) + "\n")
fp.close()
cursor.close()
source_db.close()
print("写入完成,共写入%d条数据！" % loan_count)
'''
