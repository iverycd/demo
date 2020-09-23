import binascii

import psycopg2

'''
def print_table():
    cur_tblprt = source_db.cursor()  # 生成用于输出表名的游标对象
    # tableoutput_sql = 'select table_name from user_tables where table_name=\'FRAME_USER\''
    tableoutput_sql = 'select tablename from pg_tables where schemaname=\'public\''  # 查询需要导出的表
    # tableoutput_sql = 'insert into ' + target_table + ' values(' + val_str + ')'  # 拼接insert into 目标表 values  #目标表插入语句
    cur_tblprt.execute(tableoutput_sql)  # 执行
    filename = '/tmp/table_name.csv'
    f = open(filename, 'w')
    for row_table in cur_tblprt:
        table_name = row_table[0]
        print(table_name)
        f.write(table_name + '\n')
    f.close()
    
'''

'''
def write_blob():
    """ insert a BLOB into a table """
    try:
        # read data from a picture
        conn = psycopg2.connect(database="admin", user="admin", password="11111", host="192.168.212.245",
                                port="5432")  # 源库
        cur = conn.cursor()
        img_buffer = None
        with open("/tmp/1.jpg") as reader:
            img_buffer = reader.read()
        insert_sql = "insert into test(pic) values(%s)"
        params = (psycopg2.Binary(img_buffer),)
        cur.execute(insert_sql, params)
        # commit the changes to the database
        conn.commit()
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
'''

'''
def read_blob():
    """ read BLOB data from a table """
    try:
        # read database configuration
        # connect to the PostgresQL database
        # create a new cursor object
        conn = psycopg2.connect(database="admin", user="admin", password="11111", host="192.168.212.245",
                                port="5432")  # 源库
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(""" SELECT *
                        FROM test                        
                         """
                    )

        for row in cur:
            print(str(row))
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
'''


def test():
    """ read BLOB data from a table """
    try:
        # read database configuration
        # connect to the PostgresQL database
        # create a new cursor object
        conn = psycopg2.connect(database="admin", user="admin", password="11111", host="192.168.212.245",
                                port="5432")  # 源库
        cur = conn.cursor()
        # execute the SELECT statement
        cur.execute(""" SELECT *
                        FROM test                        
                    """
                    )
        for row in cur:
            # print(str(psycopg2.Binary(row[0])))
            # print(binascii.hexlify(row[0]))
            # print(bytes(row[1])) # 字符编码值（十六进制形式）
            # print('0x'+(binascii.hexlify(row[1])).decode('UTF-8')) 输出真实16进制
            print(bytes(row[1]))
        # close the communication with the PostgresQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    test()

'''
if __name__ == '__main__':
    print_table()
'''
