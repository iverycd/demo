#encoding:utf-8
import sys
import MySQLdb
import mysqlHelper
import cx_Oracle
import oracleHelper

reload(sys)
sys.setdefaultencoding( "utf-8" )
def mig_database():
    sql="select table_name from user_tables"
    oraHelper=oracleHelper.OracleHelper()
    rows=oraHelper.queryAll(sql)
    for row in rows:
        print(row["TABLE_NAME"])
        mig_table(row["TABLE_NAME"])

def mig_table(table_name):
    file_object = open('log.txt','a+')
    print ("开始迁移"+table_name+"......")
    file_object.write("开始迁移"+table_name+"......\n")
    myhelper=mysqlHelper.MySQLHelper()
    oraHelper=oracleHelper.OracleHelper()
    myhelper.selectDb("nap")
    myhelper.query("delete from "+table_name)
    rows=oraHelper.queryAll("select * from "+table_name)
    n=0
    try:
        for row in rows:
            dict=convertDict(row)
            myhelper.selectDb("nap")
            myhelper.insert(table_name,dict)
            n=n+1
        myhelper.commit()
        print(table_name + "迁移完毕，共导入%d条数据。" % n)
        file_object.write(table_name+"迁移完毕，共导入%d条数据。\n" %n)
    except MySQLdb.Error as e:
        print(table_name + "迁移失败，程序异常！")
        file_object.write("table_name"+"迁移失败，程序异常!\n" %n)
        print(e)
    file_object.close()

def convertDict(row):
    dict={}
    for k in row.keys():
        #if isinstance(row[k],basestring):
        if isinstance(row[k],str) or isinstance(row[k],unicode):
            dict[k]= unicode2utf8(gbk2unicode(row[k]))
            #dict[k]=row[k]
        else:
            dict[k]=row[k]
    return dict


def gbk2unicode(s):
    return s.decode('gbk', 'ignore')

def unicode2utf8(s):
    return s.encode('utf-8')


if __name__ == '__main__':
    mig_database()


#encoding:utf-8
import MySQLdb
from datetime import *
class MySQLHelper:
    #def __init__(self,host,user,password,charset="utf8"):
    def __init__(self):
        self.host='localhost'
        self.user='root'
        self.password='123456'
        self.charset='utf8'
        try:
            self.conn=MySQLdb.connect(host=self.host,user=self.user,passwd=self.password)
            self.conn.set_character_set(self.charset)
            self.cur=self.conn.cursor()
        except MySQLdb.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def selectDb(self,db):
      try:
          self.conn.select_db(db)
      except MySQLdb.Error as e:
          print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def query(self,sql):
        try:
           n=self.cur.execute(sql)
           return n
        except MySQLdb.Error as e:
           print("Mysql Error:%s\nSQL:%s" %(e,sql))
           raise e


    def queryRow(self,sql):
        self.query(sql)
        result = self.cur.fetchone()
        return result

    def queryAll(self,sql):
        self.query(sql)
        result=self.cur.fetchall()
        desc =self.cur.description
        d = []
        for inv in result:
             _d = {}
             for i in range(0,len(inv)):
                 _d[desc[i][0]] = str(inv[i])
             d.append(_d)
        return d

    def insert(self,p_table_name,p_data):
        for key in p_data:
            if (isinstance(p_data[key],str) or isinstance(p_data[key],datetime) ):
                if str(p_data[key])=="None":
                    p_data[key]='null'
                else:
                    p_data[key] = "'"+str(p_data[key]).replace('%','％').replace('\'','')+"'"
            else:
                p_data[key] = str(p_data[key])

        key ='`'+ '`,`'.join(p_data.keys())+'`'
        value = ','.join(p_data.values())
        real_sql = "INSERT INTO " + p_table_name + " (" + key + ") VALUES (" + value + ")"
        return self.query(real_sql)
    def getLastInsertId(self):
        return self.cur.lastrowid

    def rowcount(self):
        return self.cur.rowcount

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


#encoding:utf-8
import cx_Oracle
from datetime import *
class OracleHelper:
    def __init__(self):
        self.charset='utf8'
        try:
            self.conn = cx_Oracle.connect('scott/tiger@DESKTOP-IASSOVJ/orcl')
            self.cur=self.conn.cursor()
        except cx_Oracle.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def selectDb(self,db):
      try:
          self.conn.select_db(db)
      except cx_Oracle.Error as e:
          print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def query(self,sql):
        try:
           n=self.cur.execute(sql)
           return n
        except cx_Oracle.Error as e:
           print("Mysql Error:%s\nSQL:%s" %(e,sql))
           raise e


    def queryRow(self,sql):
        self.query(sql)
        result = self.cur.fetchone()
        return result

    def queryAll(self,sql):
        self.query(sql)
        result=self.cur.fetchall()
        desc =self.cur.description
        d = []
        for inv in result:
             _d = {}
             for i in range(0,len(inv)):
                 _d[desc[i][0]] = str(inv[i])
             d.append(_d)
        return d

    def insert(self,p_table_name,p_data):
        for key in p_data:
            if (isinstance(p_data[key],str) or isinstance(p_data[key],datetime) ):
                if str(p_data[key])=="None":
                    p_data[key]='null'
                else:
                    p_data[key] = "'"+str(p_data[key]).replace('%','％').replace('\'','')+"'"
            else:
                p_data[key] = str(p_data[key])

        key   = ','.join(p_data.keys())
        value = ','.join(p_data.values())
        real_sql = "INSERT INTO " + p_table_name + " (" + key + ") VALUES (" + value + ")"
        return self.query(real_sql)
    def getLastInsertId(self):
        return self.cur.lastrowid

    def rowcount(self):
        return self.cur.rowcount

    def commit(self):
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()