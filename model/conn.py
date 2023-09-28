# encoding=utf-8
import pymysql
from sqlalchemy import create_engine, inspect


class clickhouse_conn:
    def __init__(self, ckuser, ckpassword, ckhost,ck_port):
        engine = create_engine('clickhouse+native://%s:%s@%s:%s/cdm?socket_timeout=300000' %(ckuser,ckpassword,ckhost,int(ck_port)))
        self.connect = engine.connect()

    def close(self):
        self.connect.close()

    def execute(self, sql):
        try:
            self.connect.execute(sql)
        except Exception as e:
            print('failed:',e.args)
            exit()
        else:
            print('success:', sql.split('(')[0].split('\n')[0])





class mysql_conn:
    def __init__(self, user, password, host, port):
        # engine = pymysql.connect(user=user, password=password, host=host, port=int(port))
        engine = create_engine("mysql+pymysql://%s:%s@%s:%s/sys?charset=utf8mb4" % (user,password,host,port))
        self.connect = engine.connect()

    def close(self):
        self.connect.close()

    def get_columns(self, dbname, tablename):
        sql = '''
                select COLUMN_NAME 
                      ,case when IS_NULLABLE='YES' then 'DEFAULT NULL' 
                            else '' end 
                      ,case when DATA_TYPE ='enum' then 'String' 
                            when DATA_TYPE ='json' then 'String' 
                            when DATA_TYPE ='time' then 'String' 
                            else REPLACE(REPLACE(COLUMN_TYPE,' unsigned',''),' zerofill','') end
                      ,COLUMN_COMMENT 
                  from information_schema.`COLUMNS` c 
                 where TABLE_SCHEMA ='%s' and TABLE_NAME ='%s'
              order by ORDINAL_POSITION ;
            ''' % (dbname, tablename)

        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        return results

    def get_tablename(self, dbname, tablename):
        sql = '''
                select TABLE_COMMENT 
                  from information_schema.TABLES t 
                 where TABLE_SCHEMA ='%s' and TABLE_NAME ='%s';
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0][0]
        return results

    def existstable(self, dbname, tablename):
        sql = '''
                select count(1)
                 from information_schema.TABLES t 
                where TABLE_SCHEMA ='%s' and TABLE_NAME ='%s' and table_type='BASE TABLE';
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        data = cursor.fetchall()[0][0]
        if data == 1:
            results = True
        else:
            results = False
        return results

    def get_primarykey(self, dbname, tablename):
        sql = '''
                select COLUMN_NAME
                  from information_schema.`COLUMNS` c 
                 where TABLE_SCHEMA ='%s' and TABLE_NAME ='%s'
                   and COLUMN_KEY='PRI'
              order by ORDINAL_POSITION
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        data = cursor.fetchall()
        result = []
        for i in data:
            result.append(i[0])
        results = ','.join(result)
        return results




class sqlserver_conn:
    def __init__(self, user, password, host, port,db):
        # self.engine = pymysql.connect(user=user, password=password, host=host, port=int(port))
        engine = create_engine("mssql+pymssql://%s:%s@%s:%s/%s?charset=utf8" % (user,password,host,port,db))
        self.connect = engine.connect()

    def close(self):
        self.connect.close()

    def get_columns(self, dbname, tablename):
        sql = '''
                 select COLUMN_NAME 
                      ,case when IS_NULLABLE='YES' then 'DEFAULT NULL' 
                            else '' end 
                      ,case when DATA_TYPE ='enum' then 'String' 
                            when NUMERIC_SCALE>0 then 'Float64'
                            when CHARACTER_MAXIMUM_LENGTH>0 then 'String'
                            when DATA_TYPE ='timestamp' then 'String' 
                            when DATA_TYPE ='decimal' then 'Float64' 
                            when DATA_TYPE ='numeric' then 'Float64' 
                            else DATA_TYPE
                            end
                      ,'' COLUMN_COMMENT 
                  from information_schema.COLUMNS c 
                 where TABLE_CATALOG= '%s' AND TABLE_SCHEMA ='dbo' and TABLE_NAME ='%s'
              order by ORDINAL_POSITION ;
            ''' % (dbname, tablename)

        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        return results

    def get_tablename(self, dbname, tablename):
        sql = '''
                select '' as TABLE_COMMENT 
                  from information_schema.TABLES t 
                 where TABLE_CATALOG= '%s' AND TABLE_SCHEMA ='dbo' and TABLE_NAME ='%s';
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0][0]
        return results

    def existstable(self, dbname, tablename):
        sql = '''
                select count(1)
                 from information_schema.TABLES t 
                where TABLE_CATALOG= '%s' AND TABLE_SCHEMA ='dbo' and TABLE_NAME ='%s' and table_type='BASE TABLE';
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        data = cursor.fetchall()[0][0]
        if data == 1:
            results = True
        else:
            results = False
        return results

    def get_primarykey(self, dbname, tablename):
        sql = '''
                select b.COLUMN_NAME
                  from information_schema.TABLE_CONSTRAINTS as a
             left join information_schema.KEY_COLUMN_USAGE as b on a.CONSTRAINT_NAME=b.CONSTRAINT_NAME
                 where a.TABLE_CATALOG= '%s' AND a.TABLE_SCHEMA ='dbo' and a.TABLE_NAME ='%s' and  a.CONSTRAINT_TYPE='PRIMARY KEY'
              order by ORDINAL_POSITION
            ''' % (dbname, tablename)
        cursor = self.connect.execute(sql)
        data = cursor.fetchall()
        result = []
        for i in data:
            result.append(i[0])
        results = ','.join(result)
        return results


# if __name__ == '__main__':
#     user = 'sa'
#     password = 'jl.123456'
#     host = '139.224.57.218'
#     port = 8006
#     db='E3TEST'
#     ss=sqlserver_conn(user,password,host,port,db)
#
#     result=ss.get_columns('E3TEST','SPKCB')
#     print(result)

