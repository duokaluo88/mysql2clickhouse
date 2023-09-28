#!/usr/local/python/bin/python
# coding=utf-8

from ..model import conn
from ..model import parser
from ..unit import ddlCrt

def mysql_getmetadata(user, password, host, port, dbname, table,etl_type,targetdb, target_pre):
    cloumns_list =[]
    sqllist = []
    mysql_conn = conn.mysql_conn(user, password, host, port)
    if mysql_conn.existstable(dbname, table):
        prikey = mysql_conn.get_primarykey(dbname, table)
        if prikey != '':
            cloumns = mysql_conn.get_columns(dbname, table)
            tablename = mysql_conn.get_tablename(dbname, table)
            sqllist,cloumns_list = ddlCrt.clickhouse_ddlcreate(cloumns, tablename, prikey, targetdb, target_pre, table, etl_type)
        else:
            print(dbname + '.' + table + ' has no primary key!')
    else:
        print(dbname + '.' + table + ' not exists!')
    mysql_conn.close()
    return sqllist,cloumns_list


def sqlserver_getmetadata(user, password, host, port, dbname, table,etl_type,targetdb, target_pre):
    cloumns_list =[]
    sqllist = []
    sqlserver_conn = conn.sqlserver_conn(user, password, host, port,dbname)
    if sqlserver_conn.existstable(dbname, table):
        prikey = sqlserver_conn.get_primarykey(dbname, table)
        if prikey != '':
            cloumns = sqlserver_conn.get_columns(dbname, table)
            tablename = sqlserver_conn.get_tablename(dbname, table)
            sqllist,cloumns_list = ddlCrt.clickhouse_ddlcreate(cloumns, tablename, prikey, targetdb, target_pre, table, etl_type)
        else:
            print(dbname + '.' + table + ' has no primary key!')
    else:
        print(dbname + '.' + table + ' not exists!')
    sqlserver_conn.close()
    return sqllist,cloumns_list