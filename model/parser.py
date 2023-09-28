#!/usr/local/python/bin/python
# coding=utf-8

def josn_read():
    f=open('D:\JetBrains\pythonProject37-pyflink1.12\mysql2clickhouse\\template\mysql_template','r')
    josn_template=f.read()
    return josn_template

# 生成变量
def join_context(cloumns_list,soucre_table,soucre_db,target_table):
    post_context=''
    source_context = 'select `' + '`,`'.join(cloumns_list) + '` from `' + soucre_db +'`.`'+soucre_table+'`'
    target_cloumns ='"`'+'`","`'.join(cloumns_list)+'`"'
    target_table = target_table
    return source_context,target_table,target_cloumns,post_context

# 替换json模板
def josn_replace(josn_template,source_context,target_table,target_cloumns,post_context,soucre_database,target_database):
    josn_template=josn_template.replace('${source_context}',str(source_context))
    josn_template = josn_template.replace('${target_table}', str(target_table))
    josn_template = josn_template.replace('${target_cloumns}', str(target_cloumns))
    josn_template = josn_template.replace('${post_context}', str(post_context))
    josn_template = josn_template.replace('${Mysql_Db}', str(soucre_database))
    josn_template = josn_template.replace('${CDMDB}', str(target_database))

    return josn_template