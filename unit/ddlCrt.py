#!/usr/local/python/bin/python
# coding=utf-8




def clickhouse_ddlcreate(cloumns, tablename, prikey, targetdb, target_pre, table,etl_type):
    cloumnsstr = ''
    sqllist = []
    cloumns_list=[]
    # drpmvstr = '''drop VIEW IF EXISTS %s.`%s%s_mv` ;''' % (targetdb, target_pre, table)

    drpreptablestr = 'DROP TABLE IF EXISTS %s.`%s%s` ;' % (targetdb, target_pre, table)

    # ReplacingMergeTree表
    createstr = 'CREATE TABLE IF NOT EXISTS %s.`%s%s` (' % (targetdb, target_pre, table)
    enginestr = 'ENGINE = ReplacingMergeTree(ts_ms)'
    orderstr = 'ORDER BY (%s)' % prikey
    settingstr = 'SETTINGS index_granularity = 8192,use_metadata_cache = 1'
    tablecommentstr = 'COMMENT \'%s\';' % tablename
    for i in cloumns:
        cloumnsstr = cloumnsstr + '`' + i[0] + '`' + ' ' + i[2] + ' ' + i[1] + ' comment \'' + i[
            3].replace('\'','') + '\',' + '\n'
        cloumns_list.append(i[0])
    cloumnsstr = cloumnsstr + '''`__op` Int8 DEFAULT 0 COMMENT '删除标记', ''' + '\n'
    cloumnsstr = cloumnsstr + '''`ts_ms` Int64 DEFAULT 0 COMMENT 'cdc时间戳', ''' + '\n'
    cloumnsstr = cloumnsstr + ''' `etl_time` DateTime DEFAULT now() COMMENT 'ETL加载时间' ''' + '\n)'
    crereptablestr = str(createstr) + '\n' \
             + cloumnsstr + '\n' \
             + enginestr + '\n' \
             + orderstr + '\n' \
             + settingstr + '\n' \
             + tablecommentstr

    # MergerTree表
    # drptablestr = 'DROP TABLE IF EXISTS %s.`%s%s_binlog` ;' % (targetdb, target_pre, table)
    #
    # createstr = 'CREATE TABLE IF NOT EXISTS %s.`%s%s_binlog` (' % (targetdb, target_pre, table)
    # enginestr2 = 'ENGINE = MergeTree'
    # cretablestr= str(createstr) + '\n' \
    #          + cloumnsstr + '\n' \
    #          + enginestr2 + '\n' \
    #          + orderstr + '\n' \
    #          + settingstr + '\n' \
    #          + tablecommentstr
    #
    # cremvstr = '''CREATE MATERIALIZED VIEW IF NOT EXISTS %s.`%s%s_mv`
    #          TO %s.`%s%s`
    #          as
    #      select *
    #        from %s.`%s%s_binlog`;''' % (targetdb, target_pre, table,targetdb, target_pre, table,targetdb, target_pre, table)

    drpviewstr = '''drop VIEW IF EXISTS %s.`%s%s_v`; ''' % (targetdb, target_pre, table)

    creviewstr = '''CREATE VIEW IF NOT EXISTS %s.`%s%s_v` 
        as
    select *
      from %s.`%s%s` final
     where `__op` !=1;''' % (targetdb, target_pre, table,targetdb, target_pre, table)
    if etl_type=='flink':
        # sqllist.append(drpmvstr)
        sqllist.append(drpreptablestr)
        sqllist.append(crereptablestr)
        # sqllist.append(drptablestr)
        # sqllist.append(cretablestr)
        # sqllist.append(cremvstr)
        sqllist.append(drpviewstr)
        sqllist.append(creviewstr)
    else:
        sqllist.append(drpreptablestr)
        sqllist.append(crereptablestr)
    return sqllist,cloumns_list