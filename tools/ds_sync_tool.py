# encoding=utf-8
import re
from sqlalchemy import create_engine
from datetime import datetime


# 标版ds工作流同步工具


class mysql_conn:
    def __init__(self, user, password, host, port):
        # self.engine = pymysql.connect(user=user, password=password, host=host, port=int(port))
        engine = create_engine("mysql+pymysql://%s:%s@%s:%s/sys?charset=utf8mb4" % (user, password, host, port))
        self.connect = engine.connect()

    def close(self):
        self.connect.close()

    def execute(self, sql):
        try:
            self.connect.execute(sql)
        except Exception as e:
            print(sql)
            print('failed:', e.args)
            exit()
        else:
            print('success:', sql)

    def get_ds_process_definition(self, processCode):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_process_definition
            where code= %s
            ''' % processCode
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)
        ss.pop(0)
        sqlstr = "delete from dolphinscheduler.t_ds_process_definition where code= %s;" % processCode
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_process_definition " \
                 "(code, name, version, description, project_code, release_state, user_id, global_params, flag, locations, warning_group_id, timeout, tenant_id, execution_type, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

    def get_ds_tenant(self):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_tenant
            where id=2
            '''
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)

        sqlstr = "delete from dolphinscheduler.t_ds_tenant where id=2;"
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_tenant " \
                 "(id, tenant_code, description, queue_id, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

    def get_ds_user(self):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_user
            where id=6
            '''
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)

        sqlstr = "delete from dolphinscheduler.t_ds_user where id=6;"
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_user " \
                 "(id, user_name, user_password, user_type, email, phone, tenant_id, create_time, update_time, queue, state)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

    def get_ds_project(self):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_project
            where id=14
            '''
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)

        sqlstr = "delete from dolphinscheduler.t_ds_project where id=14;"
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_project " \
                 "(id, name, code, description, user_id, flag, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

    def get_ds_datasource(self):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_datasource
            where id=5
            '''
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)

        sqlstr = "delete from dolphinscheduler.t_ds_datasource where id=5;"
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_datasource " \
                 "(id, name, note, `type`, user_id, connection_params, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist


    def get_ds_process_task_relation(self,processCode):
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_process_task_relation
            where project_code =1669102277000 and  process_definition_code=%s
            ''' % processCode
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        sqlstr = "delete from dolphinscheduler.t_ds_process_task_relation where project_code =1669102277000 and  process_definition_code=%s;" % processCode
        sqllist.append(sqlstr)
        for sqlresult in results:
            ss = []
            for i in sqlresult:

                # if i.__class__.__name__ == 'NoneType':
                #     i='NULL'
                if i.__class__.__name__ == 'datetime':
                    i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
                ss.append(i)
            ss.pop(0)
            sqlstr = "INSERT INTO dolphinscheduler.t_ds_process_task_relation " \
                     "(name, project_code, process_definition_code, process_definition_version, pre_task_code, pre_task_version, post_task_code, post_task_version, condition_type, condition_params, create_time, update_time)" \
                     " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
            sqllist.append(sqlstr)
        return sqllist

    def get_ds_process_task_definition(self,processCode):
        sqllist=[]
        sql = '''
            select a.*
            from dolphinscheduler.t_ds_task_definition as a 
            where code in  (
            select distinct post_task_code
            from dolphinscheduler.t_ds_process_task_relation
            where project_code =1669102277000 and  process_definition_code=%s
            ) 
            ''' % processCode
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        sqlstr = "delete from dolphinscheduler.t_ds_task_definition " \
                 "where code in  (" \
                 "select distinct post_task_code " \
                 "from dolphinscheduler.t_ds_process_task_relation " \
                 "where project_code =1669102277000 and  process_definition_code=%s) ;" % processCode
        sqllist.append(sqlstr)
        for sqlresult in results:
            ss = []
            for i in sqlresult:

                # if i.__class__.__name__ == 'NoneType':
                #     i='NULL'
                if i.__class__.__name__ == 'datetime':
                    i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
                ss.append(i)
            ss.pop(0)
            sqlstr = "INSERT INTO dolphinscheduler.t_ds_task_definition " \
                     "(code, name, version, description, project_code, user_id, task_type, task_params, flag, task_priority, worker_group, environment_code, fail_retry_times, fail_retry_interval, timeout_flag, timeout_notify_strategy, timeout, delay_time, resource_ids, create_time, update_time)" \
                     " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
            sqllist.append(sqlstr)
        return sqllist

    def get_ds_process_definition_log(self, processCode):
        ss = []
        sqllist=[]
        sql = '''
            Select *
            From dolphinscheduler.t_ds_process_definition_log
            Where project_code=1669102277000 and code=%s
            And version=(select version from dolphinscheduler.t_ds_process_definition
            Where project_code=1669102277000 and code=%s)
            ''' % (processCode,processCode)
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        sqlstr = "delete from dolphinscheduler.t_ds_process_definition_log " \
                 "where project_code=1669102277000 and code=%s;" % processCode
        sqllist.append(sqlstr)
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)
        ss.pop(0)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_process_definition_log " \
                 "(code, name, version, description, project_code, release_state, user_id, global_params, flag, locations, warning_group_id, timeout, tenant_id, execution_type, operator, operate_time, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

    def get_ds_process_task_relation_log(self,processCode):
        sqllist=[]
        sql = '''
            Select *
            From dolphinscheduler.t_ds_process_task_relation_log
            Where project_code=1669102277000 and process_definition_code=%s
            And process_definition_version in (select process_definition_version from dolphinscheduler.t_ds_process_task_relation
            Where project_code=1669102277000 and process_definition_code=%s)
            ''' % (processCode,processCode)
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        sqlstr = "delete from dolphinscheduler.t_ds_process_task_relation_log " \
                 "Where project_code=1669102277000 and process_definition_code=%s;" % processCode
        sqllist.append(sqlstr)
        for sqlresult in results:
            ss = []
            for i in sqlresult:

                # if i.__class__.__name__ == 'NoneType':
                #     i='NULL'
                if i.__class__.__name__ == 'datetime':
                    i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
                ss.append(i)
            ss.pop(0)
            sqlstr = "INSERT INTO dolphinscheduler.t_ds_process_task_relation_log " \
                     "(name, project_code, process_definition_code, process_definition_version, pre_task_code, pre_task_version, post_task_code, post_task_version, condition_type, condition_params, operator, operate_time, create_time, update_time)" \
                     " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
            sqllist.append(sqlstr)
        return sqllist

    def get_ds_process_task_definition_log(self,processCode):
        sqllist=[]
        sql = '''
            Select a.*
            From dolphinscheduler.t_ds_task_definition_log as a
            Inner join(
            Select distinct post_task_code,post_task_version
            From dolphinscheduler.t_ds_process_task_relation_log
            Where project_code=1669102277000 and process_definition_code=%s
            And process_definition_version in(select process_definition_version from dolphinscheduler.t_ds_process_task_relation
            Where project_code=1669102277000 and process_definition_code=%s)
            ) as b on a.code=b.post_task_code and a.version=b.post_task_version
            ''' % (processCode,processCode)
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()
        sqlstr = "delete from dolphinscheduler.t_ds_task_definition_log " \
                 "where code in  (" \
                 "select distinct post_task_code " \
                 "from dolphinscheduler.t_ds_process_task_relation " \
                 "where project_code =1669102277000 and  process_definition_code=%s) ;" % processCode
        sqllist.append(sqlstr)
        for sqlresult in results:
            ss = []
            for i in sqlresult:

                # if i.__class__.__name__ == 'NoneType':
                #     i='NULL'
                if i.__class__.__name__ == 'datetime':
                    i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
                ss.append(i)
            ss.pop(0)
            sqlstr = "INSERT INTO dolphinscheduler.t_ds_task_definition_log " \
                     "(code, name, version, description, project_code, user_id, task_type, task_params, flag, task_priority, worker_group, environment_code, fail_retry_times, fail_retry_interval, timeout_flag, timeout_notify_strategy, timeout, delay_time, resource_ids, operator, operate_time, create_time, update_time)" \
                     " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
            sqllist.append(sqlstr)
        return sqllist

    def get_ds_schedules(self,processCode):
        ss = []
        sqllist=[]
        sql = '''
            select *
            from dolphinscheduler.t_ds_schedules
            where process_definition_code =%s
            ''' % processCode
        cursor = self.connect.execute(sql)
        results = cursor.fetchall()[0]
        for i in results:

            # if i.__class__.__name__ == 'NoneType':
            #     i='NULL'
            if i.__class__.__name__ == 'datetime':
                i = datetime.strftime(i, "%Y-%m-%d %H:%M:%S")
            ss.append(i)
        ss.pop(0)
        sqlstr = "delete from dolphinscheduler.t_ds_schedules where process_definition_code =%s; " % processCode
        sqllist.append(sqlstr)
        sqlstr = "INSERT INTO dolphinscheduler.t_ds_schedules " \
                 "(process_definition_code, start_time, end_time, timezone_id, crontab, failure_strategy, user_id, release_state, warning_type, warning_group_id, process_instance_priority, worker_group, environment_code, create_time, update_time)" \
                 " VALUES%s ;" % str(tuple(ss)).replace('None', 'NULL')
        sqllist.append(sqlstr)
        return sqllist

class sql_parse:
    def __init__(self, connConfig):
        self.Mysql_User = connConfig['Mysql_User']
        self.Mysql_PassWord = connConfig['Mysql_PassWord']
        self.Mysql_Ip = connConfig['Mysql_Ip']
        self.CK_User = connConfig['CK_User']
        self.CK_PassWord = connConfig['CK_PassWord']
        self.CK_Ip = connConfig['CK_Ip']
        self.DbName_Re = connConfig['DbName_Re']
        self.DbName_Re1 = connConfig['DbName_Re1']



    def sql_replace(self, sql_template):
        sql_template = sql_template.replace('{"prop": "Mysql_PassWord", "direct": "IN", "type": "VARCHAR", "value": "4bueWSh@fzWwWG7"}'
                                            ,'{"prop": "Mysql_PassWord", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_PassWord)
        sql_template = sql_template.replace('{"prop":"Mysql_PassWord","direct":"IN","type":"VARCHAR","value":"4bueWSh@fzWwWG7"}'
                                            ,'{"prop": "Mysql_PassWord", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_PassWord)
        sql_template = sql_template.replace('{"prop": "Mysql_User", "direct": "IN", "type": "VARCHAR", "value": "e3plus"}'
                                            , '{"prop": "Mysql_User", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_User)
        sql_template = sql_template.replace('{"prop":"Mysql_User","direct":"IN","type":"VARCHAR","value":"e3plus"}'
                                            , '{"prop": "Mysql_User", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_User)
        sql_template = sql_template.replace('{"prop": "Mysql_Ip", "direct": "IN", "type": "VARCHAR", "value": "192.168.145.170:3306"}'
                                            , '{"prop": "Mysql_Ip", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_Ip)
        sql_template = sql_template.replace('{"prop":"Mysql_Ip","direct":"IN","type":"VARCHAR","value":"192.168.145.170:3306"}'
                                            , '{"prop": "Mysql_Ip", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.Mysql_Ip)
        sql_template = sql_template.replace('{"prop": "CK_User", "direct": "IN", "type": "VARCHAR", "value": "e3plus"}'
                                            , '{"prop": "CK_User", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_User)
        sql_template = sql_template.replace('{"prop":"CK_User","direct":"IN","type":"VARCHAR","value":"e3plus"}'
                                            , '{"prop": "CK_User", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_User)
        sql_template = sql_template.replace('{"prop": "CK_PassWord", "direct": "IN", "type": "VARCHAR", "value": "nGB1d$c!Q5$g^JX7"}'
                                            , '{"prop": "CK_PassWord", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_PassWord)
        sql_template = sql_template.replace('{"prop":"CK_PassWord","direct":"IN","type":"VARCHAR","value":"nGB1d$c!Q5$g^JX7"}'
                                            , '{"prop": "CK_PassWord", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_PassWord)
        sql_template = sql_template.replace('{"prop": "CK_Ip", "direct": "IN", "type": "VARCHAR", "value": "192.168.149.229:8123"}'
                                            , '{"prop": "CK_Ip", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_Ip)
        sql_template = sql_template.replace('{"prop":"CK_Ip","direct":"IN","type":"VARCHAR","value":"192.168.149.229:8123"}'
                                            , '{"prop": "CK_Ip", "direct": "IN", "type": "VARCHAR", "value": "%s"}' % self.CK_Ip)
        sql_template = sql_template.replace('"user":"e3plus"', '"user":"%s"' % self.CK_User)
        sql_template = sql_template.replace('"password":"nGB1d$c!Q5$g^JX7"', '"password":"%s"' % self.CK_PassWord)
        sql_template = sql_template.replace('jdbc:clickhouse://192.168.149.229:8123', 'jdbc:clickhouse://%s' % self.CK_Ip)
        datepat=re.compile(r'e3plus_([a-zA-Z0-9]+)_integrate')
        sql_template = datepat.sub(self.DbName_Re,sql_template)
        datepat=re.compile(r'e3plus_([a-zA-Z0-9]+)_([a-zA-Z0-9]+)_integrate')
        sql_template = datepat.sub(self.DbName_Re1,sql_template)
        return sql_template


def main(user, password, host, port):
    source_conn = mysql_conn(user, password, host, port)

    sqllist=[]
    result = source_conn.get_ds_tenant()
    sqllist = sqllist + result

    result= source_conn.get_ds_user()
    sqllist = sqllist + result

    result= source_conn.get_ds_project()
    sqllist = sqllist + result

    result= source_conn.get_ds_datasource()
    sqllist = sqllist + result
    # cdm工作流
    result = source_conn.get_ds_process_definition(8787915905920)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_relation(8787915905920)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_definition(8787915905920)
    sqllist = sqllist + result

    result = source_conn.get_ds_process_definition_log(8787915905920)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_relation_log(8787915905920)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_definition_log(8787915905920)
    sqllist = sqllist + result

    result= source_conn.get_ds_schedules(8787915905920)
    sqllist = sqllist + result


    # cdm工作流——铺数
    result = source_conn.get_ds_process_definition(9662588592032)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_relation(9662588592032)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_definition(9662588592032)
    sqllist = sqllist + result

    result = source_conn.get_ds_process_definition_log(9662588592032)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_relation_log(9662588592032)
    sqllist = sqllist + result

    result= source_conn.get_ds_process_task_definition_log(9662588592032)
    sqllist = sqllist + result
    source_conn.close()

    return sqllist

if __name__ == '__main__':
    # 将标版ds配置导出 或者直接写入目标数据库
    # 集成环境标版连接配置
    user = 'root'
    password = 'Data123456!'
    host = '192.168.149.144'
    port = 3306
    # # 目标数据库
    target_user = 'root'
    target_password = 'Data123456!'
    target_host = '192.168.149.229'
    target_port = 3306

    # 开发环境配置
    config = {
        'Mysql_User': 'e3plusdev',
        'Mysql_PassWord': '4bueWSh@fzWwWG7v',
        'Mysql_Ip': '192.168.145.171:3306',
        'CK_User': 'e3plus',
        'CK_PassWord': 'nGB1d$c!Q5$g^JX7',
        'CK_Ip': '192.168.149.144:8123',
        # 正则表达式替换  例如：e3plus_support_integrate -> e3plus-support-dev
        'DbName_Re':'e3plus-\\1-dev',
        # 正则表达式替换  例如：e3plus_app_b2b_integrate -> e3plus-app-b2b-dev
        'DbName_Re1': 'e3plus-\\1-\\2-dev',
    }

    # # 洪兴环境配置
    # # 目标数据库
    # target_user = 'baison'
    # target_password = 'ff0NNVWFHTXgEG4Z'
    # target_host = 'pc-wz9jz9s0vzck3178u.rwlb.rds.aliyuncs.com'
    # target_port = 3306
    # config = {
    #     'Mysql_User': 'baison',
    #     'Mysql_PassWord': 'ff0NNVWFHTXgEG4Z',
    #     'Mysql_Ip': 'pc-wz9jz9s0vzck3178u.rwlb.rds.aliyuncs.com:3306',
    #     'CK_User': 'e3plus',
    #     'CK_PassWord': 'nGB1d$c!Q5$g^JX7',
    #     'CK_Ip': '10.0.0.65:8123',
    #     # 正则表达式替换  例如：e3plus_support_integrate -> e3plus-support-dev
    #     'DbName_Re':'e3plus_\\1_integrate',
    #     # 正则表达式替换  例如：e3plus_app_b2b_integrate -> e3plus-app-b2b-dev
    #     'DbName_Re1': 'e3plus_\\1_\\2_integrate',
    # }
    # 集成环境配置
    # config = {
    #     'Mysql_User': 'e3plus',
    #     'Mysql_PassWord': '4bueWSh@fzWwWG7',
    #     'Mysql_Ip': '192.168.145.170:3306',
    #     'CK_User': 'e3plus',
    #     'CK_PassWord': 'nGB1d$c!Q5$g^JX7',
    #     'CK_Ip': '192.168.149.229:8123',
    #     # 正则表达式替换  例如：e3plus-support-integrate-new -> e3plus-support-new-dev
    #     'DbName_Re':'\\1_\\2_integrate',
    # }
    sqllist=main(user, password, host, port)
    try:
        # 如果能直连目标数据库
        target_conn = mysql_conn(target_user, target_password, target_host, target_port)
        for i in sqllist:
            print(sql_parse(config).sql_replace(i.replace('%', '%%')))
            target_conn.execute(sql_parse(config).sql_replace(i.replace('%', '%%')))
        target_conn.close()
        print('标版工作流已成功同步到目标数据库 %s' % target_host)
    except Exception as e:
        print(e)
        # 不能直连目标数据库 导出文件
        resultstr=sql_parse(config).sql_replace('\r\n'.join(sqllist))
        #
        path2 = r'ds_init.sql'
        print('标版工作流无法导入目标数据库 %s ，将写入文件 %s，请手动导入' % (target_host,path2))
        file2 = open(path2, 'w+')
        file2.write(str(resultstr))
        file2.close()



