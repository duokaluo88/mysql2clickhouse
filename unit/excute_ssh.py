#!/usr/local/python/bin/python
# coding=utf-8

from mysql2clickhouse.model import conn_ssh as conn
from mysql2clickhouse.model import parser

from mysql2clickhouse.unit import getMeta_ssh as getMeta
from mysql2clickhouse.unit.dsApi import *


class excuteFactory():
    def __init__(self, connConfig):
        self.sourceType = connConfig['sourceType']
        self.sourceUser = connConfig['sourceUser']
        self.sourcePassword = connConfig['sourcePassword']
        self.sourceHost = connConfig['sourceHost']
        self.sourcePort = connConfig['sourcePort']
        self.targetUser = connConfig['targetUser']
        self.targetPassword = connConfig['targetPassword']
        self.targetHost = connConfig['targetHost']
        self.targetPort = connConfig['targetTcpPort']
        self.targetDb = connConfig['targetDb']
        self.targetPre = connConfig['targetPre']
        self.etlType = connConfig['etlType']
        self.ssh_address_or_host= connConfig['ssh_address_or_host']
        self.ssh_username= connConfig['ssh_username']
        self.ssh_password= connConfig['ssh_password']
        self.remote_bind_address= connConfig['remote_bind_address']

    def clickhouse_excute(self,sqllist):
        ck_conn = conn.clickhouse_conn(self.targetUser, self.targetPassword, self.targetHost, self.targetPort,self.ssh_address_or_host,self.ssh_username,self.ssh_password,self.remote_bind_address)
        for sql in sqllist:
            ck_conn.execute(sql)
        ck_conn.close()

    def excute(self,tablelist):
        sqllist = []
        cloumns_list = []
        json_context = []
        sqlresult = []
        for i in tablelist:
            dbname = i.split('.')[0]
            table = i.split('.')[1]
            targettable = self.targetPre + table

            if self.sourceType == 'mysql':
                sqlresult, cloumns_list = getMeta.mysql_getmetadata(self.sourceUser, self.sourcePassword, self.sourceHost, self.sourcePort, dbname, table, self.etlType,
                                                                    self.targetDb, self.targetPre,self.ssh_address_or_host,self.ssh_username,self.ssh_password,self.remote_bind_address)
            elif self.sourceType == 'sqlserver':
                sqlresult, cloumns_list = getMeta.sqlserver_getmetadata(self.sourceUser, self.sourcePassword, self.sourceHost, self.sourcePort, dbname, table,
                                                                        self.etlType,
                                                                        self.targetDb, self.targetPre)
            if sqlresult:
                sqllist = sqllist + sqlresult
            source_context, target_table, target_cloumns, post_context = parser.join_context(cloumns_list, table,
                                                                                             dbname,
                                                                                             targettable)
            josn_template = parser.josn_replace(parser.josn_read(), source_context, target_table, target_cloumns,
                                                post_context,
                                                dbname, self.targetDb)

            json_context.append({'jobName': targettable, 'JobCode': josn_template})
        if sqllist:
            self.clickhouse_excute(sqllist)

        return json_context, sqllist

    def excuteDS(self,connConfig,jsonContext):
        xAxis = 100
        yAxis = 0
        dsApi = dsApiFactory(connConfig)
        projectCode = dsApi.getProjectCode()
        print(projectCode)
        if dsApi.checkProcessExists(projectCode):
            # 更新流程
            processCode = dsApi.getProcessCode(projectCode)
            print(processCode)
            dsApi.getProcessDefinition(projectCode, processCode)
            xAxis, yAxis = dsApi.parserLocations()
            dsApi.processRelease(projectCode, processCode, 'OFFLINE')
            for i in jsonContext:
                yAxis += 75
                dsApi.createTask(projectCode, i['JobCode'], i['jobName'], xAxis, yAxis)
            dsApi.updateProcess(projectCode, processCode)
            dsApi.processRelease(projectCode, processCode, 'ONLINE')

        else:
            # 创建流程
            for i in jsonContext:
                dsApi.createTask(projectCode, i['JobCode'], i['jobName'], xAxis, yAxis)
                yAxis += 75
            dsApi.createProcess(projectCode)
            processCode = dsApi.getProcessCode(projectCode)
            dsApi.processRelease(projectCode, processCode, 'ONLINE')
            dsApi.createSchedules(projectCode, processCode)



