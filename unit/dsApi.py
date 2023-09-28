# encoding=utf-8

import requests
import json


class dsApiFactory():
    def __init__(self, connConfig):
        self.token = connConfig['token']
        self.apiHost = connConfig['apiHost']
        self.apiPort = connConfig['apiPort']
        self.tenantCode = connConfig['tenantCode']
        self.projectName = connConfig['projectName']
        self.processName = connConfig['processName']
        self.apiUrl = 'http://%s:%s/dolphinscheduler' % (self.apiHost, self.apiPort)
        self.headers = {
            'token': self.token,
            'Accept': 'application/json',
            'User-Agent': 'Apifox/1.0.0 (https://www.apifox.cn)',
        }
        self.locations = []
        self.taskDefinitionJson = []
        self.taskRelationJson = []
        # 全局参数
        self.globalParams = [
            {"prop": "Mysql_User", "direct": "IN", "type": "VARCHAR", "value": connConfig['sourceUser']},
            {"prop": "Mysql_PassWord", "direct": "IN", "type": "VARCHAR", "value": connConfig['sourcePassword']},
            {"prop": "Mysql_Ip", "direct": "IN", "type": "VARCHAR",
             "value": "%s:%s" % (connConfig['sourceHost'], connConfig['sourcePort'])},
            {"prop": "CK_User", "direct": "IN", "type": "VARCHAR", "value": connConfig['targetUser']},
            {"prop": "CK_PassWord", "direct": "IN", "type": "VARCHAR", "value": connConfig['targetPassword']},
            {"prop": "CK_Ip", "direct": "IN", "type": "VARCHAR",
             "value": "%s:%s" % (connConfig['targetHost'], connConfig['targetJdbcPort'])},
            {"prop": "bizDate10", "direct": "IN", "type": "VARCHAR", "value": "$[yyyy-MM-dd-1]"},
            {"prop": "bizDate", "direct": "IN", "type": "VARCHAR", "value": "${system.biz.date}"},
            {"prop": "priDate", "direct": "IN", "type": "VARCHAR", "value": "$[yyyyMMdd-2]"}]
        self.schedule={
            "startTime":"2019-06-10 00:00:00",
            "endTime":"2219-06-13 00:00:00",
            "timezoneId":"Asia/Shanghai",
            "crontab":"1 0 0 * * ? *"
        }

    # 检查工作流是否存在
    def checkProcessExists(self, projectCode):
        payload = {
            'name': self.processName
        }
        url = self.apiUrl + "/projects/%s/process-definition/verify-name" % (projectCode)
        response = requests.request("GET", url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['failed']:
                print('检测到项目：%s 工作流：%s 存在，转为更新工作流 ' % (self.projectName, self.processName))
                return True
            else:
                print('检测到项目：%s 工作流：%s 不存在，转为创建工作流 ' % (self.projectName, self.processName))

    # 获取工作流Code
    def getProcessCode(self, projectCode):
        payload = {
            'name': self.processName
        }
        url = self.apiUrl + "/projects/%s/process-definition/query-by-name" % (projectCode)
        response = requests.request("GET", url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                return response.json()['data']['processDefinition']['code']

    # 生成TaskCode
    def getTaskCode(self, projectCode):
        payload = {
            'genNum': 1
        }
        url = self.apiUrl + "/projects/%s/task-definition/gen-task-codes" % (projectCode)
        response = requests.request("GET", url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                return response.json()['data'][0]

    # 创建工作流
    def createProcess(self, projectCode):
        payload = {
            'locations': json.dumps(self.locations),
            'name': self.processName,
            'taskDefinitionJson': json.dumps(self.taskDefinitionJson),
            'taskRelationJson': json.dumps(self.taskRelationJson),
            'tenantCode': self.tenantCode,
            'globalParams': json.dumps(self.globalParams)
        }
        url = self.apiUrl + "/projects/%s/process-definition" % (projectCode)
        response = requests.post(url, headers=self.headers, data=payload)
        if response.status_code == 201:
            if response.json()['success']:
                print('项目：%s 工作流：%s 创建成功 ' % (self.projectName, self.processName))
        else:
            print(response.json())

    # 生成任务所需参数
    def createTask(self, projectCode, dataxTask, taskName, xAxis=100, yAxis=100):
        taskCode = self.getTaskCode(projectCode)
        self.locations.append({"taskCode": taskCode, "x": xAxis, "y": yAxis})
        self.taskDefinitionJson.append({"code": taskCode, "name": taskName, "description": "", "taskType": "DATAX",
                                        "taskParams": {"customConfig": 1, "json": dataxTask, "localParams": [],
                                                       "xms": 1, "xmx": 1,
                                                       "dependence": {"dependTaskList": [], "relation": "AND"},
                                                       "conditionResult": {"successNode": [], "failedNode": []},
                                                       "waitStartTimeout": {}, "switchResult": {}}, "flag": "YES",
                                        "taskPriority": "MEDIUM", "workerGroup": "default", "failRetryTimes": "3",
                                        "failRetryInterval": "5", "timeoutFlag": "CLOSE", "timeoutNotifyStrategy": "",
                                        "timeout": 0, "delayTime": "0", "environmentCode": -1})
        self.taskRelationJson.append(
            {"name": "", "preTaskCode": 0, "preTaskVersion": 0, "postTaskCode": taskCode, "postTaskVersion": 0,
             "conditionType": 0, "conditionParams": {}})

    # 工作流上下线
    def processRelease(self, projectCode, processCode, processStatus):
        # ONLINE:上线,OFFLINE:下线
        payload = {
            'releaseState': processStatus
        }
        url = self.apiUrl + "/projects/%s/process-definition/%s/release" % (projectCode, processCode)
        response = requests.post(url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print('项目：%s 工作流：%s 状态变更为%s ' % (self.projectName, self.processName, processStatus))
                return response.json()['success']

    # 删除工作流
    def deleteProcess(self, projectCode, processCode):
        payload = {
        }
        url = self.apiUrl + "/projects/%s/process-definition/%s" % (projectCode, processCode)
        response = requests.delete(url, headers=self.headers, params=payload)
        print(response.json())
        if response.status_code == 200:
            if response.json()['success']:
                return response.json()['success']

    # 查询工作流定义
    def getProcessDefinition(self, projectCode, processCode):
        payload = {
        }
        url = self.apiUrl + "/projects/%s/process-definition/%s" % (projectCode, processCode)
        response = requests.get(url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print('项目：%s 工作流：%s 正在查询当前定义内容 ' % (self.projectName, self.processName))
                self.parserProcessDefinition(response.json()['data'])

    # 解析 工作流定义的参数
    def parserProcessDefinition(self, processDefinitionData):
        self.locations = json.loads(processDefinitionData['processDefinition']['locations'])
        self.globalParams = json.loads(processDefinitionData['processDefinition']['globalParams'])
        self.processName = processDefinitionData['processDefinition']['name']
        self.processCode = processDefinitionData['processDefinition']['code']
        self.projectCode = processDefinitionData['processDefinition']['projectCode']
        self.taskDefinitionJson = processDefinitionData['taskDefinitionList']
        self.taskRelationJson = processDefinitionData['processTaskRelationList']
        self.tenantCode = processDefinitionData['processDefinition']['tenantCode']
        self.description = processDefinitionData['processDefinition']['tenantCode']
        self.releaseState = processDefinitionData['processDefinition']['releaseState']
        self.timeout = processDefinitionData['processDefinition']['timeout']

    # 解析locations坐标参数
    def parserLocations(self):
        x = []
        y = []
        for i in self.locations:

            x.append(i['x'])
            y.append(i['y'])
        return min(x), max(y)

    # 更新工作流
    def updateProcess(self, projectCode, processCode):
        payload = {
            'locations': json.dumps(self.locations),
            'name': self.processName,
            'projectCode': projectCode,
            'taskDefinitionJson': json.dumps(self.taskDefinitionJson),
            'taskRelationJson': json.dumps(self.taskRelationJson),
            'tenantCode': self.tenantCode,
            'description': self.description,
            'globalParams': json.dumps(self.globalParams),
            'releaseState': self.releaseState,
            'timeout': self.timeout
        }
        url = self.apiUrl + "/projects/%s/process-definition/%s" % (projectCode, processCode)
        response = requests.put(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print('项目：%s 工作流：%s 更新成功 ' % (self.projectName, self.processName))
        else:
            print(response.json())

    # 获取TASK定义
    def getTaskDefinition(self, projectCode, taskCode):
        payload = {
        }
        url = self.apiUrl + "/projects/%s/task-definition/%s" % (projectCode, taskCode)
        response = requests.get(url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                return response.json()['data']
        else:
            print(response.json())

    # 更新Task
    def updateTaskDefinition(self, projectCode, taskCode, taskDefinitionJsonObj):
        payload = {
            'taskDefinitionJsonObj': json.dumps(taskDefinitionJsonObj)
        }
        url = self.apiUrl + "/projects/%s/task-definition/%s" % (projectCode, taskCode)
        response = requests.put(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print(response.json()['success'])
        else:
            print(response.json())

    # 删除任务
    def deleteTask(self, projectCode, taskCode):
        payload = {
        }
        url = self.apiUrl + "/projects/%s/task-definition/%s" % (projectCode, taskCode)
        response = requests.delete(url, headers=self.headers, data=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print(response.json()['success'])
        else:
            print(response.json())

    # 获取项目Code
    def getProjectCode(self):
        payload = {
            'pageNo': 1,
            'pageSize': 10,
            'searchVal': self.projectName
        }
        url = self.apiUrl + "/projects"
        response = requests.get(url, headers=self.headers, params=payload)
        if response.status_code == 200:
            if response.json()['success']:
                print(response.json())
                return response.json()['data']['totalList'][0]['code']
        else:
            print(response.json())

    # 创建定时任务
    def createSchedules(self,projectCode,processCode):
        payload = {
            'processDefinitionCode': processCode,
            'projectCode':projectCode,
            'failureStrategy':'CONTINUE',
            'processInstancePriority':'MEDIUM',
            'schedule':json.dumps(self.schedule)
        }
        url = self.apiUrl + "/projects/%s/schedules" % projectCode
        response = requests.post(url, headers=self.headers, params=payload)
        if response.status_code == 201:
            if response.json()['success']:
                print('项目：%s 工作流：%s 创建定时任务成功 ' % (self.projectName, self.processName))
                return response.json()['success']



