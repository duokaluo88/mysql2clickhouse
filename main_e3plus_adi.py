# encoding=utf-8


from unit.excute_ssh import excuteFactory

if __name__ == '__main__':
    # 京东
    # tablelist = [
    #     'e3plus-adaptor.jingdong_step_trade',
    #     'e3plus-adaptor.sys_std_platform_sku',
    #     'e3plus-adaptor.sys_std_trade',
    #     'e3plus-adaptor.sys_std_trade_item',
    #     'e3plus-adaptor.jingdong_order_split_amount',
    #     'e3plus-adaptor.sys_std_trade_promotion',
    #     'e3plus-adaptor.sku_inventory_api_log',
    #     'e3plus-adaptor.hub_api_log'
    # ]
    # connConfig = {
    #     'sourceType': 'mysql',
    #     'sourceUser': 'hbyd20230329',
    #     'sourcePassword': 'Pj7s70XHuL8e',
    #     'sourceHost': '127.0.0.1',
    #     'sourcePort': 33062,
    #     'targetUser': 'default',
    #     'targetPassword': 'Data123456!',
    #     'targetHost': '120.55.170.92',
    #     'targetTcpPort': 9000,
    #     'targetJdbcPort': 8123,
    #     'targetDb': 'cdm',
    #     'targetPre': 'ods_jd_',
    #     'etlType': 'flink',
    #     'token':'7bb1c3a0f27379bc77a74ef0a0f49bc4',
    #     'apiHost':'120.55.170.92',
    #     'apiPort':12345,
    #     'tenantCode':'root',
    #     'projectName':'DATAMAX-ETL',
    #     'processName':'CDM工作流_JD'
    #
    # }

    # 天猫
    tablelist = [
        # 'e3plus-adaptor.sys_std_trade',
        # 'e3plus-adaptor.sys_std_trade_item',
        # 'e3plus-adaptor.sys_std_trade_promotion',
        # 'e3plus-adaptor.sku_inventory_platform_log',
        # 'e3plus-adaptor.hub_api_log',
        'e3plus-adaptor.sku_inventory_api_log'
    ]
    connConfig = {
        'sourceType': 'mysql',
        'sourceUser': 'myuser',
        'sourcePassword': 'sw9lF00M4CeO',
        'sourceHost': '127.0.0.1',
        'sourcePort': 33061,
        'targetUser': 'default',
        'targetPassword': 'Data123456!',
        'targetHost': '120.55.170.92',
        'targetTcpPort': 9000,
        'targetJdbcPort': 8123,
        'targetDb': 'cdm',
        'targetPre': 'ods_tm_',
        'etlType': 'flink',
        'token':'7bb1c3a0f27379bc77a74ef0a0f49bc4',
        'apiHost':'120.55.170.92',
        'apiPort':12345,
        'tenantCode':'root',
        'projectName':'DATAMAX-ETL',
        'processName':'CDM工作流_TM'

    }
    excute=excuteFactory(connConfig)
    jsonContext, sqllist = excute.excute(tablelist)
    excute.excuteDS(connConfig, jsonContext)
