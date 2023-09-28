# encoding=utf-8


from unit.excute_ssh import excuteFactory

if __name__ == '__main__':


#     tablelist = [
#         # 'e3plus-adaptor.sys_std_trade',
#         # 'e3plus-adaptor.sys_std_trade_item',
#         # 'e3plus-adaptor.sys_std_trade_promotion',
#         # 'e3plus-adaptor.sku_inventory_platform_log',
#         # 'e3plus-adaptor.hub_api_log',
#         'e3.kehu',
#         'e3.cangku',
# 'e3.order_goods',
# 'e3.order_info',
# 'e3.api_ec_log',
# 'e3.region',
# 'e3.spkcb',
# 'e3.goods',
# 'e3.brand',
# 'e3.staff',
# 'e3.order_return',
# 'e3.taobao_trade',
# 'e3.douyinxiaodian_trade',
# 'e3.lazada_trade'
# #         'e3.platform'
#
#     ]
#     connConfig = {
#         'sourceType': 'mysql',
#         'sourceUser': 'root',
#         'sourcePassword': 'Data123456!',
#         'sourceHost': '127.0.0.1',
#         'sourcePort': 3306,
#         'targetUser': 'default',
#         'targetPassword': 'Data123456!',
#         'targetHost': '127.0.0.1',
#         'targetTcpPort': 9000,
#         'targetJdbcPort': 8123,
#         'targetDb': 'cdm_1',
#         'targetPre': 'ods_',
#         'etlType': 'flink',
#         'token':'1d978ad1ef8f3bce0d6875d69fa11bb1',
#         'apiHost':'39.98.108.60',
#         'apiPort':12345,
#         'tenantCode':'root',
#         'projectName':'DATAMAX-ETL_1',
#         'processName':'CDM工作流',
#         'ssh_address_or_host': '39.98.108.60',
#         'ssh_username': 'root',
#         'ssh_password': '2023<nu?hBsYJs`)S&R^Iq028',
#         'remote_bind_address': '172.28.2.171'
#
#     }

    # 运营报表
    tablelist = [
        # 'e3plus-adaptor.sys_std_trade',
        # 'e3plus-adaptor.sys_std_trade_item',
        # 'e3plus-adaptor.sys_std_trade_promotion',
        # 'e3plus-adaptor.sku_inventory_platform_log',
        # 'e3plus-adaptor.hub_api_log',
        'monitor.monitor_user',
'monitor.monitor_node',
'monitor.monitor_service',
'monitor.monitor_alarm',
'monitor.monitor_oms_interface',
'monitor.monitor_data_comparison',
'monitor.monitor_course',
'monitor.monitor_alarm_statistics',
        'monitor.monitor_service_cate'


    ]
    connConfig = {
        'sourceType': 'mysql',
        'sourceUser': 'root',
        'sourcePassword': 'Data123456!',
        'sourceHost': '127.0.0.1',
        'sourcePort': 3306,
        'targetUser': 'default',
        'targetPassword': 'Data123456!',
        'targetHost': '127.0.0.1',
        'targetTcpPort': 9000,
        'targetJdbcPort': 8123,
        'targetDb': 'cdm_2',
        'targetPre': 'ods_',
        'etlType': 'datax',
        'token':'1d978ad1ef8f3bce0d6875d69fa11bb1',
        'apiHost':'39.98.108.60',
        'apiPort':12345,
        'tenantCode':'root',
        'projectName':'DATAMAX-ETL_2',
        'processName':'CDM工作流',
        'ssh_address_or_host': '39.98.108.60',
        'ssh_username': 'root',
        'ssh_password': '2023<nu?hBsYJs`)S&R^Iq028',
        'remote_bind_address': '172.28.2.171'

    }
    excute=excuteFactory(connConfig)
    jsonContext, sqllist = excute.excute(tablelist)
    excute.excuteDS(connConfig, jsonContext)

