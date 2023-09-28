# encoding=utf-8

from unit import excute
from unit.excute import excuteFactory

if __name__ == '__main__':
    tablelist = [
        # 'e3plus-support-new-dev.bas_sales_item','e3plus-trade-new-dev.trade_reconciliation_bs_type_detail'
        # 'e3plus-support-new-dev.bas_attribute'
        # 'e3plus-trade-new-dev.trade_goods_size_assort_line',
# 'e3plus-trade-new-dev.trade_purchase_bill',
# 'e3plus-scm-new-dev.scm_goods_size_assort_line',
# 'e3plus-scm-new-dev.scm_consignee_in_bill',
# 'e3plus-stock-new-dev.stk_goods_size_assort_line',
# 'e3plus-stock-new-dev.stk_allocate_out_bill',
# 'e3plus-drp-new-dev.drp_goods_size_assort_line',
# 'e3plus-stock-new-dev.stk_allocate_in_bill'
#         'e3plus-goods-new-dev.gds_goods_size_assort'
#         'e3plus-support-new-dev.bas_address',
#         'e3plus-support-new-dev.bas_address_purpose'
#         'e3plus-support-new-dev.bas_contact'
#         'e3plus-support-new-dev.bas_shop_business_info'
#         'e3plus-scm-new-dev.scm_pur_order_bill',
#         'e3plus-scm-new-dev.scm_pur_order_bill_goods',
#         'e3plus-trade-new-dev.trade_purchase_bill_detail',
#         'e3plus-drp-new-dev.drp_shipment_bill',
#         'e3plus-drp-new-dev.drp_shipment_bill_line',
#         'e3plus-wms-new-dev.wms_stock_pre_in_bill',
#         'e3plus-wms-new-dev.wms_stock_preinbill_goods',
#         'e3plus-scm-new-dev.scm_consignee_in_bill_goods',
#         'e3plus-drp-new-dev.drp_sale_order_bill',
#         'e3plus-drp-new-dev.drp_sale_order_bill_line'
#         'e3plus-support-new-dev.bas_purchasing_meeting'
#         'e3plus-scm-dev.scm_consignee_return_bill',
#         'e3plus-scm-dev.scm_consignee_return_bill_goods'
#         'e3plus-trade-dev.trade_sale_bill',
#         'e3plus-trade-dev.trade_sale_bill_line'
#         'e3plus-order-dev.ord_retail_ord_goods_detail',
#         'e3plus-order-dev.ord_retail_order_bill',
#         'e3plus-app-pos-dev.pos_retail_goods_detail',
#         'e3plus-app-pos-dev.pos_retail_bill',
#         'e3plus-order-dev.ord_retail_return_chasing_gds_de',
#         'e3plus-app-pos-dev.pos_deposit_slip_bill',
#         'e3plus-app-pos-dev.pos_deposit_slip_goods',
#         'e3plus-stock-dev.stk_allocateoutbill_goods',
#         'e3plus-stock-dev.stk_stock_logic_lock_bill_detail',
#         'e3plus-stock-dev.stk_stock_logic_lock_bill'
        'e3plus-wms-dev.wms_stkphy_attradj_bill'

    ]
    connConfig = {
        'sourceType': 'mysql',
        'sourceUser': 'e3plusdev',
        'sourcePassword': '4bueWSh@fzWwWG7v',
        'sourceHost': '192.168.145.171',
        'sourcePort': 3306,
        'targetUser': 'default',
        'targetPassword': 'Data123456!',
        'targetHost': '192.168.149.144',
        'targetTcpPort': 9000,
        'targetJdbcPort': 8123,
        'targetDb': 'cdm',
        'targetPre': 'ods_',
        'etlType': 'datax',
        'token':'0f3345676bd78c376ab94aa035b1b87b',
        'apiHost':'192.168.149.229',
        'apiPort':12345,
        'tenantCode':'root',
        'projectName':'DATAMAX-ETL',
        'processName':'E3plus_CDM工作流1'
    }
    excute=excuteFactory(connConfig)
    jsonContext, sqllist = excute.excute(tablelist)
    # excute.excuteDS(connConfig, jsonContext)
