# encoding=utf-8

from unit import excute
from unit.excute import excuteFactory

if __name__ == '__main__':
    tablelist = [
        # 'e3plus-support-integrate-new.bas_discount',
        # 'e3plus-price-integrate-new.mkt_sale_goods_detail',
        # 'e3plus-price-integrate-new.mkt_sale_customer_detail'
        # 'e3plus-support-integrate-new.bas_sales_item'
        # , 'e3plus-trade-integrate-new.trade_reconciliation_bs_type_detail'
        # 'e3plus-support-integrate-new.bas_attribute'
        # 'e3plus-goods-integrate-new.gds_attribute_group'
        # 'e3plus-goods-integrate-new.gds_attribute'
        # 'e3plus-trade-integrate-new.trade_goods_size_assort_line',
        # 'e3plus-trade-integrate-new.trade_purchase_bill',
        # 'e3plus-scm-integrate-new.scm_goods_size_assort_line',
        # 'e3plus-scm-integrate-new.scm_consignee_in_bill',
        # 'e3plus-stock-integrate-new.stk_goods_size_assort_line',
        # 'e3plus-stock-integrate-new.stk_allocate_out_bill',
        # 'e3plus-drp-integrate-new.drp_goods_size_assort_line',
        # 'e3plus-stock-integrate-new.stk_allocate_in_bill'
        # 'e3plus-goods-integrate-new.gds_goods_size_assort'
        # 'e3plus-support-integrate-new.bas_address',
        # 'e3plus-support-integrate-new.bas_address_purpose'
        # 'e3plus-support-integrate-new.bas_contact'
        # 'e3plus-support-integrate-new.bas_shop_business_info'
        # 'e3plus-scm-integrate-new.scm_pur_order_bill',
        # 'e3plus-scm-integrate-new.scm_pur_order_bill_goods',
        # 'e3plus-trade-integrate-new.trade_purchase_bill_detail',
        # 'e3plus-drp-integrate-new.drp_shipment_bill',
        # 'e3plus-drp-integrate-new.drp_shipment_bill_line',
        # 'e3plus-wms-integrate-new.wms_stock_pre_in_bill',
        # 'e3plus-wms-integrate-new.wms_stock_preinbill_goods',
        # 'e3plus-scm-integrate-new.scm_consignee_in_bill_goods',
        # 'e3plus-drp-integrate-new.drp_sale_order_bill',
        # 'e3plus-drp-integrate-new.drp_sale_order_bill_line'
        # 'e3plus_support_integrate.bas_purchasing_meeting'
        # 'e3plus_order_integrate.ord_retail_ord_goods_detail',
        # 'e3plus_order_integrate.ord_retail_order_bill',
        # 'e3plus_app_pos_integrate.pos_retail_goods_detail',
        # 'e3plus_app_pos_integrate.pos_retail_bill',
        # 'e3plus_order_integrate.ord_retail_return_chasing_gds_de',
        # 'e3plus_app_pos_integrate.pos_deposit_slip_bill',
        # 'e3plus_app_pos_integrate.pos_deposit_slip_goods',
        # 'e3plus_stock_integrate.stk_allocateoutbill_goods',
        # 'e3plus_stock_integrate.stk_stock_logic_lock_bill_detail',
        # 'e3plus_stock_integrate.stk_stock_logic_lock_bill'
        'e3plus_wms_integrate.wms_stkphy_attradj_bill'


    ]
    connConfig = {
        'sourceType': 'mysql',
        'sourceUser': 'e3plus',
        'sourcePassword': '4bueWSh@fzWwWG7',
        'sourceHost': '192.168.145.170',
        'sourcePort': 3306,
        'targetUser': 'default',
        'targetPassword': 'Data123456!',
        'targetHost': '192.168.149.229',
        'targetTcpPort': 9000,
        'targetJdbcPort': 8123,
        'targetDb': 'cdm',
        'targetPre': 'ods_',
        'etlType': 'datax',
        'token':'591e9d0b90e0a3e9fa1d1af6b42883a6',
        'apiHost':'192.168.149.144',
        'apiPort':12345,
        'tenantCode':'root',
        'projectName':'DATAMAX-ETL',
        'processName':'E3plus_CDM工作流123'
    }
    excute=excuteFactory(connConfig)
    jsonContext, sqllist = excute.excute(tablelist)
    excute.excuteDS(connConfig, jsonContext)
