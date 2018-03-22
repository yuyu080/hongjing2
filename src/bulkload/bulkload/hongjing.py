#!/usr/bin/python
# -*- coding: utf-8 -*-

from util_Log.logger  import logging
from utils.hbase import HbaseUtils
from utils.date import DateUtils

import sys

reload(sys)
sys.setdefaultencoding("utf-8")
logger = logging.getLogger(__name__)


def hongjing_finance_kpi():
    dataPath = '/user/hongjing/jinrong/20170518'
    tabName = 'hongjing_financial_{version}'.format(version='20170518')
    columns = 'HBASE_ROW_KEY,features:1d_bgxx_num,features:1d_black_num,features:1d_common_interests_num,features:1d_dishonesty_num,features:1d_estatus_num,features:1d_gather_place,features:1d_high_risk_num,features:1d_jyyc_num,features:1d_ktgg_num,features:1d_lending_num,features:1d_new_finance_num,features:1d_relation_info,features:1d_rmfygg_num,features:1d_same_address_num,features:1d_sswx_num,features:1d_xzcf_num,features:1d_zgcpwsw_num,features:1d_zhixing_num,features:2d_bgxx_num,features:2d_black_num,features:2d_common_interests_num,features:2d_dishonesty_num,features:2d_estatus_num,features:2d_gather_place,features:2d_high_risk_num,features:2d_jyyc_num,features:2d_ktgg_num,features:2d_lending_num,features:2d_new_finance_num,features:2d_relation_info,features:2d_rmfygg_num,features:2d_same_address_num,features:2d_sswx_num,features:2d_xzcf_num,features:2d_zgcpwsw_num,features:2d_zhixing_num,features:3d_bgxx_num,features:3d_black_num,features:3d_common_interests_num,features:3d_dishonesty_num,features:3d_estatus_num,features:3d_gather_place,features:3d_high_risk_num,features:3d_jyyc_num,features:3d_ktgg_num,features:3d_lending_num,features:3d_new_finance_num,features:3d_relation_info,features:3d_rmfygg_num,features:3d_same_address_num,features:3d_sswx_num,features:3d_xzcf_num,features:3d_zgcpwsw_num,features:3d_zhixing_num,features:bbd_qyxx_id,features:company_name,features:tar_bgxx_fddbr_num,features:tar_bgxx_gd_num,features:tar_bgxx_gg_num,features:tar_bgxx_jyfw_num,features:tar_bgxx_total_num,features:tar_bgxx_zczb_num,features:tar_dishonesty_num,features:tar_jyyc_num,features:tar_ktgg_num,features:tar_lending_num,features:tar_out_drgree,features:tar_rmfygg_num,features:tar_ssws_num,features:tar_xzcf_num,features:tar_zgcpwsw_num,features:tar_zhixing_num'
    HbaseUtils.bulkloadToHbase(dataPath,
                               tabName,
                               columns,
                               'hongjing_financial_{version}.hbaseshell'.format(version='20170518'),
                               '\t')

def hongjing_ra_company():
    dataPath = '/user/hongjing/ra_company/20180227'
    tabName = 'hongjing_ra_company_{version}'.format(version='20180227')
    # columns = 'HBASE_ROW_KEY,info:company,info:risk_index,info:industry,info:register_area,info:risk_level,info:gmt_create,info:reason_create,info:risk_scan'
    columns = 'HBASE_ROW_KEY,info:company,info:risk_index,info:industry,info:register_area,info:risk_level,info:gmt_create,info:risk_scan'

    HbaseUtils.bulkloadToHbase(dataPath,
                               tabName,
                               columns,
                               'hongjing_ra_company_{version}.hbaseshell'.format(version='20180227'),
                               '\t')

if __name__ == "__main__":
    pass