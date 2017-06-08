[common]

RELATION_VERSIONS = ["20170117", "20170221", "20170315", "20170417", "20170518"]



[mysql]

URL = jdbc:mysql://10.10.20.180:3306/airflow?characterEncoding=UTF-8
PROP = {"user": "airflow", 
        "password":"airflow", 
        "driver": "com.mysql.jdbc.Driver",
        "ip": "10.10.20.180",
        "db_name": "airflow",
        "port": "3306"}



[work_flow]

#代码根目录
IN_PATH = /data5/antifraud/Hongjing2/src/





[input_sample_data]

#权重分布，值越大越重要
WEIGHT_DICT = {
    u'新兴金融': 1,
    u'交易场所': 2,
    u'私募基金': 3,
    u'网络借贷': 4,
    u'融资担保': 5,
    u'小额贷款': 6}

#适用于通用模型的部分
#dw.qyxx_tags 
TAGS_VERSION = 20170523
TYPE_LIST = [u'新兴金融', u'融资担保', u'小额贷款']

#网络借贷P2P
#dw.qyxg_platform_data
#dw.qyxg_wdzj
PLATFORM_VERSION = 20170521
WDZJ_VERSION = 20170521

#私募基金
#dw.qyxg_jijin_simu
SMJJ_VERSION = 20170521  

#交易场所
#dw.qyxg_exchange
EXCHANGE_VERSION = 20170521

#需要过滤的数据版本
FILTER_VERSION = 20170531

#输出路径
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/raw/




[common_company_info]

#dw.qyxx_basic
BASIC_VERSION = 20170521
#dw.qyxx_zhuanli
ZHUANLI_VERSION = 20170521
#dw.xgxx_shangbiao
SHANGBIAO_VERSION = 20170521
#dw.domain_name_website_info
DOMAIN_WEBSITE_VERSION = 20170521
#dw.qyxx_bgxx
BGXX_VERSION = 20170521
#dw.recruit
RECRUIT_VERSION = 20170521
#dw.shgy_zhaobjg
ZHAOBIAO_VERSION = 20170521
#dw.shgy_zhongbjg
ZHONGBIAO_VERSION = 20170521 
#dw.ktgg
KTGG_VERSION = 20170521
#dw.zgcpwsw
ZGCPWSW_VERSION = 20170521
#dw.rmfygg
RMFYGG_VERSION = 20170521
#dw.Xzcf
XZCF_VERSION = 20170521
#dw.zhixing
ZHIXING_VERSION = 20170521
#dw.dishonesty
DISHONESTY_VERSION = 20170504
#dw.qyxg_jyyc
JYYC_VERSION = 20170504
#dw.qyxg_circxzcf
CIRCXZCF_VERSION = 20170521
#dw.qyxx_fzjg_extend
FZJG_VERSION = 20170521
#dw.qyxg_leijinrong_blacklist
BLACK_VERSION = 20170602

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/raw/





[common_company_info_merge]

IN_PATH = /user/antifraud/hongjing2/dataflow/step_one/raw/
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/tid/
TMP_PATH = /user/antifraud/hongjing2/dataflow/step_one/tmp/




[ex_company_feature]

#dw.qyxg_exchange
EXCHANGE_VERSION = 20170521
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/prd/







[p2p_company_feature]

#dw.qyxg_platform_data
PLATFORM_VERSION = 20170521
#dw.qyxg_wdzj
WDZJ_VERSION = 20170521
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/prd/








[pe_company_feature]

#dw.qyxg_jijin_simu
SMJJ_VERSION = 20170521
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/prd/








[ex_feature_merge]

#dw.qyxg_ex_member_list
EX_MEMBER_VERSION = 20170423
IN_PAHT = /user/antifraud/hongjing2/dataflow/step_one/prd/
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_two/raw/







[all_company_info]
#风险划分的比例
HIGH_RISK_RATIO = 0.05
MIDDLE_RISK_RATIO = 0.5