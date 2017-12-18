[common]

RELATION_VERSIONS = ["20170117", "20170221", "20170315", 
                     "20170417", "20170518", "20170627",
                     "20170720", "20170825", "20170924",
                     "20171018", "20171124"]
#dw.qyxg_wdzj
WDZJ_VERSION_LIST = ['20170427', '20170521', '20170624', 
                     '20170722', '20170828', '20170920',
                     '20171018', '20171124']
#dw.qyxg_exchange
EXCHANGE_VERSION_LIST = ['20170427', '20170521', '20170624',
                         '20170722', '20170828', '20170920',
                         '20171018', '20171124']
#dw.qyxg_jijin_simu
SMJJ_VERSION_LIST = ['20170427', '20170521', '20170624',
                     '20170722', '20170828', '20170920',
                     '20171018', '20171124']


[mysql]

URL = jdbc:mysql://10.28.100.45:3306/ra_two_zhaoyunfeng?characterEncoding=UTF-8
PROP = {"user": "ra_two_zyf", 
        "password":"123456", 
        "driver": "com.mysql.jdbc.Driver",
        "ip": "10.28.100.45",
        "db_name": "ra_two_zhaoyunfeng",
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

#适用于通用模型的部分，红警监控企业类型
#qyxx_tag
TABLE_NAME = dw.qyxx_tag
TAGS_VERSION = 20171124
TYPE_LIST = [u'新兴金融', u'融资担保', u'小额贷款',
             u'私募基金', u'交易场所', u'网络借贷']
#用于LR模型的金融细分行业
TYPE_LR_LIST = [u'新兴金融', u'融资担保', u'小额贷款',
                u'私募基金', u'交易场所', u'网络借贷']
#用通用模型打分的‘新兴金融’行业
TYPE_NF_LIST = [u'新兴金融', u'融资担保', u'小额贷款']
#需要增加的白名单
WHITE_TABLE_NAME = dw.qyxx_tag_white
WHITE_TAGS_VERSION = 20171124 

#需要过滤的数据版本
BLACK_TABLE_NAME = dw.qyxx_tag_black
BLACK_VERSION = 20171124

#输出路径
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/raw/




[common_company_info]

#dw.qyxx_basic
BASIC_VERSION = 20171124
#dw.qyxx_zhuanli
ZHUANLI_VERSION = 20171124
#dw.xgxx_shangbiao
SHANGBIAO_VERSION = 20171124
#dw.domain_name_website_info
DOMAIN_WEBSITE_VERSION = 20171124
#dw.qyxx_bgxx
BGXX_VERSION = 20171124
#dw.recruit
RECRUIT_VERSION = 20171124
#dw.shgy_zhaobjg
ZHAOBIAO_VERSION = 20171124
#dw.shgy_zhongbjg
ZHONGBIAO_VERSION = 20171124 
#dw.ktgg
KTGG_VERSION = 20171124
#dw.zgcpwsw
ZGCPWSW_VERSION = 20171124
#dw.rmfygg
RMFYGG_VERSION = 20171124
#dw.Xzcf
XZCF_VERSION = 20171124
#dw.zhixing
ZHIXING_VERSION = 20171124
#dw.dishonesty
DISHONESTY_VERSION = 20171124
#dw.qyxg_jyyc
JYYC_VERSION = 20171124
#dw.qyxg_circxzcf
CIRCXZCF_VERSION = 20171124
#dw.qyxx_fzjg_extend
FZJG_VERSION = 20171124
#dw.qyxg_leijinrong_blacklist
BLACK_VERSION = 20170901
#dw.qyxx_state_owned_enterprise_background
STATE_OWNED_VERSION = 20171124

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/raw/






[common_company_info_merge]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/tid/
TMP_PATH = /user/antifraud/hongjing2/dataflow/step_one/tmp/






[common_company_feature]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_one/prd/






[ex_company_feature]

#dw.qyxg_exchange
EXCHANGE_VERSION = 20171124






[p2p_company_feature]

#dw.qyxg_platform_data
PLATFORM_VERSION = 20171124
#dw.qyxg_wdzj
WDZJ_VERSION = 20171124






[pe_company_feature]

#dw.qyxg_jijin_simu
SMJJ_VERSION = 20171124






[ex_feature_merge]

#dw.qyxg_ex_member_list
EX_MEMBER_VERSION = 20170423






[feature_merge]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_two/raw/






[risk_score]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_two/prd/






[info_merge]

MAPPING_PATH = /user/antifraud/source/company_county_mapping
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_three/raw/






[feature_tags]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_three/tid/






[feature_preprocessing]

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_two/tid/






[all_company_info]
#风险划分的比例
HIGH_RISK_RATIO = 0.05
MIDDLE_RISK_RATIO = 0.5

OUT_PATH = /user/antifraud/hongjing2/dataflow/step_three/prd/






[ra_area_count]

TMP_PATH = /user/antifraud/hongjing2/dataflow/step_four/tmp








[to_mysql]

IS_INTO_MYSQL = False
OUT_PATH = /user/antifraud/hongjing2/dataflow/step_four/raw












