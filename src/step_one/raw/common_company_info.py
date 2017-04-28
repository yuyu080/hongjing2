# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
common_company_info.py

'''

import configparser
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp
from pyspark.sql import Row
from pyspark.sql import SparkSession
import os


def add_col(one_col, two_col):
    '''
    将2个col合成一个元祖
    '''
    return (one_col, two_col)


def to_dict(col):
    '''
    转换成字典
    '''
    return dict(col)


#投资方、被投资方经营状态是否为吊销
def is_not_revoked(col):
    try:
        if u'吊销' not in col and u'注销' not in col:
            return 0
        else:
            return 1
    except:
        return 1

def has_keyword(opescope):
    '''
    关键字匹配
    '''
    keywords_list_1 = [
        u'民间借贷', u'民间融资', u'资产管理', u'养老', u'艺术品', u'生态农业',
        u'养生', u'新能源', u'生物科技', u'环保科技', u'资产管理', u'投资管理', 
        u'基金管理', u'支付业务', u'互联网支付', u'电子支付', u'货币兑换', 
        u'移动电话支付', u'银行卡收单', u'预付卡受理', u'金融信息服务', 
        u'电子商务', u'投资咨询', u'投资管理', u'基金募集', u'基金销售',
        u'资产管理', u'征信', u'经济贸易咨询', u'财务咨询', u'风险投资', 
        u'资产经营', u'众筹', u'财富管理',  u'纳米']
    keywords_list_2 = [
        u'投资', u'咨询', u'贸易', u'租赁', u'保理', u'交易场所', u'小额贷款', 
        u'担保', u'金融信息服务', u'网络科技', u'信息科技', u'信息技术']

    keyword_dict = dict(
        zip(keywords_list_1, len(keywords_list_1) * ['k_1']) + 
        zip(keywords_list_1, len(keywords_list_2) * ['k_2']))
    
    if opescope is not None:
        for each_keyword, keyword_type in keyword_dict.iteritems():
            if each_keyword in opescope:
                return keyword_type
        return 'k_0'
    else:
        return 'k_0'


def get_company_namefrag(iterator):
    '''
    构建DAG；这里因为涉及到加载词典，只能用mapPartition，不然IO开销太大
    '''
    try:
        from dafei_keyword import KeywordExtr
        _obj = KeywordExtr("city", "1gram.words", "2gram.words", "new.work.words")
        keyword_list = []
        for row in iterator:
            keyword_list.append((row.company_name, _obj.clean(row.company_name)))
        return keyword_list
    except Exception, e:
        return e

def get_spark_session():
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 15)
    conf.set("spark.executor.memory", "20g")
    conf.set("spark.executor.instances", 30)
    conf.set("spark.executor.cores", 5)
    conf.set("spark.python.worker.memory", "3g")
    conf.set("spark.default.parallelism", 600)
    conf.set("spark.sql.shuffle.partitions", 600)
    conf.set("spark.broadcast.blockSize", 1024)
    conf.set("spark.executor.extraJavaOptions",
             "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")    
    conf.set("spark.submit.pyFiles", 
             "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/dafei_keyword.py")
    conf.set("spark.files", 
                ("hdfs://bbdc6ha/user/antifraud/source/keyword_demo/city,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/1gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/2gram.words,"
                 "hdfs://bbdc6ha/user/antifraud/source/keyword_demo/new.work.words"))
    
    spark = SparkSession \
        .builder \
        .appName("hongjing2_one_raw_common") \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()    
    
    return spark
        
def run():
    #注册udf
    to_dict_udf = fun.udf(to_dict, tp.MapType(tp.StringType(), tp.StringType()))
    add_col_udf = fun.udf(add_col, tp.ArrayType(tp.StringType()))
    is_not_revoked_udf = fun.udf(is_not_revoked, tp.IntegerType())
    has_keyword_udf = fun.udf(has_keyword, tp.StringType())

    #原始样本
    sample_df = spark.sql(
        '''
        SELECT
        company company_name
        FROM
        hongjing.raw_company_namefrag
        WHERE
        dt='20170320_quanguo'
        '''
    )
    os.system("hadoop fs -rmr {path}/ljr_sample/{version}".format(version=LEIJINRONG_VERSION,
                                                                  path=OUT_PATH))
    sample_df.repartition(10).write.parquet(
        "{path}/ljr_sample/{version}".format(version=LEIJINRONG_VERSION,
                                             path=OUT_PATH))
    
    #国企列表
    url = "jdbc:mysql://10.10.10.12:3306/bbd_higgs?characterEncoding=UTF-8"
    prop = {"user": "reader", "password":"Hkjhsdwe35", 
            "driver": "com.mysql.jdbc.Driver"}
    table = "qyxx_state_owned_enterprise_background"
    so_df = spark.read.jdbc(url=url, table=table, properties=prop)
    os.system("hadoop fs -rmr \
        {path}/qyxx_state_owned_enterprise_background/{version}".format(version=RELATION_VERSION,
                                                                        path=OUT_PATH))
    so_df.write.parquet(
        "{path}/qyxx_state_owned_enterprise_background/{version}".format(version=RELATION_VERSION,
                                                                         path=OUT_PATH))
    
    #基础工商信息
    basic_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        ipo_company,
        regcap_amount,
        realcap_amount,
        esdate,
        operate_scope,
        trim(address) address,
        enterprise_status,
        company_province,
        company_county
        FROM
        dw.qyxx_basic
        WHERE
        dt='{version}'  
        '''.format(version=BASIC_VERSION)
    ).select(
        'bbd_qyxx_id',
        'company_name',
        'ipo_company',
        'regcap_amount',
        'realcap_amount',
        'esdate',
        has_keyword_udf('operate_scope').alias('operate_scope'),
        'address',
        is_not_revoked_udf('enterprise_status').alias('enterprise_status'),
        'company_province',
        'company_county'
    ).cache()
    os.system("hadoop fs -rmr {path}/basic/{version}".format(version=RELATION_VERSION,
                                                             path=OUT_PATH))
    basic_df.repartition(10).write.parquet(
        "{path}/basic/{version}".format(version=RELATION_VERSION,
                                        path=OUT_PATH))
    
    #专利信息
    zhuanli_count_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        count(*) zhuanli_num
        FROM
        dw.qyxx_zhuanli
        WHERE
        dt='{version}' 
        AND
        publidate <= '{relation_version}'
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=ZHUANLI_VERSION,
                   relation_version=RELATION_VERSION)
    )
    os.system(
        "hadoop fs -rmr {path}/zhuanli/{version}".format(version=RELATION_VERSION, 
                                                         path=OUT_PATH))
    zhuanli_count_df.repartition(10).write.parquet(
        "{path}/zhuanli/{version}".format(version=RELATION_VERSION, 
                                          path=OUT_PATH))
    
    #商标信息
    shangbiao_count_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        count(*) shangbiao_num
        FROM
        dw.xgxx_shangbiao
        WHERE
        dt='{version}' 
        GROUP BY 
        bbd_qyxx_id
        '''.format(version=SHANGBIAO_VERSION)
    )
    os.system(
        "hadoop fs -rmr {path}/shangbiao/{version}".format(version=RELATION_VERSION, 
                                                           path=OUT_PATH))
    shangbiao_count_df.repartition(10).write.parquet(
        "{path}/shangbiao/{version}".format(version=RELATION_VERSION, 
                                            path=OUT_PATH))
    
    #域名与网址
    domain_website_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        homepage_url,
        domain_name, 
        record_license
        FROM
        dw.domain_name_website_info
        WHERE
        dt='{version}' 
        '''.format(version=DOMAIN_WEBSITE_VERSION)
    )
    os.system(
        "hadoop fs -rmr {path}/domain_website/{version}".format(version=RELATION_VERSION, 
                                                                path=OUT_PATH))
    domain_website_df.repartition(10).write.parquet(
        "{path}/domain_website/{version}".format(version=RELATION_VERSION, 
                                                 path=OUT_PATH))
    
    
    #变更信息
    bgxx_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        change_items,
        count(*) change_num
        FROM
        dw.qyxx_bgxx
        WHERE
        dt='{version}' 
        AND
        change_date <= '{relation_version}'
        AND
        (change_items like '%高管%'  OR change_items like '%法定代表人%'  OR 
        change_items like '%股东%'  OR change_items like '%注册资本%'  OR
        change_items like '%经营范围%' )
        GROUP BY 
        bbd_qyxx_id, change_items
        '''.format(version=BGXX_VERSION,
                   relation_version=RELATION_VERSION)
    )
    bgxx_df = bgxx_df.withColumn('tid_tuple', add_col_udf('change_items', 
                                                          'change_num')) \
        .groupBy('bbd_qyxx_id') \
        .agg({'tid_tuple': 'collect_list'}) \
        .withColumnRenamed('collect_list(tid_tuple)', 'tid_list') \
        .withColumn('bgxx_dict', to_dict_udf('tid_list')) \
        .select('bbd_qyxx_id', 'bgxx_dict')
    os.system(
        "hadoop fs -rmr {path}/bgxx/{version}".format(version=RELATION_VERSION, 
                                                      path=OUT_PATH))
    bgxx_df.repartition(10).write.parquet(
        "{path}/bgxx/{version}".format(version=RELATION_VERSION, 
                                       path=OUT_PATH))
    
    #招聘信息
    recruit_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        sum(bbd_recruit_num) bbd_recruit_num,
        education_required
        FROM
        dw.recruit
        WHERE
        dt='{version}' 
        AND
        pubdate <= '{relation_version}'
        GROUP BY
        bbd_qyxx_id, education_required
        '''.format(version=RECRUIT_VERSION,
                   relation_version=RELATION_VERSION)
    )
    recruit_df = recruit_df \
        .withColumn('tid_tuple', 
                    add_col_udf('education_required', 'bbd_recruit_num')) \
        .groupBy('bbd_qyxx_id') \
        .agg({'tid_tuple': 'collect_list'}) \
        .withColumnRenamed('collect_list(tid_tuple)', 'tid_list') \
        .withColumn('recruit_dict', to_dict_udf('tid_list')) \
        .select('bbd_qyxx_id', 'recruit_dict')
    os.system(
        "hadoop fs -rmr {path}/recruit/{version}".format(version=RELATION_VERSION, 
                                                         path=OUT_PATH))
    recruit_df.repartition(10).write.parquet(
        "{path}/recruit/{version}".format(version=RELATION_VERSION, 
                                          path=OUT_PATH))
    
    #招标信息
    zhaobiao_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        pubdate
        FROM
        dw.shgy_zhaobjg
        WHERE
        dt='{version}' 
        AND
        pubdate <= '{relation_version}'
        '''.format(version=ZHAOBIAO_VERSION,
                   relation_version=RELATION_VERSION)
    )
    zhaobiao_count_df = zhaobiao_df.where(
        fun.date_add('pubdate', 365) > fun.current_date()) \
    .where(zhaobiao_df.bbd_qyxx_id.isNotNull()) \
    .groupBy('bbd_qyxx_id') \
    .count() \
    .withColumnRenamed('count', 'zhaobiao_num')
    os.system(
        "hadoop fs -rmr {path}/zhaobiao/{version}".format(version=RELATION_VERSION, 
                                                          path=OUT_PATH))
    zhaobiao_count_df.repartition(10).write.parquet(
        "{path}/zhaobiao/{version}".format(version=RELATION_VERSION, 
                                           path=OUT_PATH))
    
    #中标信息
    zhongbiao_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        pubdate
        FROM
        dw.shgy_zhongbjg
        WHERE
        dt='{version}' 
        AND
        pubdate <= '{relation_version}'
        '''.format(version=ZHONGBIAO_VERSION,
                   relation_version=RELATION_VERSION)
    )
    zhongbiao_count_df = zhongbiao_df.where(
            fun.date_add('pubdate', 365) > fun.current_date()) \
        .where(zhongbiao_df.bbd_qyxx_id.isNotNull()) \
        .groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'zhongbiao_num')
    os.system(
        "hadoop fs -rmr {path}/zhongbiao/{version}".format(version=RELATION_VERSION, 
                                                           path=OUT_PATH))
    zhongbiao_count_df.repartition(10).write.parquet(
        "{path}/zhongbiao/{version}".format(version=RELATION_VERSION, 
                                            path=OUT_PATH))
    
    #开庭公告
    ktgg_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        action_cause
        FROM
        dw.ktgg
        WHERE
        dt='{version}'
        AND
        bbd_qyxx_id is not null
        AND
        action_cause != 'NULL'
        AND
        trial_date <= '{relation_version}'
        '''.format(version=KTGG_VERSION,
                   relation_version=RELATION_VERSION)
    )
    ktgg_count_df = ktgg_df.groupBy('bbd_qyxx_id')\
        .count() \
        .withColumnRenamed('count', 'ktgg_num')
    os.system(
        "hadoop fs -rmr {path}/ktgg/{version}".format(version=RELATION_VERSION, 
                                                      path=OUT_PATH))
    ktgg_count_df.repartition(10).write.parquet(
        "{path}/ktgg/{version}".format(version=RELATION_VERSION, 
                                       path=OUT_PATH))
    
    
    #裁判文书
    zgcpwsw_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        action_cause
        FROM
        dw.zgcpwsw
        WHERE
        dt='{version}'
        AND
        bbd_qyxx_id is not null
        AND
        action_cause != 'NULL'
        AND
        sentence_date <= '{relation_version}'
        '''.format(version=ZGCPWSW_VERSION,
                   relation_version=RELATION_VERSION)
    )
    zgcpwsw_count_df = zgcpwsw_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'zgcpwsw_num')
    os.system(
        "hadoop fs -rmr {path}/zgcpwsw/{version}".format(version=RELATION_VERSION, 
                                                         path=OUT_PATH))
    zgcpwsw_count_df.repartition(10).write.parquet(
        "{path}/zgcpwsw/{version}".format(version=RELATION_VERSION, 
                                          path=OUT_PATH))
    
    
    #法院公告
    rmfygg_df = spark.sql(
        '''
        SELECT 
        bbd_qyxx_id,
        '' action_cause
        FROM
        dw.rmfygg
        WHERE
        dt='{version}'
        AND
        bbd_qyxx_id is not null
        AND
        notice_time <= '{relation_version}'
        '''.format(version=RMFYGG_VERSION,
                   relation_version=RELATION_VERSION)
    )
    rmfygg_count_df = rmfygg_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'rmfygg_num')
    os.system(
        "hadoop fs -rmr {path}/rmfygg/{version}".format(version=RELATION_VERSION, 
                                                        path=OUT_PATH))
    rmfygg_count_df.repartition(10).write.parquet(
        "{path}/rmfygg/{version}".format(version=RELATION_VERSION, 
                                         path=OUT_PATH))
    
    
    #民间借贷
    def filter_machine(col):
        if u'民间借贷' in col:
            return True
        else:
            return False
    filter_machine_udf = fun.udf(filter_machine, tp.BooleanType())
    lawsuit_df = ktgg_df.union(zgcpwsw_df).union(rmfygg_df)
    lawsuit_count_df = lawsuit_df.where(filter_machine_udf('action_cause')) \
        .groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'lawsuit_num')
    os.system(
        "hadoop fs -rmr {path}/lawsuit/{version}".format(version=RELATION_VERSION, 
                                                         path=OUT_PATH))
    lawsuit_count_df.repartition(10).write.parquet(
        "{path}/lawsuit/{version}".format(version=RELATION_VERSION, 
                                          path=OUT_PATH))
    
    
    #行政处罚
    xzcf_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.Xzcf
        WHERE
        dt='{version}'
        AND
        public_date <= '{relation_version}'
        '''.format(version=XZCF_VERSION,
                   relation_version=RELATION_VERSION)
    )
    xzcf_count_df = xzcf_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'xzcf_num')
    os.system(
        "hadoop fs -rmr {path}/xzcf/{version}".format(version=RELATION_VERSION, 
                                                      path=OUT_PATH))
    xzcf_count_df.repartition(10).write.parquet(
        "{path}/xzcf/{version}".format(version=RELATION_VERSION, 
                                       path=OUT_PATH))
    
    
    #被执行
    zhixing_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.zhixing
        WHERE
        dt='{version}'
        AND
        case_create_time <= '{relation_version}'
        '''.format(version=ZHIXING_VERSION,
                   relation_version=RELATION_VERSION)
    )
    zhixing_count_df = zhixing_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'zhixing_num')
    os.system(
        "hadoop fs -rmr {path}/zhixing/{version}".format(version=RELATION_VERSION, 
                                                         path=OUT_PATH))
    zhixing_count_df.repartition(10).write.parquet(
        "{path}/zhixing/{version}".format(version=RELATION_VERSION, 
                                          path=OUT_PATH))
    
    
    #失信被执行
    dishonesty_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.dishonesty
        WHERE
        dt='{version}'
        AND
        case_create_time <= '{relation_version}'
        '''.format(version=DISHONESTY_VERSION,
                   relation_version=RELATION_VERSION)
    )
    dishonesty_count_df = dishonesty_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'dishonesty_num')
    os.system(
        "hadoop fs -rmr {path}/dishonesty/{version}".format(version=RELATION_VERSION, 
                                                            path=OUT_PATH))
    dishonesty_count_df.repartition(10).write.parquet(
        "{path}/dishonesty/{version}".format(version=RELATION_VERSION, 
                                             path=OUT_PATH))
    
    
    #经营异常
    jyyc_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.qyxg_jyyc
        WHERE
        dt='{version}'
        AND
        rank_date <= '{relation_version}'
        '''.format(version=JYYC_VERSION,
                   relation_version=RELATION_VERSION)
    )
    jyyc_count_df = jyyc_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'jyyc_num')
    os.system(
        "hadoop fs -rmr {path}/jyyc/{version}".format(version=RELATION_VERSION, 
                                                      path=OUT_PATH))
    jyyc_count_df.repartition(10).write.parquet(
        "{path}/jyyc/{version}".format(version=RELATION_VERSION, 
                                       path=OUT_PATH))
    
    
    #银监会行政处罚
    circxzcf_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.qyxg_circxzcf
        WHERE
        dt='{version}'
        AND
        pubdate <= '{relation_version}'
        '''.format(version=CIRCXZCF_VERSION,
                   relation_version=RELATION_VERSION)
    )
    circxzcf_count_df = circxzcf_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'circxzcf_num')
    os.system(
        "hadoop fs -rmr {path}/circxzcf/{version}".format(version=RELATION_VERSION, 
                                                          path=OUT_PATH))
    circxzcf_count_df.repartition(10).write.parquet(
        "{path}/circxzcf/{version}".format(version=RELATION_VERSION, 
                                           path=OUT_PATH))
    
    
    #分支机构
    fzjg_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id
        FROM
        dw.qyxx_fzjg_extend
        WHERE
        dt='{version}'
        '''.format(version=FZJG_VERSION)
    )
    fzjg_count_df = fzjg_df.groupBy('bbd_qyxx_id') \
        .count() \
        .withColumnRenamed('count', 'fzjg_num')
    os.system(
        "hadoop fs -rmr {path}/fzjg/{version}".format(version=RELATION_VERSION, 
                                                      path=OUT_PATH))
    fzjg_count_df.repartition(10).write.parquet(
        "{path}/fzjg/{version}".format(version=RELATION_VERSION, 
                                       path=OUT_PATH))
    
    
    #公司字号
    namefrag_df = sample_df.rdd \
        .repartition(100) \
        .mapPartitions(get_company_namefrag) \
        .map(lambda r: Row(company_name=r[0], namefrag=r[1])) \
        .toDF()
    os.system(
        "hadoop fs -rmr {path}/namefrag/{version}".format(version=RELATION_VERSION, 
                                                          path=OUT_PATH))
    namefrag_df.repartition(10).write.parquet(
        "{path}/namefrag/{version}".format(version=RELATION_VERSION, 
                                           path=OUT_PATH))
    
    
    #黑企业名单
    black_df = spark.sql(
        '''
        SELECT 
        company_name, 
        'black' company_type
        FROM 
        dw.qyxg_leijinrong_blacklist
        '''
    ).dropDuplicates(
        ['company_name']    
    )
    #黑企业省份分布
    black_province_df  = black_df.join(
        basic_df,
        basic_df.company_name == black_df.company_name
    ).select(
        black_df.company_name,
        basic_df.company_province
    ).where(
        basic_df.company_province != 'NULL'
    ).groupBy(
        'company_province'
    ).agg(
        {"company_name": "count"}
    ).withColumnRenamed(
        'count(company_name)', 'province_black_num'
    )
    os.system(
        "hadoop fs -rmr {path}/black_province/{version}".format(version=RELATION_VERSION, 
                                                                path=OUT_PATH))
    black_province_df.repartition(10).write.parquet(
        "{path}/black_province/{version}".format(version=RELATION_VERSION, 
                                                 path=OUT_PATH))
    
    
    #类金融名单
    leijinrong_df = spark.sql(
        '''
        SELECT 
        company 
        FROM 
        hongjing.raw_company_namefrag 
        WHERE
        dt='{version}'
        '''.format(version=LEIJINRONG_VERSION))
    #类金融企业省份分布
    leijinrong_province_df  = leijinrong_df.join(
        basic_df,
        basic_df.company_name == leijinrong_df.company,
    ).select(
        basic_df.company_name,
        basic_df.company_province
    ).where(
        basic_df.company_province != 'NULL'
    ).groupBy(
        'company_province'
    ).agg(
        {"company_name": "count"}
    ).withColumnRenamed(
        'count(company_name)', 'province_leijinrong_num'
    )
    os.system(
        "hadoop fs -rmr {path}/leijinrong_province/{version}".format(version=RELATION_VERSION, 
                                                                     path=OUT_PATH))
    leijinrong_province_df.repartition(10).write.parquet(
    "{path}/leijinrong_province/{version}".format(version=RELATION_VERSION, 
                                                  path=OUT_PATH))


    
if __name__ == "__main__":
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.conf")

    #关联方版本
    RELATION_VERSION = '20170403'    
    
    #输入数据版本
    SO_VERSION = conf.get('step_one', 'SO_VERSION')
    BASIC_VERSION = conf.get('step_one', 'BASIC_VERSION')
    ZHUANLI_VERSION = conf.get('step_one', 'ZHUANLI_VERSION')
    SHANGBIAO_VERSION = conf.get('step_one', 'SHANGBIAO_VERSION')
    DOMAIN_WEBSITE_VERSION = conf.get('step_one', 'DOMAIN_WEBSITE_VERSION')
    BGXX_VERSION = conf.get('step_one', 'BGXX_VERSION')
    RECRUIT_VERSION = conf.get('step_one', 'RECRUIT_VERSION')
    ZHAOBIAO_VERSION = conf.get('step_one', 'ZHAOBIAO_VERSION')
    ZHONGBIAO_VERSION = conf.get('step_one', 'ZHONGBIAO_VERSION')
    KTGG_VERSION = conf.get('step_one', 'KTGG_VERSION')
    ZGCPWSW_VERSION = conf.get('step_one', 'ZGCPWSW_VERSION')
    RMFYGG_VERSION = conf.get('step_one', 'RMFYGG_VERSION')
    XZCF_VERSION = conf.get('step_one', 'XZCF_VERSION')
    ZHIXING_VERSION = conf.get('step_one', 'ZHIXING_VERSION')
    DISHONESTY_VERSION = conf.get('step_one', 'DISHONESTY_VERSION')
    JYYC_VERSION = conf.get('step_one', 'JYYC_VERSION')
    CIRCXZCF_VERSION = conf.get('step_one', 'CIRCXZCF_VERSION')
    FZJG_VERSION = conf.get('step_one', 'CIRCXZCF_VERSION')
    LEIJINRONG_VERSION = conf.get('step_one', 'LEIJINRONG_VERSION')
    
    #数据输出路径
    OUT_PATH = conf.get('step_one', 'raw_out_path')

    #sparkSession
    spark = get_spark_session()
    
    run()