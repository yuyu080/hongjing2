# -*- coding: utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-class-path /usr/share/java/mysql-connector-java-5.1.39.jar \
--jars /usr/share/java/mysql-connector-java-5.1.39.jar \
ra_area_chart.py
'''


import os
import configparser
import json
import datetime
from collections import OrderedDict, Counter

import MySQLdb
from operator import itemgetter
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as fun
from pyspark.sql import types as tp

def truncate_table(table):
    '''连接mysql，执行一个SQL'''
    db = MySQLdb.connect(host=PROP['ip'], user=PROP['user'], 
                         passwd=PROP['password'], db=PROP['db_name'], 
                         charset="utf8")
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    sql = "TRUNCATE TABLE {0}".format(table)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
    # 关闭数据库连接
    db.close()
    
    print "清空表{0}成功".format(table)


def get_all_company_info_df(version):
    '''
    获取单个版本的df
    '''
    raw_df = spark.read.parquet(
        ("{path}"
         "/all_company_info/{version}").format(path=IN_PATH,
                                               version=version))
    
    return raw_df

def get_wdzj_df(version):
    '''
    获取单个版本的df
    '''
    raw_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        company_name,
        platform_name,
        avg_return,
        dynamic_analysis,
        dt
        FROM
        dw.qyxg_wdzj
        WHERE
        dt='{version}'
        '''.format(version=version)
    )
    
    return raw_df

def get_exchange_df(version):
    '''
    获取单个版本的df
    '''
    raw_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        exchange_type,
        trading_variety,
        dt
        FROM
        dw.qyxg_exchange
        WHERE
        dt='{version}'
        '''.format(version=version)
    )
    
    return raw_df

def get_smjj_df(version):
    '''
    获取单个版本的df
    '''
    raw_df = spark.sql(
        '''
        SELECT
        bbd_qyxx_id,
        interim_before_fund,
        interim_after_fund,
        managed_fund_type,
        company_nature,
        employees,
        dt
        FROM
        dw.qyxg_jijin_simu
        WHERE
        dt='{version}'
        '''.format(version=version)
    )
    
    return raw_df

def raw_spark_data_flow(func, version_list):
    df_list = []
    for each_version in version_list:
        each_df = func(each_version)
        df_list.append(each_df)
    
    #将多个df合并
    tid_df = eval(
        "df_list[{0}]".format(0) + 
        "".join([
                ".union(df_list[{0}])".format(df_index) 
                for df_index in range(1, len(df_list))])
    )    

    return tid_df

def get_dt_month(dt):
    '''
    日期解析
    '''
    return datetime.datetime.strptime(dt, '%Y%M%d').strftime('%Y-%M')

def get_avg_return_num(avg_return):
    '''
    字段解析
    '''
    try:
        return float(avg_return.replace('%', ''))
    except:
        return 0.
       
def get_dynamic_analysis_obj(dynamic_analysis):
    '''
    字段解析
    '''
    try:
        return float(
            json.loads(
                dynamic_analysis
            ).get(
                u'预期投资期限', ''
            ).replace(u'\u4e2a\u6708', '')
        )
    except:
        return 0.    

def avg(nums):
    '''
    得到除0外的平均值
    '''
    f_nums = filter(None, nums)
    return  round(sum(f_nums) * 1. / len(f_nums), 
                           2) if f_nums else 0.    

def get_exchange_type_num(col):
    cont = Counter(col)
    top_five_info = cont.most_common(5)
    return json.dumps(dict(top_five_info), ensure_ascii=False)


def get_trading_variety_num(col):
    obj = ','.join(col)
    obj = filter(lambda x: x and x != 'NULL', obj.split(','))
    cont = Counter(obj)
    return json.dumps(dict(cont.most_common(5)), ensure_ascii=False)

def get_time_interval(col):
    result = []
    for each_item in col:
        if each_item > 50:
            result.append(u'50人以上')
        elif 10 <= each_item <= 50:
            result.append(u'10-50人')
        elif each_item < 10:
            result.append(u'10人以下')
    return json.dumps(dict(Counter(result)), ensure_ascii=False)

def get_counter(col):
    cont = Counter(col)
    return json.dumps(dict(cont), ensure_ascii=False)

def get_num(before, after):
    before, after = json.loads(before), json.loads(after)
    result = before + after
    return len(set(result))

def get_time_sequence(times):
    version_keys = sorted(map(get_dt_month, SMJJ_VERSION_LIST))
    times_data = dict(map(lambda x: x.split(':'), times))
    version_values = [times_data.get(key, 0) for key in version_keys]
    result = dict(
        date=version_keys,
        value=version_values
    )
    return json.dumps(result, ensure_ascii=False)
    
def get_high_risk_num(col):
    cont = Counter(col)
    version_keys = sorted(map(get_dt_month, VERSION_LIST))
    version_values = [cont.get(key, 0) for key in version_keys]
    result = dict(
        date=version_keys,
        value=version_values
    )
    return json.dumps(result, ensure_ascii=False)    
    
def get_id():
    return ''
    
    

def wdzj_data_flow():
    get_dt_month_udf = fun.udf(get_dt_month, tp.StringType())
    get_avg_return_num_udf = fun.udf(get_avg_return_num,
                                     tp.FloatType())        
    get_dynamic_analysis_obj_udf = fun.udf(get_dynamic_analysis_obj,
                                           tp.FloatType())
    avg_udf = fun.udf(avg, tp.FloatType())
    
    
    raw_df = raw_spark_data_flow(get_all_company_info_df, 
                                 VERSION_LIST).cache()
    raw_wdzj_df = raw_spark_data_flow(get_wdzj_df, 
                                      WDZJ_VERSION_LIST).cache()
    tmp_wdzj_df = raw_df.where(
        raw_df.company_type == u'网络借贷'
    ).select(
        'bbd_qyxx_id',
        'province',
        'city',
        'county',
        get_dt_month_udf('data_version').alias('dt'),
    )

    tid_wdzj_df = raw_wdzj_df.select(
        'bbd_qyxx_id',
        'platform_name',
        get_dt_month_udf('dt').alias('dt'),
        get_avg_return_num_udf('avg_return').alias('avg_return'),
        get_dynamic_analysis_obj_udf(
            'dynamic_analysis'
        ).alias('dynamic_analysis')
    )

    tid_wdzj_2_df = tid_wdzj_df.join(
        tmp_wdzj_df,
        [tid_wdzj_df.bbd_qyxx_id == tmp_wdzj_df.bbd_qyxx_id, 
         tid_wdzj_df.dt == tmp_wdzj_df.dt]
    ).select(
        tmp_wdzj_df.province,
        tmp_wdzj_df.city,
        tmp_wdzj_df.county,
        tmp_wdzj_df.bbd_qyxx_id,
        tmp_wdzj_df.dt,
        'avg_return',
        'dynamic_analysis'
    ).fillna(
        0.
    ).distinct(
    )        
    
    #全国平均数据
    tid_wdzj_9_df = tid_wdzj_2_df.groupBy(
        'dt'
    ).agg(
        {'avg_return': 'collect_list', 
         'dynamic_analysis': 'collect_list'}
    ).cache()
    
    
    data = tid_wdzj_9_df.select(
        'dt',
        fun.concat_ws(
            ':', 'dt', avg_udf('collect_list(dynamic_analysis)')
        ).alias('times_dynamic_analysis'),
        fun.concat_ws(
            ':', 'dt', avg_udf('collect_list(avg_return)')
        ).alias('times_avg_return')
    ).groupBy(
    ).agg(
        {'times_dynamic_analysis': 'collect_list', 
         'times_avg_return': 'collect_list'}
    ).withColumnRenamed(
        'collect_list(times_dynamic_analysis)', 'times_dynamic_analysis'
    ).withColumnRenamed(
        'collect_list(times_avg_return)', 'times_avg_return'
    ).rdd.map(
        lambda r: (r.times_avg_return, r.times_dynamic_analysis)
    ).collect()
    
    #全国平均参考收益率
    nationwide_times_avg_return = map(
        lambda item: OrderedDict(sorted(map(lambda x: x.split(':'), item))), 
        data[0])[0]
    #全国预期投资期限
    nationwide_times_dynamic_analysis = map(
        lambda item: OrderedDict(sorted(map(lambda x: x.split(':'), item))), 
        data[0])[1]
    
    #格式化结果
    def get_avg_return_json(province, city, county, info,  
                            nationwide=nationwide_times_avg_return):
        '''
        将数据格式化为一个json
        '''
        info_dict = dict(sorted(map(lambda x: x.split(':'), info)))
        for k, v in nationwide.iteritems():
            if info_dict.has_key(k):
                continue
            else:
                info_dict[k] = '0.0'
        
        info_keys = sorted(info_dict.keys())
        info_values = [info_dict[key] for key in info_keys]
    
        if county:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    county: info_values
                },
                date=info_keys
            )
        elif city:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    city: info_values
                },
                date=info_keys
            )
        else:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    province: info_values
                },
                date=info_keys
            )
            
        return json.dumps(result, ensure_ascii=False)
    
    get_avg_return_json_udf = fun.udf(get_avg_return_json, 
                                      tp.StringType())
    
    
    def get_dynamic_analysis_json(province, city, county, info,  
                                  nationwide=nationwide_times_dynamic_analysis):
        '''
        将数据格式化为一个json
        '''
        info_dict = dict(sorted(map(lambda x: x.split(':'), info)))
        for k, v in nationwide.iteritems():
            if info_dict.has_key(k):
                continue
            else:
                info_dict[k] = '0.0'
                
        info_keys = sorted(info_dict.keys())
        info_values = [info_dict[key] for key in info_keys]
    
        if county:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    county: info_values
                },
                date=info_keys
            )
        elif city:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    city: info_values
                },
                date=info_keys
            )
        else:
            result = dict(
                value={
                    u'全国': nationwide.values(),
                    province: info_values
                },
                date=info_keys
            )
            
        return  json.dumps(result, ensure_ascii=False)
    
    get_dynamic_analysis_json_udf = fun.udf(get_dynamic_analysis_json, 
                                            tp.StringType())
    
            
    tid_wdzj_3_df = tid_wdzj_2_df.groupBy(
        'province', 'city', 'county', 'dt'
    ).agg(
        {'avg_return': 'collect_list', 'dynamic_analysis': 'collect_list'}
    ).select(
        'province',
        'city', 
        'county', 
        'dt',
        fun.concat_ws(
            ':', 'dt', avg_udf('collect_list(dynamic_analysis)')
        ).alias('times_dynamic_analysis'),
        fun.concat_ws(
            ':', 'dt', avg_udf('collect_list(avg_return)')
        ).alias('times_avg_return')
    ).groupBy(
        'province', 'city', 'county'
    ).agg(
        {'times_dynamic_analysis': 'collect_list', 
         'times_avg_return': 'collect_list'}
    ).cache()
    
    prd_wdzj_df = tid_wdzj_3_df.select(
        'province',
        'city',
        'county',
        get_avg_return_json_udf(
            'province', 'city', 'county', 
            'collect_list(times_avg_return)'
        ).alias('net_avg_return_rate'),
        get_dynamic_analysis_json_udf(
            'province', 'city', 'county', 
            'collect_list(times_dynamic_analysis)'
        ).alias('net_avg_loan_date')
    )

    return prd_wdzj_df


def jjcs_data_flow():
    
    get_exchange_type_num_udf = fun.udf(get_exchange_type_num, 
                                        tp.StringType())
    get_trading_variety_num_udf = fun.udf(get_trading_variety_num, 
                                          tp.StringType())  
    get_dt_month_udf = fun.udf(get_dt_month, tp.StringType())    
    
    raw_df = get_all_company_info_df(VERSION_LIST[-1]).cache()
    raw_exchange_df = get_exchange_df(EXCHANGE_VERSION_LIST[-1]).cache()

    tid_exchange_df = raw_exchange_df.select(
        'bbd_qyxx_id',
        'exchange_type',
        'trading_variety',
        get_dt_month_udf('dt').alias('dt')
    )
    
    tmp_exchange_df = raw_df.where(
        raw_df.company_type == u'交易场所'
    ).select(
        'bbd_qyxx_id',
        'province',
        'city',
        'county',
        get_dt_month_udf('data_version').alias('dt'),
    )
    
    tid_exchange_2_df = tid_exchange_df.join(
        tmp_exchange_df,
        [tid_exchange_df.bbd_qyxx_id == tmp_exchange_df.bbd_qyxx_id, 
         tid_exchange_df.dt == tmp_exchange_df.dt]
    ).select(
        tmp_exchange_df.province,
        tmp_exchange_df.city,
        tmp_exchange_df.county,
        tmp_exchange_df.bbd_qyxx_id,
        tmp_exchange_df.dt,
        'exchange_type',
        'trading_variety'
    ).distinct(
    ).sort(
        'province', 'dt'
    ).cache()
    
    #统计exchange_type, trading_variety
    prd_exchange_df = tid_exchange_2_df.groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'exchange_type': 'collect_list',
         'trading_variety': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        get_exchange_type_num_udf(
            'collect_list(exchange_type)'
        ).alias('trade_place_type'),
        get_trading_variety_num_udf(
            'collect_list(trading_variety)'
        ).alias('trade_place_trade_type')
    )
    
    return prd_exchange_df

def smjj_data_flow():
    get_counter_udf = fun.udf(get_counter, tp.StringType())
    get_time_interval_udf = fun.udf(get_time_interval, tp.StringType())    
    get_num_udf = fun.udf(get_num, tp.IntegerType())    
    get_time_sequence_udf = fun.udf(get_time_sequence, tp.StringType())
    get_dt_month_udf = fun.udf(get_dt_month, tp.StringType()) 
    
    raw_df = raw_spark_data_flow(get_all_company_info_df, VERSION_LIST).cache()
    raw_smjj_df = raw_spark_data_flow(get_smjj_df, SMJJ_VERSION_LIST).cache()

    tid_smjj_df = raw_smjj_df.select(
        'bbd_qyxx_id',
        'interim_before_fund',
        'interim_after_fund',
        'managed_fund_type',
        'company_nature',
        'employees',
        get_dt_month_udf('dt').alias('dt')
    )
    
    tmp_smjj_df = raw_df.where(
        raw_df.company_type == u'私募基金'
    ).select(
        'bbd_qyxx_id',
        'province',
        'city',
        'county',
        get_dt_month_udf('data_version').alias('dt'),
    )

    tid_smjj_2_df = tid_smjj_df.join(
        tmp_smjj_df,
        [tid_smjj_df.bbd_qyxx_id == tmp_smjj_df.bbd_qyxx_id, 
         tid_smjj_df.dt == tmp_smjj_df.dt]
    ).select(
        tmp_smjj_df.province,
        tmp_smjj_df.city,
        tmp_smjj_df.county,
        tmp_smjj_df.bbd_qyxx_id,
        fun.concat_ws(
            ':', tmp_smjj_df.dt, get_num_udf('interim_before_fund', 
                                             'interim_after_fund')
        ).alias('times_fund_num')
    ).distinct(
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'times_fund_num': 'collect_list'}
    ).select(
        'province', 
        'city',
        'county',
        get_time_sequence_udf(
            'collect_list(times_fund_num)'
        ).alias('private_fund_product_num')
    ).cache()
    
    #产品类型、企业类型、人员规模
    #取最新时间
    
    tid_smjj_3_df = tid_smjj_df.where(
        tid_smjj_df.dt == get_dt_month(SMJJ_VERSION_LIST[-1])
    ).join(
        tmp_smjj_df,
        [tid_smjj_df.bbd_qyxx_id == tmp_smjj_df.bbd_qyxx_id, 
         tid_smjj_df.dt == tmp_smjj_df.dt]
    ).select(
        tmp_smjj_df.province,
        tmp_smjj_df.city,
        tmp_smjj_df.county,
        tmp_smjj_df.bbd_qyxx_id,
        'managed_fund_type',
        'company_nature',
        'employees'
    ).distinct(
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'managed_fund_type': 'collect_list',
         'company_nature': 'collect_list',
         'employees': 'collect_list'}
    ).select(
        'province',
        'city',
        'county',
        get_counter_udf(
            'collect_list(company_nature)'
        ).alias('private_fund_company_type'),
        get_counter_udf(
            'collect_list(managed_fund_type)'
        ).alias('private_fund_product_type'),
        get_time_interval_udf(
            'collect_list(employees)'
        ).alias('private_fund_employee_scale')
    ).cache()

    return tid_smjj_2_df, tid_smjj_3_df

def other_data_flow():
    get_high_risk_num_udf = fun.udf(get_high_risk_num, tp.StringType())
    get_dt_month_udf = fun.udf(get_dt_month, tp.StringType()) 
    
    raw_df = raw_spark_data_flow(get_all_company_info_df, 
                                 VERSION_LIST).cache()

    raw_xxjr_df = raw_df.where(
        raw_df.risk_rank == u'高危预警'
    ).where(
        raw_df.company_type == u'新兴金融'
    ).select(
        'province',
        'city',
        'county',
        'company_type',
        get_dt_month_udf('data_version').alias('dt'),
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'dt': 'collect_list'}
    ).select(
        'province', 
        'city', 
        'county',
        get_high_risk_num_udf(
            'collect_list(dt)'
        ).alias('other_high_risk_num')
    )
    
    raw_xedk_df = raw_df.where(
        raw_df.risk_rank == u'高危预警'
    ).where(
        raw_df.company_type == u'小额贷款'
    ).select(
        'province',
        'city',
        'county',
        'company_type',
        get_dt_month_udf('data_version').alias('dt'),
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'dt': 'collect_list'}
    ).select(
        'province', 
        'city', 
        'county',
        get_high_risk_num_udf(
            'collect_list(dt)'
        ).alias('petty_loan_high_risk_num')
    )
    
    raw_rzdb_df = raw_df.where(
        raw_df.risk_rank == u'高危预警'
    ).where(
        raw_df.company_type == u'融资担保'
    ).select(
        'province',
        'city',
        'county',
        'company_type',
        get_dt_month_udf('data_version').alias('dt'),
    ).groupBy(
        ['province', 'city', 'county']
    ).agg(
        {'dt': 'collect_list'}
    ).select(
        'province', 
        'city', 
        'county',
        get_high_risk_num_udf(
            'collect_list(dt)'
        ).alias('financing_guarantee_high_risk_num')
    )

    return raw_xxjr_df, raw_xedk_df, raw_rzdb_df





def spark_data_flow():
    get_id_udf = fun.udf(get_id, tp.StringType())
    
    #全量的all_company_info数据
    raw_df = raw_spark_data_flow(get_all_company_info_df, 
                                 VERSION_LIST).cache()

    #准备各个字段
    prd_wdzj_df = wdzj_data_flow()
    prd_exchange_df = jjcs_data_flow()
    tid_smjj_2_df, tid_smjj_3_df = smjj_data_flow()
    raw_xxjr_df, raw_xedk_df, raw_rzdb_df = other_data_flow()
        
    prd_df = raw_df.dropDuplicates(
        ['province', 'city', 'county']
    ).select(
        'province', 
        'city', 
        'county'
    ).join(
        prd_wdzj_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        prd_exchange_df,
        ['province', 'city', 'county'],
        'left_outer'
    ).join(
        tid_smjj_2_df,
        ['province', 'city', 'county'],
        'left_outer'    
    ).join(
        tid_smjj_3_df,
        ['province', 'city', 'county'],
        'left_outer'    
    ).join(
        raw_xxjr_df,
        ['province', 'city', 'county'],
        'left_outer'  
    ).join(
        raw_xedk_df,
        ['province', 'city', 'county'],
        'left_outer'  
    ).join(
        raw_rzdb_df,
        ['province', 'city', 'county'],
        'left_outer'  
    ).select(
        get_id_udf().alias('id'),
        raw_df.province,
        raw_df.city,
        raw_df.county.alias('area'),
        'net_avg_return_rate',
        'net_avg_loan_date',
        'trade_place_type',
        'trade_place_trade_type',
        'private_fund_product_num',
        'private_fund_product_type',
        'private_fund_company_type',
        'private_fund_employee_scale',
        'financing_guarantee_high_risk_num',
        'petty_loan_high_risk_num',
        'other_high_risk_num',
        fun.current_timestamp().alias('gmt_create'),
        fun.current_timestamp().alias('gmt_update')
    ).distinct(
    ).fillna(
        {'city': u'无', 'area': u'无', 'province': u'无'}
    ).fillna(
        ''
    )
    
    return prd_df

def run():
    prd_df = spark_data_flow()
    
    os.system(
        ("hadoop fs -rmr " 
         "{path}/"
         "ra_area_chart/{version}").format(path=OUT_PATH, 
                                           version=NEW_VERSION))
    prd_df.repartition(
        10
    ).rdd.map(
        lambda r:
            '\t'.join([
                r.id,
                r.province,
                r.city,
                r.area,
                r.net_avg_return_rate,
                r.net_avg_loan_date,
                r.trade_place_type,
                r.trade_place_trade_type,
                r.private_fund_product_num,
                r.private_fund_product_type,
                r.private_fund_company_type,
                r.private_fund_employee_scale,
                r.financing_guarantee_high_risk_num,
                r.petty_loan_high_risk_num,
                r.other_high_risk_num,
                r.gmt_create.strftime('%Y-%m-%d %H:%M:%S'),
                r.gmt_update.strftime('%Y-%m-%d %H:%M:%S')
            ])
    ).saveAsTextFile(
        "{path}/ra_area_chart/{version}".format(path=OUT_PATH,
                                                version=NEW_VERSION)
    )

    #输出到mysql
    if IS_INTO_MYSQL:
        truncate_table('ra_area_chart')
        os.system(
        ''' 
        sqoop export \
        --connect {url} \
        --username {user} \
        --password '{password}' \
        --table {table} \
        --export-dir {path}/{table}/{version} \
        --input-fields-terminated-by '\\t' 
        '''.format(
                url=URL,
                user=PROP['user'],
                password=PROP['password'],
                table=TABLE,
                path=OUT_PATH,
                version=NEW_VERSION
            )
        )
       
        print '\n************\n导入大成功SUCCESS !!\n************\n'

def get_spark_session():   
    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.set("spark.yarn.am.cores", 7)
    conf.set("spark.executor.memory", "50g")
    conf.set("spark.executor.instances", 10)
    conf.set("spark.executor.cores", 10)
    conf.set("spark.python.worker.memory", "2g")
    conf.set("spark.default.parallelism", 1000)
    conf.set("spark.sql.shuffle.partitions", 1000)
    conf.set("spark.broadcast.blockSize", 1024)   
    conf.set("spark.shuffle.file.buffer", '512k')
    conf.set("spark.speculation", True)
    conf.set("spark.speculation.quantile", 0.98)

    spark = SparkSession \
        .builder \
        .appName("hgongjing2_four_raw_all_info") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()  
        
    return spark 

if __name__ == '__main__':
    conf = configparser.ConfigParser()    
    conf.read("/data5/antifraud/Hongjing2/conf/hongjing2.py")
    
    #输入数据版本
    VERSION_LIST = eval(conf.get('common', 'RELATION_VERSIONS'))
    VERSION_LIST.sort()
    NEW_VERSION = VERSION_LIST[-1]
    #细分行业的累计输入版本号
    WDZJ_VERSION_LIST = eval(conf.get('common', 'WDZJ_VERSION_LIST'))
    EXCHANGE_VERSION_LIST = eval(conf.get('common', 'EXCHANGE_VERSION_LIST'))
    SMJJ_VERSION_LIST = eval(conf.get('common', 'SMJJ_VERSION_LIST'))
    
    #结果存一份在HDFS，同时判断是否输出到mysql
    IN_PATH = conf.get('all_company_info', 'OUT_PATH')
    OUT_PATH = conf.get('to_mysql', 'OUT_PATH')
    IS_INTO_MYSQL = conf.getboolean('to_mysql', 'IS_INTO_MYSQL')
    
    #mysql输出信息
    TABLE = 'ra_area_count'
    URL = conf.get('mysql', 'URL')
    PROP = eval(conf.get('mysql', 'PROP'))
    
    spark = get_spark_session()
    
    run()