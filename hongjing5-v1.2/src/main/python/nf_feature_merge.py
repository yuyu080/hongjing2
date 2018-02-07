# -*- encoding=utf-8 -*-
'''
提交命令：
/opt/spark-2.0.2/bin/spark-submit \
--master yarn \
--deploy-mode client \
--queue project.hongjing \
nf_feature_merge.py
'''
import os

import configparser
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
reload(sys)
sys.setdefaultencoding("utf-8")
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
        .appName("hgongjing5_two_raw_nf_feature_merge") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def spark_data_flow(static_version, dynamic_version):
    '''
    合并静态与动态风险：当动态风险不存时，值为null，类型为srtuct
    '''
    static_df = spark.read.json(
        "{path}/"
        "common_static_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_TWO,
                           version=static_version))
    dynamic_df = spark.read.json(
        "{path}/"
        "common_dynamic_feature_distribution_v2/"
        "{version}".format(path=IN_PATH_TWO,
                           version=dynamic_version))

    sample_df = spark.read.parquet(
         ("{path}"
          "/ljr_sample/"
          "{version}").format(path=IN_PATH_ONE,
                              version=RELATION_VERSION))

    #这里需要一个样本df,
    #将某些类型的企业选出来分别打分
    some_type_df = sample_df.where(
        sample_df.company_type.isin(TYPE_NF_LIST)
    )

    feature_df = static_df.join(
        dynamic_df,
        static_df.bbd_qyxx_id == dynamic_df.bbd_qyxx_id,
        'left_outer'
    ).join(
        some_type_df,
        some_type_df.bbd_qyxx_id == static_df.bbd_qyxx_id
    ).select(
        [static_df.bbd_qyxx_id,
         static_df.company_name,
         'feature_1', 'feature_10', 'feature_12', 'feature_13', 'feature_15', 'feature_16', 'feature_17', 'feature_18', 'feature_19', 'feature_2', 'feature_20', 'feature_22', 'feature_23', 'feature_24', 'feature_26', 'feature_4', 'feature_5', 'feature_6', 'feature_7', 'feature_8', 'feature_9']
    )
    return feature_df

def run():
    # 静态与动态风险
    raw_df = spark_data_flow(static_version=RELATION_VERSION,
                             dynamic_version=RELATION_VERSION)

    os.system(
        ("hadoop fs -rmr "
         "{path}/"
         "nf_feature_merge/{version}").format(path=OUT_PATH,
                                              version=RELATION_VERSION))
    # 重分区，并且以parquet的形式储存
    raw_df.repartition(10).write.parquet(
        ("{path}/"
         "nf_feature_merge/{version}").format(path=OUT_PATH,
                                              version=RELATION_VERSION))

if __name__ == '__main__':
    # 读取配置文件
    config = configparser.ConfigParser()
    config.read("/data5/antifraud/qiling/conf/hongjing5.ini", encoding='UTF-8')

    # 计算公司列表
    TYPE_NF_LIST = eval(config.get('input_sample_data', 'TYPE_NF_LIST'))

    # 版本，以后改为手动输入
    RELATION_VERSION = '20171219'

    # 样例数据

    # 输出路径（是step_one计算出来的）
    # IN_PATH_ONE = /user/antifraud/hongjing2/dataflow/step_one/raw/
    IN_PATH_ONE = config.get('input_sample_data', 'OUT_PATH')

    # 公有的公司特征：静态与动态风险（是step_one计算出来的）
    # IN_PATH_TWO = /user/antifraud/hongjing2/dataflow/step_one/prd/
    IN_PATH_TWO = config.get('common_company_feature', 'OUT_PATH')

    # 特征输出的位置（到step_two了）
    OUT_PATH = config.get('feature_merge', 'OUT_PATH')
    # OUT_PATH = /user/gengqiling/hongjing5/dataflow/step_two/raw/

    # 测试集
    # TEST_LIST = ["4429cd5b184f45a88f357c88d543576e", "a230dd4c7bb54070832ccc8fa3a7775e", "b9dd447cbaa046f9baa2d3d6a1d6074d", "ba0b6d63fb1b47cdbdc1d2867e7407e5", "d03b9e1635064bff82218d7581235373", "73093209b7f445c5b1f49ad56df5290d", "fa06ef33909a4775b12ec1f231182bb4", "e9dbc71cb07347499eac35cf512cb594", "b894fcba9b0d460b92bfae73b874d0cc", "40e31c63e07641d1be07e46ab03ede71", "a42b5a56000840b086dc888645b3ca92", "8e409798fe004b1981ce1644400071e6", "31c575790f2141178bb1833a76f01487", "268fd934db5b4f8e90e9f334b3bed359", "6b7bd7d5c3a14632bf68d7b821578acb", "973e44a9609748b5a949d2cd01386945", "1ee13254b62845f4b4fc672ce7f3447c", "edca8932a9b145f6a8d07e1f139f589c", "a7d9cdd35fee47b7b39f72fbde7099f8", "f700e7a7454b4dd78f8835a0d4542a28", "1b378a01dacb41688c21d76f3e5ba2bf", "e8242d1da78f4716b88146c1c14c04ff", "7e9d7d2edae043529d5c529e38d812b7", "04bf1f8cc83e4f3fb3e79e227de17862", "b656b05eca7e481f861a7fb76ccd62f5", "15de9b292fa14cf1906c0af1eb458675", "57bf0a44b68940ebb72b08dbce38994c", "9863958877384bf9a759ad7c8e795eac", "c9095f150ce746d994f9e47f52c2d40a", "473b7808b27b4e22aa2b4412999d2cab", "27c2c52fd1c746b18f56ae68beacf804", "5aaf50c4b06a4ee6960d997db7d3e4cc", "81cf229cb29947f784dd7ffe9ca7c662", "bba745b23d3b460abdb0d23c861931ec", "9c8b5353b8954dd1bbe39204dcd35e24", "0a8e659466ed4716b2b790988824d514", "b0bd123a09c74105b0013cfc8c30b9c9", "c1ce247a19b34413949c66b68de551b7", "dcf1513bbc194b20a0aca2bd5eed0f25", "66df227246da4ea2acfb8ab6e90c5fd1", "2174020c1a2e4654b44c05f3bb8fbe35", "6d9ac52abab54e1fbd8512ad690e510c", "3109479bd3bc4ba5a4ae25f4685e08b4", "515eb6d29cde4b76a930c5344e2a6bbc", "5c18bd2d386f422c9c5c43f660eb2f24", "b7a4abfafc114275ad7501e2a5353785", "e4e125bc0d2f40c1a4ad69fe493804c8", "d9b99e49ec104537a5faee4adf6eabbf", "d12bebb2e8624b47b734638610f8b984", "3b4239bbfd584426a4bd8cde92d53f90", "dcfe1721ee644b00a0ab7571c9530e3c", "a2d007c05c0e4f18b368f9ec70e15a4c", "80b35faf97814b7db3e9d92cad27d342", "4dd700c2b6504895802f2a4c822ef115", "a2b46c835ea54579b5525a7f6f781c49", "19808dd4f5da414bad85201959f78a02", "0562d853f758452ba20049ee61a77f72", "9e87922d87de43418dab6638d625ec81", "bb37f1c7d08149c597fe963576519dd6", "81ebd65a59fb4bb38eb00a875974b7e0", "20603bd167ed45659f8409bcf26459b8", "00ff3c4b2eed412f8616baa8089d7617", "f23c8a0e6e764faa8346552042ba7d75", "b5ccb738ee5d46cdb634b0f1476f0bab", "2940d9b7b7464f8b8bdc141bf81af5bf", "880df818e70a4648afefd0e8f5321d09", "ba712441e5f140e9870936825bb645f4", "fc7d7591b283495c9f375cb9da6cae09", "8324249040144fa8bbf10c1efad0424c", "dba3b067b2d04587b38b254020f8ffd4", "573f1d76163844b1810c653c8a3511a7", "fc86168dbe294904884f5f61825dab0d", "ebfc2679002745f5b067b56cf3ea4fe8", "e31c69c083da44eeb4f9944c406067ff", "879b263a4faa4100bd0aa8944c2d937a", "207dc37a525f425191fb9b5a87cfae45", "dbffd69164b3463dbd3b0293e6d375ae", "909a873fd5244980967c65af8dd4afdd", "6bbe656364924729ada8904f4bf4a404", "95a98685e901460cb327eeb45d5c013f", "5134add2ad564af4b5f41838510836a8", "1be9878c23c442b49c42bc40aaa293ea", "dcddf3817d6c4ed2b7585b4d16d55e9e", "ab950d8a76d3428cb86c5d34dcb9a3e2", "5a639a84563d4254a56dac54d4f12a5f", "d4a3fbf9fb7644be90760fe20766481d", "211231bb2ccc473792f2ac9e1a5bd089", "03b0a0f495fe4d929ef1ea28b38c133c", "efa46e5b03f24dd09da26e2ee1d3d407", "03ec428439a8438aa33f652354bd90ef", "2fdc40e7c1d2481689952a0cd47687b8", "5a3cc3c2cdfa49568ffcb0b85f311ade", "7e535d29e7554f698d45914e71f9d12f", "5c09bc21bcac4c0e97ee9cb8b4724974", "dfd08bdb735a43c8873b1765906f15b4", "f02fad0dcc234cf08b1d9b20fafd8260", "91e59020f7f2455b89c25a568a38378e", "d67ee38c0f4544798316b5eee2f6c062", "4f7b8c228c58472da3a302e0f7428b97", "39aa3ad562ed4f61a737c770bb039771"]

    # 获取spark
    spark = get_spark_session()

    run()