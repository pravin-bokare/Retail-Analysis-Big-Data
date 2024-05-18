import configparser
from pyspark import SparkConf

# loading the application configs in python dictionary
def get_app_conf(env):
    config = configparser.ConfigParser()
    config.read('configs/application.conf')
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

# loading the spark configs in python dictionary
def get_spark_conf(env):
    config = configparser.ConfigParser()
    config.read('configs/pyspark.conf')
    spark_conf = SparkConf()
    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
