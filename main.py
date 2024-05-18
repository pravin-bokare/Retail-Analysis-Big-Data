import sys
from lib import DataManipulation, DataReader, utils
from pyspark.sql.functions import *
from lib.logger import Log4j


if __name__ == "__main__":
    if len(sys.argv)<2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print('Creating Spark Session')

    spark = utils.get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.info("Created Spark Session")

    orders_df = DataReader.read_orders(spark, job_run_env)
    customer_df = DataReader.read_customers(spark, job_run_env)

    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    joined_df = DataManipulation.join_orders_customers(orders_filtered, customer_df)
    aggregated_result = DataManipulation.count_orders_state(joined_df)
    aggregated_result.show()

    logger.info("end of main")


