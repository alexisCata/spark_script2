from time import time
from manager.spark_manager import SparkManager
from iface.select_fields import *

# /opt/conf/spark-defaults.conf
# #RedshiftJDBC42-1.2.1.1001.jar
# http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver
# #spark-avro_2.11-3.2.0.jar      https://spark-packages.org/package/databricks/spark-avro

# spark.driver.extraClassPath /home/alexis/Downloads/jars/*
# spark.executor.extraClassPath /home/alexis/Downloads/jars/*
# spark.sql.broadcastTimeout 600

if __name__ == "__main__":
    logger.info("START TIME: {}".format(datetime.now()))

    spark_manager = SparkManager(URL, DRIVER, USER, PASS)

    df_query = main_query(spark_manager)

    load_tables(spark_manager)

    df_result = calculate_fields(spark_manager, df_query)

    format_and_write_file(df_result)

    spark_manager.stop_session()

    logger.info("END TIME: {}".format(datetime.now()))
