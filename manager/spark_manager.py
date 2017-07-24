# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from settings import AWS_KEY, AWS_PASS, S3_PATH


class SparkManager:
    """
    Manager for Spark
    """

    def __init__(self, url, driver, user=None, password=None, table=None, app_name=None, master=None):
        self.url = url
        self.driver = driver
        self.user = user
        self.password = password
        self.table = table
        self.app_name = app_name
        self.master = master
        self.session = None
        # self._locate_spark()
        self.start_session()
        # self._check_driver()

    # @staticmethod
    # def _locate_spark():
    #     located_spark = False
    #     try:
    #         findspark.init()
    #         located_spark = True
    #     except Exception as e:
    #         print ("Error: Can't locate Spark: {}".format(e.message))
    #
    #     return located_spark

    # def _check_driver(self):
    #
    #     check_driver = False
    #     if os.environ['SPARK_CLASSPATH']:
    #         check_driver = True
    #     elif not os.environ['SPARK_CLASSPATH'] and self.driver:
    #         os.environ['SPARK_CLASSPATH'] = self.driver
    #         check_driver = True
    #     else:
    #         print ("Can't locate Spark Driver")
    #
    #     return check_driver

    def start_session(self):
        SparkSession
        # if self._check_driver and self._locate_spark():
        self.session = SparkSession.builder \
            .master(self.master or "local[*]") \
            .appName(self.app_name or "App Title") \
            .enableHiveSupport().getOrCreate()

    def get_dataframe(self, table=None):
        """
        It connects to DB and returns the given table or query
        :param table: Could be a table or a query '(select a from B) as name_for_query'
        :return: Dataframe
        """
        if table:
            self.table = table
        elif not self.table:
            print ("Table needed to retrieve data")

        self.start_session()

        return self.session.read.jdbc(url=self.url,
                                      table=self.table,
                                      properties={"driver": self.driver,
                                                  "user": self.user,
                                                  "password": self.password})

        # sc = self.session.sparkContext
        # sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        # sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", AWS_KEY)
        # sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", AWS_PASS)
        # sql = HiveContext(sc)
        #
        # return sql.read.format("com.databricks.spark.redshift").options(url=self.url,
        #                                                                 query=self.table,
        #                                                                 user=self.user,
        #                                                                 password=self.password,
        #                                                                 tempdir=S3_PATH
        #                                                                 ).load()

    def stop_session(self):
        self.session.stop()
