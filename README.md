### Spark Script ###

It connects to a Redshift database using Pyspark. It queries de db for a given day, retrieves data, makes some transformations and writes an avro file in S3

* Version: 1.0

### Dependencies ###

* [Pyspark](https://spark.apache.org/)
* [Findspark](https://github.com/minrk/findspark)
* [CurrencyExchange](https://bitbucket.org/CathedralSW/currency_exchange)

### How do I run it? ###

The cluster needs to be created with a config file:
```shell
aws emr create-cluster ...  --configurations file://./myConfig.json
```
myConfig.json
```
[   
    {
        "Classification": "capacity-scheduler",
        "Properties": {
            "yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
        }
    },

    {
        "Classification": "spark",
        "Properties": {
            "maximizeResourceAllocation": "true"
        }
    },

    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.dynamicAllocation.enabled": "true",
            "spark.executor.instances": "0"
        }
    } 
]
```
Execute spark submit:
```shell
/usr/bin/spark-submit --packages="com.amazonaws:aws-java-sdk-s3:1.11.140,com.databricks:spark-avro_2.11:3.2.0,com.databricks:spark-redshift_2.11:2.0.1" --jars RedshiftJDBC42-1.2.1.1001.jar --deploy-mode client --master yarn --conf spark.sql.broadcastTimeout=3600 --conf spark.network.timeout=10000000 --py-files dependencies.zip booking_extractions.py 2016-10-01 > output.log
```
