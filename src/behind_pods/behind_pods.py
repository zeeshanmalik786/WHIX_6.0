from src.preprocessor.preprocess  import preprocessor
from src.config.config import config
from pyspark.sql import functions as func
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from datetime import datetime
from pyspark.sql.functions import *
from pytz import timezone


class behind_pods:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.tz = timezone('EST')

    def __behind_pods__(self, run_date):

        get_distinct_pod_id = self.obj.get_data("edl_raw_iptv.cpe_life_pods_raw", ["device.accountSourceId",
                                                                                   "device.deviceSourceId",
                                                                                   "pods.podId",
                                                                                   "pods.clientDevicesCount",
                                                                                   "received_date"]).\
            filter(col("received_date")==lit(run_date)).\
            filter(col("accountSourceId")=="f8666759399694794").\
            withColumn("combined_pod_ids", func.arrays_zip(func.split(col("podId").cast(StringType()), ",").alias("podId"),
                                                           func.split(col("clientDevicesCount").cast(StringType()),",").alias("clientDevicesCount"))).\
            withColumn("combined_exploded_pod_id", func.explode_outer("combined_pod_ids")).\
            select("accountSourceId",
                   "deviceSourceId",
                   func.regexp_replace("combined_exploded_pod_id.podId","[^0-9A-Za-z]", "").alias("podId"),
                   func.regexp_replace("combined_exploded_pod_id.clientDevicesCount", "[^0-9A-Za-z]", "").alias("clientDevicesCount"),
                   "received_date").\
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "podId",
                    "received_date").agg(func.percentile_approx("clientDevicesCount","0.5").alias("clientDevicesCount"))
        self.spark.sql("DROP TABLE IF EXISTS default.tmp_test")

        get_client_devices = self.obj.get_data("edl_raw_iptv.cpe_device_event_raw",["device.accountSourceid",
                                                                                    "device.deviceSourceId",
                                                                                    "deviceTopology.id",
                                                                                    "deviceTopology.model",
                                                                                    col("deviceTopology.wifiConfig").alias("wifiConfig"),
                                                                                    "deviceTopology",
                                                                                    "received_date"]). \
            filter(col("received_date") == lit(run_date)).\
            filter(col("accountSourceId") == "f8666759399694794").\
            filter(col("deviceTopology").isNotNull()).\
            select("accountSourceId",
                   "deviceSourceId",
                   func.arrays_zip(col("id"),col("model"),col("wifiConfig")[1]).alias("device"),
                   "deviceTopology",
                   "received_date").\
            select("accountSourceId",
                   "deviceSourceId",
                   col("device.2").alias("device"),
                   "received_date").\
            select("accountSourceId",
                   "deviceSourceId",
                   func.arrays_zip(col("device.id"), col("device"), col("device.devices")),
                   "received_date").\
            write.saveAsTable("default.tmp_test")



        # get_client_devices.show()




        return True