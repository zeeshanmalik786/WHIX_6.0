from pyspark.sql import functions as func
from pytz import timezone
from cpe_devices.preprocess  import preprocessor
from cpe_devices.config import config

class device_names:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.tz = timezone('est')


    def __device_names__(self, run_date):

        self.spark.sql("DROP TABLE IF EXISTS default.rdkb_wifi_fact_device_table_staging")

        self.obj.get_data("edl_raw_iptv.cpe_device_event_raw", ["device.accountSourceId",
                                                                "device.deviceSourceId",
                                                                "deviceData.mac",
                                                                "deviceData.deviceType",
                                                                "deviceData.deviceModel",
                                                                "deviceData.interfaceName",
                                                                "deviceData.streamingDevice",
                                                                "deviceData.brand",
                                                                "received_date"]).\
            filter(func.col("received_date") == func.date_sub(func.lit(run_date), 3)).\
            withColumn("combined_cpe", func.arrays_zip(func.col("mac"),
                                                      func.col("deviceType"),
                                                      func.col("deviceModel"),
                                                      func.col("interfaceName"),
                                                      func.col("streamingDevice"),
                                                      func.col("brand"))). \
            withColumn("combined_exploded_cpe", func.explode_outer("combined_cpe")).\
            select("accountSourceId",
                   "deviceSourceId",
                   func.col("combined_exploded_cpe.mac"),
                   func.col("combined_exploded_cpe.deviceType"),
                   func.col("combined_exploded_cpe.deviceModel"),
                   func.col("combined_exploded_cpe.interfaceName"),
                   func.col("combined_exploded_cpe.streamingDevice"),
                   func.col("combined_exploded_cpe.brand"),
                   "received_date").\
            write.saveAsTable("default.rdkb_wifi_fact_device_table_staging")

        self.spark.sql("INSERT INTO default.rdkb_wifi_fact_device_table (accountSourceId, deviceSourceId, mac, deviceType, deviceModel, interfaceName, streamingDevice, brand, received_date) SELECT accountSourceId, deviceSourceId, mac, deviceType, deviceModel, interfaceName, streamingDevice, brand, received_date  from default.rdkb_wifi_fact_device_table_staging")

