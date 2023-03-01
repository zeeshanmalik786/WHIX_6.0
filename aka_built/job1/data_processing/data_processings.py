from data_processing.preprocess  import preprocessor
from data_processing.config import config
from pyspark.sql import functions as func
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from pyspark.sql.functions import *
from pytz import timezone
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StringType


class data_process:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.tz = timezone('EST')

    def __whix_data_processing__(self, run_date, hour):

        #  enlarge rdk records
        print(hour)
        print(run_date)
        connected_devices_5G = self.obj.get_data("edl_raw_iptv.rdkb_raw", ["device.accountSourceId",
                                                                           "device.deviceSourceId",
                                                                           "device.firmwareVersion",
                                                                           "RAWATTRIBUTES.AccountId",
                                                                           "device.make",
                                                                           "RAWATTRIBUTES.Wifi_5G_utilization_split",
                                                                           "RAWATTRIBUTES.Total_2G_clients_split",
                                                                           "RAWATTRIBUTES.5GclientMac_split",
                                                                           "RAWATTRIBUTES.MAXRX_2_split",
                                                                           "RAWATTRIBUTES.WIFI_RX_2_split",
                                                                           "RAWATTRIBUTES.WIFI_TX_2_split",
                                                                           "RAWATTRIBUTES.Total_5G_clients_split",
                                                                           "RAWATTRIBUTES.Total_6G_clients_split",
                                                                           "RAWATTRIBUTES.MAXTX_2_split",
                                                                           "RAWATTRIBUTES.WIFIRetransCount2_split",
                                                                           "RAWATTRIBUTES.WIFI_ERRORSSENT_2_split",
                                                                           "RAWATTRIBUTES.WIFI_SNR_2_split",
                                                                           "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_2_split",
                                                                           "RAWATTRIBUTES.NF_2_split",
                                                                           "RAWATTRIBUTES.5GRSSI_split",
                                                                           "RAWATTRIBUTES.WIFI_CW_2_split",
                                                                           "RAWATTRIBUTES.rdkb_rebootreason_split",
                                                                           "RAWATTRIBUTES.thermal_fanspeed_split",
                                                                           "RAWATTRIBUTES.thermal_ChipTemp_split",
                                                                           "RAWATTRIBUTES.WIFI_ERROR_Diag3DataCorrupt",
                                                                           "RAWATTRIBUTES.mem_syseventd",
                                                                           "RAWATTRIBUTES.CMTSMAC_split",
                                                                           "RAWATTRIBUTES.5GPODSSID_split",
                                                                           "RAWATTRIBUTES.UPTIME_split",
                                                                           "RAWATTRIBUTES.2GPODSSID_split",
                                                                           "RAWATTRIBUTES.WIFI_INFO_clientdisconnect",
                                                                           "RAWATTRIBUTES.LoadAvg_split",
                                                                           "RAWATTRIBUTES.UPDays_split",
                                                                           "DEVICE.postalCode",
                                                                           "DEVICE.model",
                                                                           "received_hour"]). \
            withColumn("2G_clients", func.when(func.col("Total_2G_clients_split").isNull(), 0).otherwise(func.col("Total_2G_clients_split"))). \
            withColumn("5G_clients", func.when(func.col("Total_5G_clients_split").isNull(), 0).otherwise(func.col("Total_5G_clients_split"))). \
            withColumn("6G_clients", func.when(func.col("Total_6G_clients_split").isNull(), 0).otherwise(func.col("Total_6G_clients_split"))). \
            withColumn("Total_Clients", func.col("2G_clients") + func.col("5G_clients") + func.col("6G_clients")). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')).\
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (func.hour(col("received_hour"))==lit(hour))).\
            withColumn("combined_5G", func.arrays_zip(
                                                                         func.split(func.col("5GclientMac_split"), ",").alias("5GclientMac_split"),
                                                                         func.split(func.col("5GRSSI_split"), ",").cast(ArrayType(StringType())).alias("5GRSSI_split"),
                                                                         func.split(func.col("WIFI_CW_2_split"), ",").cast(ArrayType(StringType())).alias("WIFI_CW_2_split"),
                                                                         func.split(func.col("NF_2_split"), ",").cast(ArrayType(StringType())).alias("NF_2_split"),
                                                                         func.split(func.col("WIFI_PACKETSSENTCLIENTS_2_split"), ",").cast(ArrayType(StringType())).alias("WIFI_PACKETSSENTCLIENTS_2_split"),
                                                                         func.split(func.col("WIFI_SNR_2_split"), ",").cast(ArrayType(IntegerType())).alias("WIFI_SNR_2_split"),
                                                                         func.split(func.col("MAXTX_2_split"), ",").cast(ArrayType(DoubleType())).alias("MAXTX_2_split"),
                                                                         func.split(func.col("WIFI_TX_2_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_TX_2_split"),
                                                                         func.split(func.col("WIFIRetransCount2_split"), ",").cast(ArrayType(DoubleType())).alias("WIFIRetransCount2_split"),
                                                                         func.split(func.col("WIFI_ERRORSSENT_2_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_ERRORSSENT_2_split"),
                                                                         func.split(func.col("MAXRX_2_split"), ",").cast(ArrayType(DoubleType())).alias("MAXRX_2_split"),
                                                                         func.split(func.col("WIFI_RX_2_split"),",").cast(ArrayType(DoubleType())).alias("WIFI_RX_2_split"))).\
            withColumn("GW_MODELS", substring(col("firmwareVersion"), 0, 6)). \
            withColumn("GW_MODEL", func.when(col("GW_MODELS") == "TG3482", "XB6-A").otherwise(
            func.when(col("GW_MODELS") == "CGM414", "XB6-T")
                .otherwise(func.when(col("GW_MODELS") == "CGM433", "XB7-T")
                           .otherwise(func.when(col("GW_MODELS") == "TG4482", "XB7-C")
                .otherwise(func.when(col("GW_MODELS") == "CGM498", "XB8-T")
                                      .otherwise("None")))))). \
            withColumn("combined_exploded_5G", func.explode_outer("combined_5G")).\
            withColumn("reboot", func.when(func.col("rdkb_rebootreason_split").isNotNull(), 1).otherwise(0)).\
            select("accountSourceId",
                   "deviceSourceId",
                   "AccountId",
                   "GW_MODEL",
                   "WIFI_ERROR_Diag3DataCorrupt",
                   "CMTSMAC_split",
                   "mem_syseventd",
                   "make",
                   "postalCode",
                   "model",
                   "5GclientMac_split",
                   "combined_exploded_5G",
                   "reboot",
                   "rdkb_rebootreason_split",
                   "thermal_fanspeed_split",
                   "thermal_ChipTemp_split",
                   "WIFI_INFO_clientdisconnect",
                   "Wifi_5G_utilization_split",
                   "Total_Clients",
                   "2GPODSSID_split",
                   "5GPODSSID_split",
                   "LoadAvg_split",
                   "UPTIME_split",
                   "UPDays_split",
                   "received_hour")

        connected_devices_2G = self.obj.get_data("edl_raw_iptv.rdkb_raw", ["device.accountSourceId",
                                                                           "device.deviceSourceId",
                                                                           "device.firmwareVersion",
                                                                           "RAWATTRIBUTES.AccountId",
                                                                           "device.make",
                                                                           "RAWATTRIBUTES.2GclientMac_split",
                                                                           "RAWATTRIBUTES.MAXRX_1_split",
                                                                           "RAWATTRIBUTES.MAXTX_1_split",
                                                                           "RAWATTRIBUTES.WIFI_RX_1_split",
                                                                           "RAWATTRIBUTES.WIFI_TX_1_split",
                                                                           "RAWATTRIBUTES.Total_2G_clients_split",
                                                                           "RAWATTRIBUTES.WIFIRetransCount1_split",
                                                                           "RAWATTRIBUTES.WIFI_ERRORSSENT_1_split",
                                                                           "RAWATTRIBUTES.WIFI_SNR_1_split",
                                                                           "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_1_split",
                                                                           "RAWATTRIBUTES.NF_1_split",
                                                                           "RAWATTRIBUTES.2GRSSI_split",
                                                                           "RAWATTRIBUTES.WIFI_CW_1_split",
                                                                           "RAWATTRIBUTES.Wifi_2G_utilization_split",
                                                                           "RAWATTRIBUTES.rdkb_rebootreason_split",
                                                                           "RAWATTRIBUTES.thermal_fanspeed_split",
                                                                           "RAWATTRIBUTES.thermal_ChipTemp_split",
                                                                           "RAWATTRIBUTES.WIFI_ERROR_Diag3DataCorrupt",
                                                                           "RAWATTRIBUTES.mem_syseventd",
                                                                           "RAWATTRIBUTES.CMTSMAC_split",
                                                                           "RAWATTRIBUTES.2GPODSSID_split",
                                                                           "RAWATTRIBUTES.UPTIME_split",
                                                                           "RAWATTRIBUTES.WIFI_INFO_clientdisconnect",
                                                                           "RAWATTRIBUTES.LoadAvg_split",
                                                                           "RAWATTRIBUTES.UPDays_split",
                                                                           "DEVICE.postalCode",
                                                                           "DEVICE.model",
                                                                           "received_hour"]). \
            withColumn("2G_clients", func.when(func.col("Total_2G_clients_split").isNull(), 0).otherwise(func.col("Total_2G_clients_split"))). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (
                    func.hour(func.col("received_hour")) == func.lit(hour))). \
            withColumn("combined_2G", func.arrays_zip(func.split(func.col("2GclientMac_split"), ",").alias("2GclientMac_split"),
                                                                 func.split(func.col("2GRSSI_split"), ",").cast(ArrayType(StringType())).alias("2GRSSI_split"),
                                                                 func.split(func.col("WIFI_CW_1_split"), ",").cast(ArrayType(StringType())).alias("WIFI_CW_1_split"),
                                                                 func.split(func.col("NF_1_split"), ",").cast(ArrayType(StringType())).alias("NF_1_split"),
                                                                 func.split(func.col("WIFI_PACKETSSENTCLIENTS_1_split"), ",").cast(ArrayType(StringType())).alias("WIFI_PACKETSSENTCLIENTS_1_split"),
                                                                 func.split(func.col("WIFI_SNR_1_split"), ",").cast(ArrayType(IntegerType())).alias("WIFI_SNR_1_split"),
                                                                 func.split(func.col("MAXTX_1_split"), ",").cast(ArrayType(DoubleType())).alias("MAXTX_1_split"),
                                                                 func.split(func.col("WIFI_TX_1_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_TX_1_split"),
                                                                 func.split(func.col("WIFIRetransCount1_split"), ",").cast(ArrayType(DoubleType())).alias("WIFIRetransCount1_split"),
                                                                 func.split(func.col("WIFI_ERRORSSENT_1_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_ERRORSSENT_1_split"),
                                                                 func.split(func.col("WIFI_RX_1_split"),",").cast(ArrayType(DoubleType())).alias("WIFI_RX_1_split"),
                                                                 func.split(func.col("MAXRX_1_split"), ",").cast(ArrayType(DoubleType())).alias("MAXRX_1_split"))). \
            withColumn("combined_exploded_2G", func.explode_outer("combined_2G")). \
            filter((func.col("combined_exploded_2G.2GclientMac_split").isNotNull())). \
            filter((func.col("combined_exploded_2G.2GclientMac_split") != "")).\
            select("accountSourceId",
                   "deviceSourceId",
                   "2GclientMac_split",
                   "Wifi_2G_utilization_split",
                   "combined_exploded_2G",
                   "received_hour")

        connected_devices_6G = self.obj.get_data("edl_raw_iptv.rdkb_raw",["device.accountSourceId",
                                                                        "device.deviceSourceId",
                                                                        "device.firmwareVersion",
                                                                        "RAWATTRIBUTES.AccountId",
                                                                        "device.make",
                                                                        "RAWATTRIBUTES.WIFI_CW_17_split",
                                                                        "RAWATTRIBUTES.6GclientMac_split",
                                                                        "RAWATTRIBUTES.6GRSSI_split",
                                                                        "RAWATTRIBUTES.CWconfig_3_split",
                                                                        "RAWATTRIBUTES.NF_3_split",
                                                                        "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_17_split",
                                                                        "RAWATTRIBUTES.WIFI_SNR_17_split",
                                                                        "RAWATTRIBUTES.MAXTX_17_split",
                                                                        "RAWATTRIBUTES.WIFI_TX_17_split",
                                                                        "RAWATTRIBUTES.WIFIRetransCount17_split",
                                                                        "RAWATTRIBUTES.WIFI_ERRORSSENT_17_split",
                                                                        "RAWATTRIBUTES.MAXRX_17_split",
                                                                        "RAWATTRIBUTES.WIFI_RX_17_split",
                                                                        "RAWATTRIBUTES.Wifi_6G_utilization_split",
                                                                        "RAWATTRIBUTES.6GSSID_split",
                                                                        "RAWATTRIBUTES.Total_6G_clients_split",
                                                                        "RAWATTRIBUTES.WifiTemp_6G_split",
                                                                        "RAWATTRIBUTES.rdkb_rebootreason_split",
                                                                        "RAWATTRIBUTES.thermal_fanspeed_split",
                                                                        "RAWATTRIBUTES.thermal_ChipTemp_split",
                                                                        "RAWATTRIBUTES.WIFI_ERROR_Diag3DataCorrupt",
                                                                        "RAWATTRIBUTES.mem_syseventd",
                                                                        "RAWATTRIBUTES.CMTSMAC_split",
                                                                        "RAWATTRIBUTES.UPTIME_split",
                                                                        "RAWATTRIBUTES.WIFI_INFO_clientdisconnect",
                                                                        "RAWATTRIBUTES.LoadAvg_split",
                                                                        "RAWATTRIBUTES.UPDays_split",
                                                                        "DEVICE.postalCode",
                                                                        "DEVICE.model",
                                                                        "received_hour"]). \
            withColumn("6G_clients", func.when(func.col("Total_6G_clients_split").isNull(), 0).otherwise(func.col("Total_6G_clients_split"))). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (func.hour(func.col("received_hour")) == func.lit(hour))).\
            withColumn("combined_6G",func.arrays_zip(func.split(func.col("6GclientMac_split"), ",").alias("6GclientMac_split"),
                                                     func.split(func.col("WIFI_CW_17_split"), ",").alias("WIFI_CW_17_split"),
                                                     func.split(func.col("6GRSSI_split"), ",").cast(ArrayType(StringType())).alias("6GRSSI_split"),
                                                     func.split(func.col("NF_3_split"), ",").cast(ArrayType(StringType())).alias("NF_3_split"),
                                                     func.split(func.col("WIFI_TX_17_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_TX_17_split"),
                                                     func.split(func.col("WIFI_RX_17_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_RX_17_split"))).\
            withColumn("combined_exploded_6G", func.explode_outer("combined_6G")).\
            filter((func.col("combined_exploded_6G.6GclientMac_split").isNotNull())). \
            filter((func.col("combined_exploded_6G.6GclientMac_split") != "")). \
            select("accountSourceId",
                   "deviceSourceId",
                   "6GclientMac_split",
                   "Wifi_6G_utilization_split",
                   "combined_exploded_6G",
                   "received_hour")

        connected_devices_6G = connected_devices_6G.\
                               withColumn("WIFI_PACKETSSENTCLIENTS_17_split", lit(0)).\
                               withColumn("WIFI_SNR_17_split", lit(0)).\
                               withColumn("MAXTX_17_split", lit(0)).\
                               withColumn("WIFIRetransCount17_split", lit(0)).\
                               withColumn("WIFI_ERRORSSENT_17_split", lit(0)).\
                               withColumn("MAXRX_17_split", lit(0))

        connected_devices_t = self.obj.join_three_frames(connected_devices_5G, connected_devices_2G, connected_devices_6G, "left", ["accountSourceId",
                                                                                                                                    "deviceSourceId",
                                                                                                                                    "received_hour"])
        connected_devices_pods_2G = self.obj.get_data("edl_raw_iptv.rdkb_raw",["device.accountSourceId",
                                                                               "device.deviceSourceId",
                                                                               "RAWATTRIBUTES.2GPodMac_split",
                                                                               "RAWATTRIBUTES.2GPodRSSI_split",
                                                                               "DEVICE.postalCode",
                                                                               "DEVICE.model",
                                                                               "received_hour"
                                                                              ]). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (
                func.hour(func.col("received_hour")) == func.lit(hour))). \
                withColumn("combined_pod_2G", func.arrays_zip(func.split(func.col("2GPodMac_split"), ",").alias("2GPodMac_split"),
                                                      func.split(func.col("2GPodRSSI_split"), ",").cast(ArrayType(StringType())).alias("2GPodRSSI_split"))).\
                withColumn("combined_pods_2G", func.explode_outer("combined_pod_2G")).\
                filter((func.col("combined_pods_2G.2GPodMac_split").isNotNull())).\
                filter((func.col("combined_pods_2G.2GPodMac_split") != "")).\
                select("accountSourceId",
                   "deviceSourceId",
                   "combined_pods_2G",
                   "received_hour")

        connected_devices_pods_5G = self.obj.get_data("edl_raw_iptv.rdkb_raw", ["device.accountSourceId",
                                                                                "device.deviceSourceId",
                                                                                "RAWATTRIBUTES.5GPodMac_split",
                                                                                "RAWATTRIBUTES.5GPodRSSI_split",
                                                                                "DEVICE.postalCode",
                                                                                "DEVICE.model",
                                                                                "received_hour"
                                                                                ]). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (
                func.hour(func.col("received_hour")) == func.lit(hour))). \
            withColumn("combined_pod_5G", func.arrays_zip(func.split(func.col("5GPodMac_split"), ",").alias("5GPodMac_split"),
                                          func.split(func.col("5GPodRSSI_split"), ",").cast(ArrayType(StringType())).alias("5GPodRSSI_split"))). \
            withColumn("combined_pods_5G", func.explode_outer("combined_pod_5G")). \
            filter((func.col("combined_pods_5G.5GPodMac_split").isNotNull())). \
            filter((func.col("combined_pods_5G.5GPodMac_split") != "")).\
            select("accountSourceId",
                   "deviceSourceId",
                   "combined_pods_5G",
                   "received_hour")

        connected_devices_pods = self.obj.join_two_frames(connected_devices_pods_5G,connected_devices_pods_2G,"left",["accountSourceId",
                                                                                                                      "deviceSourceId",
                                                                                                                      "received_hour"])

        connected_devices_sub = self.obj.join_two_frames(connected_devices_t,connected_devices_pods,"left",["accountSourceId",
                                                                                                        "deviceSourceId",
                                                                                                        "received_hour"])


        devices_reconnects = self.obj.get_data("edl_raw_iptv.rdkb_raw", ["device.accountSourceId",
                                                                         "device.deviceSourceId",
                                                                         "RAWATTRIBUTES.WIFI_REC_1_split",
                                                                         "RAWATTRIBUTES.WIFI_REC_2_split",
                                                                         "received_hour"]). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (
                    func.hour(func.col("received_hour")) == func.lit(hour))).\
            withColumn("WIFI_REC_1_COUNT", size(split(col("WIFI_REC_1_split"), ","))).\
            withColumn("WIFI_REC_2_COUNT", size(split(col("WIFI_REC_2_split"), ","))).\
            withColumn("WIFI_REC_1_COUNT",func.when(col("WIFI_REC_1_COUNT") == -1,  0).otherwise(col("WIFI_REC_1_COUNT"))). \
            withColumn("WIFI_REC_2_COUNT",func.when(col("WIFI_REC_2_COUNT") == -1, 0).otherwise(col("WIFI_REC_2_COUNT"))). \
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "received_hour").agg(sum(col("WIFI_REC_1_COUNT")).alias("WIFI_REC_1_COUNT"),
                                         sum(col("WIFI_REC_2_COUNT")).alias("WIFI_REC_2_COUNT"))

        # self.spark.sql("DROP TABLE IF EXISTS default.rdkb_whix_six_table_staging_tmp")

        # devices_reconnects.filter(col("accountSourceId") == "L6660413073517444").write.saveAsTable("default.rdkb_whix_six_table_staging_tmp")


        connected_devices_all = self.obj.join_two_frames(connected_devices_sub, devices_reconnects, "left", ["accountSourceId",
                                                                                                         "deviceSourceId",
                                                                                                         "received_hour"])

        rdkb_events = self.obj.get_data("edl_raw_iptv.rdkb_raw", ["device.accountSourceId",
                                                                  "device.deviceSourceId",
                                                                  "RDKBEVENTS.eventCode",
                                                                  "RDKBEVENTS.eventCount",
                                                                  "received_hour"]). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (func.hour(func.col("received_hour")) == func.lit(hour))). \
            withColumn("combined_events", func.arrays_zip(col("eventCode"), col("eventCount"))). \
            withColumn("combined_exp", func.explode_outer(col("combined_events"))). \
            select("accountSourceId",
                   "deviceSourceId",
                   func.col("combined_exp.eventCode").alias("eventCode"),
                   func.col("combined_exp.eventCount").alias("eventCount"),
                   "received_hour"). \
            filter(col("eventCode").like("%WIFI_ERROR%"))

        connected_devices = self.obj.join_two_frames(connected_devices_all, rdkb_events, "left", ["accountSourceId",
                                                                                                  "deviceSourceId",
                                                                                                  "received_hour"])
        self.spark.sql("DROP TABLE IF EXISTS default.iptv_whix_raw_staging_one")

        connected_devices_all = connected_devices.select("accountSourceId",
                                 "deviceSourceId",
                                 "AccountId",
                                 "GW_MODEL",
                                 "make",
                                 "postalCode",
                                 "model",
                                 func.col("combined_exploded_2G.2GclientMac_split"),
                                 func.col("combined_exploded_5G.5GclientMac_split"),
                                 func.col("combined_exploded_6G.6GclientMac_split"),
                                 func.col("combined_exploded_2G.2GRSSI_split"),
                                 func.col("combined_exploded_5G.5GRSSI_split"),
                                 func.col("combined_exploded_6G.6GRSSI_split"),
                                 func.col("combined_exploded_2G.WIFI_CW_1_split"),
                                 func.col("combined_exploded_5G.WIFI_CW_2_split"),
                                 func.col("combined_exploded_6G.WIFI_CW_17_split"),
                                 func.col("combined_exploded_2G.NF_1_split"),
                                 func.col("combined_exploded_5G.NF_2_split"),
                                 func.col("combined_exploded_6G.NF_3_split"),
                                 func.col("combined_exploded_2G.WIFI_PACKETSSENTCLIENTS_1_split"),
                                 func.col("combined_exploded_5G.WIFI_PACKETSSENTCLIENTS_2_split"),
                                 "WIFI_PACKETSSENTCLIENTS_17_split",
                                 func.col("combined_exploded_2G.WIFI_SNR_1_split"),
                                 func.col("combined_exploded_5G.WIFI_SNR_2_split"),
                                 "WIFI_SNR_17_split",
                                 func.col("combined_exploded_2G.MAXTX_1_split"),
                                 func.col("combined_exploded_5G.MAXTX_2_split"),
                                 "MAXTX_17_split",
                                 func.col("combined_exploded_2G.MAXRX_1_split"),
                                 func.col("combined_exploded_5G.MAXRX_2_split"),
                                 "MAXRX_17_split",
                                 func.col("combined_exploded_2G.WIFI_RX_1_split"),
                                 func.col("combined_exploded_5G.WIFI_RX_2_split"),
                                 func.col("combined_exploded_6G.WIFI_RX_17_split"),
                                 func.col("combined_exploded_2G.WIFI_TX_1_split"),
                                 func.col("combined_exploded_5G.WIFI_TX_2_split"),
                                 func.col("combined_exploded_6G.WIFI_TX_17_split"),
                                 func.col("combined_exploded_2G.WIFI_ERRORSSENT_1_split"),
                                 func.col("combined_exploded_5G.WIFI_ERRORSSENT_2_split"),
                                 "WIFI_ERRORSSENT_17_split",
                                 func.col("combined_pods_2G.2GPodMac_split"),
                                 func.col("combined_pods_5G.5GPodMac_split"),
                                 func.col("combined_pods_2G.2GPodRSSI_split"),
                                 func.col("combined_pods_5G.5GPodRSSI_split"),
                                 func.col("combined_exploded_2G.WIFIRetransCount1_split"),
                                 func.col("combined_exploded_5G.WIFIRetransCount2_split"),
                                 "WIFIRetransCount17_split",
                                 func.col("WIFI_REC_1_COUNT"),
                                 func.col("WIFI_REC_2_COUNT"),
                                 "eventCode",
                                 "eventCount",
                                 "Wifi_2G_utilization_split",
                                 "Wifi_5G_utilization_split",
                                 "Wifi_6G_utilization_split",
                                 "WIFI_ERROR_Diag3DataCorrupt",
                                 "LoadAvg_split",
                                 "CMTSMAC_split",
                                 "mem_syseventd",
                                 "thermal_ChipTemp_split",
                                 "thermal_fanspeed_split",
                                 "rdkb_rebootreason_split",
                                 "2GPODSSID_split",
                                 "5GPODSSID_split",
                                 "UPTIME_split",
                                 "WIFI_INFO_clientdisconnect",
                                 "UPDays_split",
                                 "Total_Clients",
                                 "received_hour"). \
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "AccountId",
                    "GW_Model",
                    "make",
                    "postalCode",
                    "model",
                    "2GclientMac_split",
                    "5GclientMac_split",
                    "6GclientMac_split",
                    "2GPodMac_split",
                    "5GPodMac_split",
                    "received_hour").agg(
                                         max(col("2GRSSI_split")).alias("2GRSSI_split"),
                                         max(col("5GRSSI_split")).alias("5GRSSI_split"),
                                         max(col("6GRSSI_split")).alias("6GRSSI_split"),
                                         max(col("WIFI_CW_1_split")).alias("WIFI_CW_1_split"),
                                         max(col("WIFI_CW_2_split")).alias("WIFI_CW_2_split"),
                                         max(col("WIFI_CW_17_split")).alias("WIFI_CW_3_split"),
                                         max(col("NF_1_split")).alias("NF_1_split"),
                                         max(col("NF_2_split")).alias("NF_2_split"),
                                         max(col("NF_3_split")).alias("NF_3_split"),
                                         max(col("WIFI_PACKETSSENTCLIENTS_1_split")).alias("WIFI_PACKETSSENTCLIENTS_1_split"),
                                         max(col("WIFI_PACKETSSENTCLIENTS_2_split")).alias("WIFI_PACKETSSENTCLIENTS_2_split"),
                                         max(col("WIFI_PACKETSSENTCLIENTS_17_split")).alias("WIFI_PACKETSSENTCLIENTS_3_split"),
                                         max(col("WIFI_SNR_1_split")).alias("WIFI_SNR_1_split"),
                                         max(col("WIFI_SNR_2_split")).alias("WIFI_SNR_2_split"),
                                         max(col("WIFI_SNR_17_split")).alias("WIFI_SNR_3_split"),
                                         max(col("MAXTX_1_split")).alias("MAXTX_1_split"),
                                         max(col("MAXTX_2_split")).alias("MAXTX_2_split"),
                                         max(col("MAXTX_17_split")).alias("MAXTX_17_split"),
                                         max(col("MAXRX_1_split")).alias("MAXRX_1_split"),
                                         max(col("MAXRX_2_split")).alias("MAXRX_2_split"),
                                         max(col("MAXRX_17_split")).alias("MAXRX_17_split"),
                                         max(col("WIFI_RX_1_split")).alias("WIFI_RX_1_split"),
                                         max(col("WIFI_RX_2_split")).alias("WIFI_RX_2_split"),
                                         max(col("WIFI_RX_17_split")).alias("WIFI_RX_17_split"),
                                         max(col("WIFI_TX_1_split")).alias("WIFI_TX_1_split"),
                                         max(col("WIFI_TX_2_split")).alias("WIFI_TX_2_split"),
                                         max(col("WIFI_TX_17_split")).alias("WIFI_TX_17_split"),
                                         max(col("WIFI_ERRORSSENT_1_split")).alias("WIFI_ERRORSSENT_1_split"),
                                         max(col("WIFI_ERRORSSENT_2_split")).alias("WIFI_ERRORSSENT_2_split"),
                                         max(col("WIFI_ERRORSSENT_17_split")).alias("WIFI_ERRORSSENT_3_split"),
                                         max(col("2GPodRSSI_split")).alias("2GPodRSSI_split"),
                                         max(col("5GPodRSSI_split")).alias("5GPodRSSI_split"),
                                         max(col("WIFIRetransCount1_split")).alias("WIFIRetransCount1_split"),
                                         max(col("WIFIRetransCount2_split")).alias("WIFIRetransCount2_split"),
                                         max(col("WIFIRetransCount17_split")).alias("WIFIRetransCount3_split"),
                                         max(col("WIFI_REC_1_COUNT")).alias("WIFI_REC_1_COUNT"),
                                         max(col("WIFI_REC_2_COUNT")).alias("WIFI_REC_2_COUNT"),
                                         max(col("eventCode")).alias("eventCode"),
                                         max(col("eventCount")).alias("eventCount"),
                                         max(col("Wifi_2G_utilization_split")).alias("Wifi_2G_utilization_split"),
                                         max(col("Wifi_5G_utilization_split")).alias("Wifi_5G_utilization_split"),
                                         max(col("Wifi_6G_utilization_split")).alias("Wifi_6G_utilization_split"),
                                         max(col("WIFI_ERROR_Diag3DataCorrupt")).alias("WIFI_ERROR_Diag3DataCorrupt"),
                                         max(col("LoadAvg_split")).alias("LoadAvg_split"),
                                         max(col("CMTSMAC_split")).alias("CMTSMAC_split"),
                                         max(col("mem_syseventd")).alias("mem_syseventd"),
                                         max(col("thermal_ChipTemp_split")).alias("thermal_ChipTemp_split"),
                                         max(col("thermal_fanspeed_split")).alias("thermal_fanspeed_split"),
                                         max(col("rdkb_rebootreason_split")).alias("rdkb_rebootreason_split"),
                                         max(col("2GPODSSID_split")).alias("2GPODSSID_split"),
                                         max(col("5GPODSSID_split")).alias("5GPODSSID_split"),
                                         max(col("UPTIME_split")).alias("UPTIME_split"),
                                         max(col("WIFI_INFO_clientdisconnect")).alias("WIFI_INFO_clientdisconnect"),
                                         max(col("UPDays_split")).alias("UPDays_split"),
                                         max(col("Total_Clients")).alias("Total_Clients")).\
            filter(col("accountSourceId").isNotNull())

        gw_reboots = self.obj.get_data("edl_raw_iptv.rdkb_raw",            ["device.accountSourceId",
                                                                           "device.deviceSourceId",
                                                                           "device.firmwareVersion",
                                                                           "RAWATTRIBUTES.AccountId",
                                                                           "device.make",
                                                                           "RAWATTRIBUTES.Wifi_5G_utilization_split",
                                                                           "RAWATTRIBUTES.5GclientMac_split",
                                                                           "RAWATTRIBUTES.2GclientMac_split",
                                                                           "RAWATTRIBUTES.MAXRX_2_split",
                                                                           "RAWATTRIBUTES.MAXRX_1_split",
                                                                           "RAWATTRIBUTES.WIFI_RX_1_split",
                                                                           "RAWATTRIBUTES.WIFI_RX_2_split",
                                                                           "RAWATTRIBUTES.WIFI_TX_1_split",
                                                                           "RAWATTRIBUTES.WIFI_TX_2_split",
                                                                           "RAWATTRIBUTES.Total_2G_clients_split",
                                                                           "RAWATTRIBUTES.Total_5G_clients_split",
                                                                           "RAWATTRIBUTES.MAXTX_2_split",
                                                                           "RAWATTRIBUTES.MAXTX_1_split",
                                                                           "RAWATTRIBUTES.WIFIRetransCount2_split",
                                                                           "RAWATTRIBUTES.WIFI_ERRORSSENT_2_split",
                                                                           "RAWATTRIBUTES.WIFI_SNR_2_split",
                                                                           "RAWATTRIBUTES.WIFI_SNR_1_split",
                                                                           "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_2_split",
                                                                           "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_1_split",
                                                                           "RAWATTRIBUTES.NF_2_split",
                                                                           "RAWATTRIBUTES.5GRSSI_split",
                                                                           "RAWATTRIBUTES.2GRSSI_split",
                                                                           "RAWATTRIBUTES.WIFI_CW_1_split",
                                                                           "RAWATTRIBUTES.WIFI_CW_2_split",
                                                                           "RAWATTRIBUTES.WIFI_CW_17_split",
                                                                           "RAWATTRIBUTES.6GclientMac_split",
                                                                           "RAWATTRIBUTES.6GRSSI_split",
                                                                           "RAWATTRIBUTES.CWconfig_3_split",
                                                                           "RAWATTRIBUTES.NF_1_split",
                                                                           "RAWATTRIBUTES.NF_3_split",
                                                                           "RAWATTRIBUTES.WIFI_PACKETSSENTCLIENTS_17_split",
                                                                           "RAWATTRIBUTES.WIFI_SNR_17_split",
                                                                           "RAWATTRIBUTES.MAXTX_17_split",
                                                                           "RAWATTRIBUTES.WIFI_TX_17_split",
                                                                           "RAWATTRIBUTES.WIFIRetransCount1_split",
                                                                           "RAWATTRIBUTES.WIFI_ERRORSSENT_1_split",
                                                                           "RAWATTRIBUTES.WIFIRetransCount17_split",
                                                                           "RAWATTRIBUTES.WIFI_ERRORSSENT_17_split",
                                                                           "RAWATTRIBUTES.MAXRX_17_split",
                                                                           "RAWATTRIBUTES.WIFI_RX_17_split",
                                                                           "RAWATTRIBUTES.Wifi_6G_utilization_split",
                                                                           "RAWATTRIBUTES.Wifi_2G_utilization_split",
                                                                           "RAWATTRIBUTES.6GSSID_split",
                                                                           "RAWATTRIBUTES.Total_6G_clients_split",
                                                                           "RAWATTRIBUTES.WifiTemp_6G_split",
                                                                           "RAWATTRIBUTES.rdkb_rebootreason_split",
                                                                           "RAWATTRIBUTES.thermal_fanspeed_split",
                                                                           "RAWATTRIBUTES.thermal_ChipTemp_split",
                                                                           "RAWATTRIBUTES.WIFI_ERROR_Diag3DataCorrupt",
                                                                           "RAWATTRIBUTES.mem_syseventd",
                                                                           "RAWATTRIBUTES.CMTSMAC_split",
                                                                           "RAWATTRIBUTES.2GPODSSID_split",
                                                                           "RAWATTRIBUTES.5GPODSSID_split",
                                                                           "RAWATTRIBUTES.UPTIME_split",
                                                                           "RAWATTRIBUTES.WIFI_INFO_clientdisconnect",
                                                                           "RAWATTRIBUTES.LoadAvg_split",
                                                                           "RAWATTRIBUTES.UPDays_split",
                                                                           "DEVICE.postalCode",
                                                                           "DEVICE.model",
                                                                           "received_hour"]). \
            withColumn("2G_clients", func.when(func.col("Total_2G_clients_split").isNull(), 0).otherwise(
            func.col("Total_2G_clients_split"))). \
            withColumn("5G_clients", func.when(func.col("Total_5G_clients_split").isNull(), 0).otherwise(
            func.col("Total_5G_clients_split"))). \
            withColumn("6G_clients", func.when(func.col("Total_6G_clients_split").isNull(), 0).otherwise(
            func.col("Total_6G_clients_split"))). \
            withColumn("Total_Clients", func.col("2G_clients") + func.col("5G_clients") + func.col("6G_clients")). \
            withColumn("received_hour", col("received_hour") + func.expr('INTERVAL 4 HOURS')). \
            filter((func.to_date(func.col("received_hour")) == func.lit(run_date)) & (
                    func.hour(col("received_hour")) == lit(hour))). \
            withColumn("combined_5G", func.arrays_zip(
            func.split(func.col("5GclientMac_split"), ",").alias("5GclientMac_split"),
            func.split(func.col("5GRSSI_split"), ",").cast(ArrayType(StringType())).alias("5GRSSI_split"),
            func.split(func.col("WIFI_CW_2_split"), ",").cast(ArrayType(StringType())).alias("WIFI_CW_2_split"),
            func.split(func.col("NF_2_split"), ",").cast(ArrayType(StringType())).alias("NF_2_split"),
            func.split(func.col("WIFI_PACKETSSENTCLIENTS_2_split"), ",").cast(ArrayType(StringType())).alias(
                "WIFI_PACKETSSENTCLIENTS_2_split"),
            func.split(func.col("WIFI_SNR_2_split"), ",").cast(ArrayType(IntegerType())).alias("WIFI_SNR_2_split"),
            func.split(func.col("MAXTX_2_split"), ",").cast(ArrayType(DoubleType())).alias("MAXTX_2_split"),
            func.split(func.col("WIFI_TX_2_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_TX_2_split"),
            func.split(func.col("WIFIRetransCount2_split"), ",").cast(ArrayType(DoubleType())).alias(
                "WIFIRetransCount2_split"),
            func.split(func.col("WIFI_ERRORSSENT_2_split"), ",").cast(ArrayType(DoubleType())).alias(
                "WIFI_ERRORSSENT_2_split"),
            func.split(func.col("MAXRX_2_split"), ",").cast(ArrayType(DoubleType())).alias("MAXRX_2_split"),
            func.split(func.col("WIFI_RX_2_split"), ",").cast(ArrayType(DoubleType())).alias("WIFI_RX_2_split"))). \
            withColumn("GW_MODELS", substring(col("firmwareVersion"), 0, 6)). \
            withColumn("GW_MODEL", func.when(col("GW_MODELS") == "TG3482", "XB6-A").otherwise(
            func.when(col("GW_MODELS") == "CGM414", "XB6-T")
                .otherwise(func.when(col("GW_MODELS") == "CGM433", "XB7-T")
                           .otherwise(func.when(col("GW_MODELS") == "TG4482", "XB7-C")
                                      .otherwise("None"))))). \
            withColumn("combined_exploded_5G", func.explode_outer("combined_5G")). \
            withColumn("reboot", func.when(func.col("rdkb_rebootreason_split").isNotNull(), 1).otherwise(0)). \
            select("accountSourceId",
                   "deviceSourceId",
                   "AccountId",
                   "reboot",
                   "received_hour").\
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "received_hour").agg(sum(col("reboot")).alias("reboot"))

        self.obj.join_two_frames(connected_devices_all, gw_reboots, "left", ["accountSourceId",
                                                                             "deviceSourceId",
                                                                             "received_hour"]). \
            withColumn("received_hour", func.col("received_hour") - func.expr("INTERVAL 4 HOURS")).\
            select("accountSourceId",
                   "deviceSourceId",
                   "AccountId",
                   "GW_MODEL",
                    "make",
                    "postalCode",
                    "model",
                    "2GclientMac_split",
                    "5GclientMac_split",
                    "6GclientMac_split",
                    "2GRSSI_split",
                                 "5GRSSI_split",
                                 "6GRSSI_split",
                                 "WIFI_CW_1_split",
                                 "WIFI_CW_2_split",
                                 "WIFI_CW_3_split",
                                 "NF_1_split",
                                 "NF_2_split",
                                 "NF_3_split",
                                 "WIFI_PACKETSSENTCLIENTS_1_split",
                                 "WIFI_PACKETSSENTCLIENTS_2_split",
                                 "WIFI_PACKETSSENTCLIENTS_3_split",
                                 "WIFI_SNR_1_split",
                                 "WIFI_SNR_2_split",
                                 "WIFI_SNR_3_split",
                                 "MAXTX_1_split",
                                 "MAXTX_2_split",
                                 "MAXTX_17_split",
                                 "MAXRX_1_split",
                                 "MAXRX_2_split",
                                 "MAXRX_17_split",
                                 "WIFI_RX_1_split",
                                 "WIFI_RX_2_split",
                                 "WIFI_RX_17_split",
                                 "WIFI_TX_1_split",
                                 "WIFI_TX_2_split",
                                 "WIFI_TX_17_split",
                                 "WIFI_ERRORSSENT_1_split",
                                 "WIFI_ERRORSSENT_2_split",
                                 "WIFI_ERRORSSENT_3_split",
                                 "2GPodMac_split",
                                 "5GPodMac_split",
                                 "2GPodRSSI_split",
                                 "5GPodRSSI_split",
                                 "WIFIRetransCount1_split",
                                 "WIFIRetransCount2_split",
                                 "WIFIRetransCount3_split",
                                 "WIFI_REC_1_COUNT",
                                 "WIFI_REC_2_COUNT",
                                 "eventCode",
                                 "eventCount",
                                 "Wifi_2G_utilization_split",
                                 "Wifi_5G_utilization_split",
                                 "Wifi_6G_utilization_split",
                                 "WIFI_ERROR_Diag3DataCorrupt",
                                 "LoadAvg_split",
                                 "CMTSMAC_split",
                                 "mem_syseventd",
                                 "reboot",
                                 "thermal_ChipTemp_split",
                                 "thermal_fanspeed_split",
                                 "rdkb_rebootreason_split",
                                 "2GPODSSID_split",
                                 "5GPODSSID_split",
                                 "UPTIME_split",
                                 "WIFI_INFO_clientdisconnect",
                                 "UPDays_split",
                                 "Total_Clients",
                                 "received_hour").write.saveAsTable("default.iptv_whix_raw_staging_one")

        self.spark.sql("INSERT INTO default.iptv_whix_raw_staging_one_historical SELECT * from default.iptv_whix_raw_staging_one")

        return True