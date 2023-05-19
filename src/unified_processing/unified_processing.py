from src.preprocessor.preprocess  import preprocessor
from src.config.config import config
from pyspark.sql import functions as func
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from datetime import datetime
from pyspark.sql.functions import *
from pytz import timezone


class unified_process:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.tz = timezone('EST')

    def __whix_unified_processing__(self, run_date):

        flattening_rdk = self.obj.get_data("iptv_whix_raw_staging_one", ["accountSourceId",
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
                                                                            "received_hour"])

        twog_features = flattening_rdk.select("accountSourceId",
                                              "deviceSourceId",
                                              "AccountId",
                                              "GW_MODEL",
                                              "make",
                                              "postalCode",
                                              "model",
                                              "2GclientMac_split",
                                              "2GRSSI_split",
                                              "WIFI_CW_1_split",
                                              "NF_1_split",
                                              "WIFI_PACKETSSENTCLIENTS_1_split",
                                              "WIFIRetransCount1_split",
                                              "WIFI_SNR_1_split",
                                              "MAXTX_1_split",
                                              "MAXRX_1_split",
                                              "WIFI_TX_1_split",
                                              "WIFI_RX_1_split",
                                              "WIFI_ERRORSSENT_1_split",
                                              "WIFI_REC_1_COUNT",
                                              "eventCode",
                                              "eventCount",
                                              "Wifi_2G_utilization_split",
                                              "WIFI_ERROR_Diag3DataCorrupt",
                                              "LoadAvg_split",
                                              "reboot",
                                              "thermal_ChipTemp_split",
                                              "thermal_fanspeed_split",
                                              "UPTIME_split",
                                              "WIFI_INFO_clientdisconnect",
                                              "Total_Clients",
                                              "received_hour"). \
            withColumnRenamed("2GclientMac_split", "clientMac"). \
            withColumnRenamed("2GRSSI_split", "rssi"). \
            withColumnRenamed("WIFI_CW_1_split", "channel_width"). \
            withColumnRenamed("NF_1_split", "noise"). \
            withColumnRenamed("WIFI_PACKETSSENTCLIENTS_1_split", "packets_sent"). \
            withColumnRenamed("WIFI_SNR_1_split", "snr"). \
            withColumnRenamed("MAXTX_1_split", "maxtx"). \
            withColumnRenamed("WIFI_TX_1_split", "tx"). \
            withColumnRenamed("MAXRX_1_split", "maxrx"). \
            withColumnRenamed("WIFI_RX_1_split", "rx"). \
            withColumnRenamed("WIFI_ERRORSSENT_1_split", "errorsent"). \
            withColumnRenamed("WIFI_REC_1_COUNT", "rapid_reconnects_count"). \
            withColumnRenamed("WIFIRetransCount1_split", "retransmissions"). \
            withColumnRenamed("Wifi_2G_utilization_split", "channel_utilization"). \
            withColumn("tag", lit("2G"))

        fiveg_features = flattening_rdk.select("accountSourceId",
                                               "deviceSourceId",
                                               "AccountId",
                                               "GW_MODEL",
                                               "make",
                                               "postalCode",
                                               "model",
                                               "5GclientMac_split",
                                               "5GRSSI_split",
                                               "WIFI_CW_2_split",
                                               "NF_2_split",
                                               "WIFI_PACKETSSENTCLIENTS_2_split",
                                               "WIFIRetransCount2_split",
                                               "WIFI_SNR_2_split",
                                               "MAXTX_2_split",
                                               "MAXRX_2_split",
                                               "WIFI_TX_2_split",
                                               "WIFI_RX_2_split",
                                               "WIFI_ERRORSSENT_2_split",
                                               "WIFI_REC_2_COUNT",
                                               "eventCode",
                                               "eventCount",
                                               "Wifi_5G_utilization_split",
                                               "WIFI_ERROR_Diag3DataCorrupt",
                                               "LoadAvg_split",
                                               "reboot",
                                               "thermal_ChipTemp_split",
                                               "thermal_fanspeed_split",
                                               "UPTIME_split",
                                               "WIFI_INFO_clientdisconnect",
                                               "Total_Clients",
                                               "received_hour"). \
            withColumnRenamed("5GclientMac_split", "clientMac"). \
            withColumnRenamed("5GRSSI_split", "rssi"). \
            withColumnRenamed("WIFI_CW_2_split", "channel_width"). \
            withColumnRenamed("NF_2_split", "noise"). \
            withColumnRenamed("WIFI_PACKETSSENTCLIENTS_2_split", "packets_sent"). \
            withColumnRenamed("WIFI_SNR_2_split", "snr"). \
            withColumnRenamed("MAXTX_2_split", "maxtx"). \
            withColumnRenamed("MAXRX_2_split", "maxrx"). \
            withColumnRenamed("WIFI_TX_2_split", "tx"). \
            withColumnRenamed("WIFI_RX_2_split", "rx"). \
            withColumnRenamed("WIFI_ERRORSSENT_2_split", "errorsent"). \
            withColumnRenamed("WIFI_REC_2_COUNT", "rapid_reconnects_count"). \
            withColumnRenamed("WIFIRetransCount2_split", "retransmissions"). \
            withColumnRenamed("Wifi_5G_utilization_split", "channel_utilization"). \
            withColumn("tag", lit("5G"))

        sixg_features = flattening_rdk.select(
            "accountSourceId",
            "deviceSourceId",
            "AccountId",
            "GW_MODEL",
            "make",
            'postalCode',
            "model",
            "6GclientMac_split",
            "6GRSSI_split",
            "WIFI_CW_3_split",
            "NF_3_split",
            "WIFI_PACKETSSENTCLIENTS_3_split",
            "WIFIRetransCount3_split",
            "WIFI_SNR_3_split",
            "MAXTX_17_split",
            "MAXRX_17_split",
            "WIFI_TX_17_split",
            "WIFI_RX_17_split",
            "WIFI_ERRORSSENT_3_split",
            "eventCode",
            "eventCount",
            "Wifi_6G_utilization_split",
            "WIFI_ERROR_Diag3DataCorrupt",
            "LoadAvg_split",
            "reboot",
            "thermal_ChipTemp_split",
            "thermal_fanspeed_split",
            "UPTIME_split",
            "WIFI_INFO_clientdisconnect",
            "Total_Clients",
            "received_hour"). \
            withColumnRenamed("6GclientMac_split", "clientMac"). \
            withColumnRenamed("6GRSSI_split", "rssi"). \
            withColumnRenamed("WIFI_CW_3_split", "channel_width"). \
            withColumnRenamed("NF_3_split", "noise"). \
            withColumnRenamed("WIFI_PACKETSSENTCLIENTS_3_split", "packets_sent"). \
            withColumnRenamed("WIFI_SNR_3_split", "snr"). \
            withColumnRenamed("MAXTX_17_split", "maxtx"). \
            withColumnRenamed("MAXRX_17_split", "maxrx"). \
            withColumnRenamed("WIFI_TX_17_split", "tx"). \
            withColumnRenamed("WIFI_RX_17_split", "rx"). \
            withColumnRenamed("WIFI_ERRORSSENT_3_split", "errorsent"). \
            withColumn("rapid_reconnects_count", lit(0)). \
            withColumnRenamed("WIFIRetransCount3_split", "retransmissions"). \
            withColumnRenamed("Wifi_6G_utilization_split", "channel_utilization"). \
            withColumn("tag", lit("6G")). \
            select(
            "accountSourceId",
            "deviceSourceId",
            "AccountId",
            "GW_MODEL",
            "make",
            "postalCode",
            "model",
            "clientMac",
            "rssi",
            "channel_width",
            "noise",
            "packets_sent",
            "retransmissions",
            "snr",
            "maxtx",
            "maxrx",
            "tx",
            "rx",
            "errorsent",
            "rapid_reconnects_count",
            "eventCode",
            "eventCount",
            "channel_utilization",
            "WIFI_ERROR_Diag3DataCorrupt",
            "LoadAvg_split",
            "reboot",
            "thermal_ChipTemp_split",
            "thermal_fanspeed_split",
            "UPTIME_split",
            "WIFI_INFO_clientdisconnect",
            "Total_Clients",
            "received_hour",
            "tag")

        combined_features_to_5G = twog_features.union(fiveg_features)
        combined_features = combined_features_to_5G.union(sixg_features)

        self.spark.sql("DROP TABLE IF EXISTS default.iptv_whix_raw_staging_two")

        combined_client_staging = combined_features.groupBy("accountSourceId",
                                                            "deviceSourceId",
                                                            "AccountId",
                                                            "GW_MODEL",
                                                            "make",
                                                            "postalCode",
                                                            "model",
                                                            "clientMac",
                                                            "tag",
                                                            "received_hour").agg(max(col("rssi")).alias("rssi"),
                                                                                 max(col("channel_width")).alias(
                                                                                     "channel_width"),
                                                                                 max(col("noise")).alias("noise"),
                                                                                 max(col("packets_sent")).alias(
                                                                                     "packets_sent"),
                                                                                 max(col("snr")).alias("snr"),
                                                                                 max(col("maxtx")).alias("maxtx"),
                                                                                 max(col("maxrx")).alias("maxrx"),
                                                                                 max(col("tx")).alias("tx"),
                                                                                 max(col("rx")).alias("rx"),
                                                                                 max(col("errorsent")).alias(
                                                                                     "errorsent"),
                                                                                 max(col(
                                                                                     "rapid_reconnects_count")).alias(
                                                                                     "rapid_reconnects_count"),
                                                                                 max(col("retransmissions")).alias(
                                                                                     "retransmissions"),
                                                                                 max(col("eventCode")).alias(
                                                                                     "eventCode"),
                                                                                 max(col("eventCount")).alias(
                                                                                     "eventCount"),
                                                                                 max(col("channel_utilization")).alias(
                                                                                     "channel_utilization"),
                                                                                 max(col(
                                                                                     "WIFI_ERROR_Diag3DataCorrupt")).alias(
                                                                                     "diag3datacorrupt_error"),
                                                                                 max(col("LoadAvg_split")).alias(
                                                                                     "loadavg_split"),
                                                                                 max(col("reboot")).alias("reboot"),
                                                                                 max(col(
                                                                                     "thermal_ChipTemp_split")).alias(
                                                                                     "thermal_chiptemp_split"),
                                                                                 max(col(
                                                                                     "thermal_fanspeed_split")).alias(
                                                                                     "thermal_fanspeed_split"),
                                                                                 max(col("UPTIME_split")).alias(
                                                                                     "uptime_split"),
                                                                                 max(col(
                                                                                     "WIFI_INFO_clientdisconnect")).alias(
                                                                                     "wifi_info_clientdisconnect"),
                                                                                 max(col("Total_Clients")).alias(
                                                                                     "total_clients")). \
            withColumn("client_type", lit("connected_client"))

        pods_features_twog = flattening_rdk.select("accountSourceId",
                                                   "deviceSourceId",
                                                   "AccountId",
                                                   "GW_MODEL",
                                                   "make",
                                                   "postalCode",
                                                   "model",
                                                   "2GPodMac_split",
                                                   "2GPodRSSI_split",
                                                   "received_hour"). \
            withColumnRenamed("2GPodMac_split", "clientMac"). \
            withColumnRenamed("2GPodRSSI_split", "rssi"). \
            withColumn("tag", lit("2G"))

        pods_features_fiveg = flattening_rdk.select("accountSourceId",
                                                    "deviceSourceId",
                                                    "AccountId",
                                                    "GW_MODEL",
                                                    "make",
                                                    "postalCode",
                                                    "model",
                                                    "5GPodMac_split",
                                                    "5GPodRSSI_split",
                                                    "received_hour"). \
            withColumnRenamed("5GPodMac_split", "clientMac"). \
            withColumnRenamed("5GPodRSSI_split", "rssi"). \
            withColumn("tag", lit("5G"))

        pod_features = pods_features_twog.union(pods_features_fiveg)

        pods_combined = pod_features.groupBy("accountSourceId",
                                             "deviceSourceId",
                                             "AccountId",
                                             "GW_MODEL",
                                             "make",
                                             "postalCode",
                                             "model",
                                             "clientmac",
                                             "tag",
                                             "received_hour").agg(max(col("rssi"))). \
            withColumnRenamed("max(rssi)", "rssi"). \
            withColumn("channel_width", lit(0)). \
            withColumn("noise", lit(0)). \
            withColumn("packets_sent", lit(0)). \
            withColumn("snr", lit(0)). \
            withColumn("maxtx", lit(0)). \
            withColumn("maxrx", lit(0)). \
            withColumn("tx", lit(0)). \
            withColumn("rx", lit(0)). \
            withColumn("errorsent", lit(0)). \
            withColumn("rapid_reconnects_count", lit(0)). \
            withColumn("retransmissions", lit(0)). \
            withColumn("client_type", lit("pod")). \
            withColumn("retransmissions", lit(0)). \
            withColumn("eventCode", lit(0)). \
            withColumn("eventCount", lit(0)). \
            withColumn("channel_utilization", lit(0)). \
            withColumn("diag3datacorrupt_error", lit(0)). \
            withColumn("loadavg_split", lit(0)). \
            withColumn("reboot", lit(0)). \
            withColumn("thermal_chiptemp_split", lit(0)). \
            withColumn("thermal_fanspeed_split", lit(0)). \
            withColumn("uptime_split", lit(0)). \
            withColumn("wifi_info_clientdisconnect", lit(0)). \
            withColumn("total_clients", lit(0))

        combined_clients = combined_client_staging.select("accountSourceId",
                                                          "deviceSourceId",
                                                          "AccountId",
                                                          "GW_MODEL",
                                                          "make",
                                                          "postalCode",
                                                          "model",
                                                          "clientMac",
                                                          "received_hour",
                                                          "rssi",
                                                          "channel_width",
                                                          "noise",
                                                          "packets_sent",
                                                          "snr",
                                                          "maxtx",
                                                          "maxrx",
                                                          "tx",
                                                          "rx",
                                                          "errorsent",
                                                          "rapid_reconnects_count",
                                                          "retransmissions",
                                                          "eventcode",
                                                          "eventCount",
                                                          "channel_utilization",
                                                          "tag",
                                                          "diag3datacorrupt_error",
                                                          "loadavg_split",
                                                          "reboot",
                                                          "thermal_chiptemp_split",
                                                          "thermal_fanspeed_split",
                                                          "uptime_split",
                                                          "wifi_info_clientdisconnect",
                                                          "total_clients",
                                                          "client_type").union(pods_combined.select("accountSourceId",
                                                                                                    "deviceSourceId",
                                                                                                    "AccountId",
                                                                                                    "GW_MODEL",
                                                                                                    "make",
                                                                                                    "postalCode",
                                                                                                    "model",
                                                                                                    "clientMac",
                                                                                                    "received_hour",
                                                                                                    "rssi",
                                                                                                    "channel_width",
                                                                                                    "noise",
                                                                                                    "packets_sent",
                                                                                                    "snr",
                                                                                                    "maxtx",
                                                                                                    "maxrx",
                                                                                                    "tx",
                                                                                                    "rx",
                                                                                                    "errorsent",
                                                                                                    "rapid_reconnects_count",
                                                                                                    "retransmissions",
                                                                                                    "eventcode",
                                                                                                    "eventCount",
                                                                                                    "channel_utilization",
                                                                                                    "tag",
                                                                                                    "diag3datacorrupt_error",
                                                                                                    "loadavg_split",
                                                                                                    "reboot",
                                                                                                    "thermal_chiptemp_split",
                                                                                                    "thermal_fanspeed_split",
                                                                                                    "uptime_split",
                                                                                                    "wifi_info_clientdisconnect",
                                                                                                    "total_clients",
                                                                                                    "client_type")). \
            withColumn("clientMac", func.upper(col("clientMac")))


        cujo_devices = self.obj.get_data("default.rdkb_wifi_fact_device_table", ["mac",
                                                                                 "deviceType",
                                                                                 "deviceModel",
                                                                                 "interfaceName",
                                                                                 "streamingDevice",
                                                                                 "brand",
                                                                                 "received_date"]). \
            filter(func.col("received_date") == func.date_sub(func.lit(run_date), 3))

        cujo_devices_groupby = cujo_devices.groupBy("mac", "received_date").agg(
            func.max(col("deviceType")),
            func.max(col("deviceModel")),
            func.max(col("streamingDevice")),
            func.max(col("brand"))). \
            withColumnRenamed("mac", "clientMac"). \
            withColumnRenamed("max(deviceType)", "deviceType"). \
            withColumnRenamed("max(deviceModel)", "deviceModel"). \
            withColumnRenamed("max(streamingDevice)", "streamingDevice"). \
            withColumnRenamed("max(brand)", "brand")

        self.obj.join_two_frames(combined_clients, cujo_devices_groupby, "left", "clientMac"). \
            select("accountSourceId",
                   "deviceSourceId",
                   "AccountId",
                   "GW_MODEL",
                   "make",
                   "postalCode",
                   "model",
                   "clientMac",
                   "received_hour",
                   "rssi",
                   "channel_width",
                   "noise",
                   "packets_sent",
                   "snr",
                   "maxtx",
                   "maxrx",
                   "tx",
                   "rx",
                   "errorsent",
                   "rapid_reconnects_count",
                   "retransmissions",
                   "eventcode",
                   "eventCount",
                   "channel_utilization",
                   "tag",
                   "diag3datacorrupt_error",
                   "loadavg_split",
                   "reboot",
                   "thermal_chiptemp_split",
                   "thermal_fanspeed_split",
                   "uptime_split",
                   "wifi_info_clientdisconnect",
                   "total_clients",
                   "client_type",
                   "deviceType",
                   "deviceModel",
                   "streamingDevice",
                   "brand"). \
            filter(col("clientMac") != "").\
            write.saveAsTable("default.iptv_whix_raw_staging_two")

        return True

        # # tier_df = self.obj.get_data("hem.modem_attainability_daily",["CM_MAC_ADDR",
        # #                                                              "TIER",
        # #                                                              "event_date"]).\
        # #     withColumn("deviceSourceId", func.regexp_replace(func.upper(col("CM_MAC_ADDR")),"-",":")).\
        # #     withColumnRenamed("TIER","tier").\
        # #     filter((func.to_date(func.col("event_date")) == date_sub(func.lit(run_date),2)))
        # #
        # # tier_df_groupBy = tier_df.groupBy("deviceSourceId").agg(max(col("tier")).alias("tier"))
        # #
        # # self.obj.join_two_frames(conn_devices, tier_df_groupBy, "left" , ["deviceSourceId"]).\
        # #     select("accountSourceId",
        # #                                "deviceSourceId",
        # #                                "AccountId",
        # #                                "GW_MODEL",
        # #                                "make",
        # #                                "postalCode",
        # #                                "model",
        # #                                "clientMac",
        # #                                "rssi",
        # #                                "channel_width",
        # #                                "noise",
        # #                                "packets_sent",
        # #                                "snr",
        # #                                "maxtx",
        # #                                "maxrx",
        # #                                "tx",
        # #                                "rx",
        # #                                "tx_dt",
        # #                                "rx_dt",
        # #                                "errorsent",
        # #                                "rapid_reconnects_mac",
        # #                                "rapid_reconnects_count",
        # #                                "retransmissions",
        # #                                "packet_loss",
        # #                                "eventcode",
        # #                                "eventCount",
        # #                                "channel_utilization",
        # #                                "tag",
        # #                                "diag3datacorrupt_error",
        # #                                "loadavg_split",
        # #                                "reboot",
        # #                                "thermal_chiptemp_split",
        # #                                "thermal_fanspeed_split",
        # #                                "uptime_split",
        # #                                "wifi_info_clientdisconnect",
        # #                                "total_clients",
        # #                                "client_type",
        # #                                "deviceType",
        # #                                "deviceModel",
        # #                                "streamingDevice",
        # #                                "brand",
        # #                                "tier",
        # #                                "received_hour").\
        # # write.saveAsTable("default.rdkb_wifi_fact_table_combined_client_staging")
        #
        # return True
        #
















