from scoring.preprocess  import preprocessor
from scoring.config import config
from pyspark.sql import functions as func
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from datetime import datetime
from pyspark.sql.functions import *
from pytz import timezone
from datetime import datetime, timedelta


class scoring_calculation:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.tz = timezone('EST')

        # Usage Threshold

        self.usage_threshold=0.60
        self.podsminrssi = -100
        self.podsmaxrssi = -1
        # Parameterization of RSSI

        self.xb6aminrssi_lower_twog=-77
        self.xb6amaxrssi_lower_twog=-30
        self.xb6aminrssi_middle_twog=-81
        self.xb6amaxrssi_middle_twog=-30
        self.xb6aminrssi_high_twog=-72
        self.xb6amaxrssi_high_twog=-30

        self.xb6aminrssi_lower_fiveg=-89
        self.xb6amaxrssi_lower_fiveg=-30
        self.xb6aminrssi_middle_fiveg = -95
        self.xb6amaxrssi_middle_fiveg = -30
        self.xb6aminrssi_high_fiveg = -78
        self.xb6amaxrssi_high_fiveg = -30

        self.xb6tminrssi_lower_twog = -77
        self.xb6tmaxrssi_lower_twog = -30
        self.xb6tminrssi_middle_twog = -78
        self.xb6tmaxrssi_middle_twog = -30
        self.xb6tminrssi_high_twog = -75
        self.xb6tmaxrssi_high_twog = -30

        self.xb6tminrssi_lower_fiveg = -87
        self.xb6tmaxrssi_lower_fiveg = -30
        self.xb6tminrssi_middle_fiveg = -88
        self.xb6tmaxrssi_middle_fiveg = -30
        self.xb6tminrssi_high_fiveg = -82
        self.xb6tmaxrssi_high_fiveg = -30

        self.xb7cminrssi_lower_twog = -84
        self.xb7cmaxrssi_lower_twog = -30
        self.xb7cminrssi_middle_twog = -85
        self.xb7cmaxrssi_middle_twog = -30
        self.xb7cminrssi_high_twog = -82
        self.xb7cmaxrssi_high_twog = -30

        self.xb7cminrssi_lower_fiveg = -94
        self.xb7cmaxrssi_lower_fiveg = -30
        self.xb7cminrssi_middle_fiveg = -94
        self.xb7cmaxrssi_middle_fiveg = -30
        self.xb7cminrssi_high_fiveg = -85
        self.xb7cmaxrssi_high_fiveg = -30

        self.xb7tminrssi_lower_twog = -84
        self.xb7tmaxrssi_lower_twog = -30
        self.xb7tminrssi_middle_twog = -85
        self.xb7tmaxrssi_middle_twog = -30
        self.xb7tminrssi_high_twog = -82
        self.xb7tmaxrssi_high_twog = -30

        self.xb7tminrssi_lower_fiveg = -94
        self.xb7tmaxrssi_lower_fiveg = -30
        self.xb7tminrssi_middle_fiveg = -94
        self.xb7tmaxrssi_middle_fiveg = -30
        self.xb7tminrssi_high_fiveg = -85
        self.xb7tmaxrssi_high_fiveg = -30

        self.xb8tminrssi_lower_twog = -84
        self.xb8tmaxrssi_lower_twog = -30
        self.xb8tminrssi_middle_twog = -88
        self.xb8tmaxrssi_middle_twog = -30
        self.xb8tminrssi_high_twog = -75
        self.xb8tmaxrssi_high_twog = -30

        self.xb8tminrssi_lower_fiveg = -87
        self.xb8tmaxrssi_lower_fiveg = -30
        self.xb8tminrssi_middle_fiveg = -90
        self.xb8tmaxrssi_middle_fiveg = -30
        self.xb8tminrssi_high_fiveg = -84
        self.xb8tmaxrssi_high_fiveg = -30

        self.xb8tminrssi_lower_sixg = -87
        self.xb8tmaxrssi_lower_sixg = -30
        self.xb8tminrssi_middle_sixg = -90
        self.xb8tmaxrssi_middle_sixg = -30
        self.xb8tminrssi_high_sixg = -84
        self.xb8tmaxrssi_high_sixg = -30

        # Parameterization of SNR

        self.xb6aminsnr_lower_twog = 10
        self.xb6amaxsnr_lower_twog = 30
        self.xb6aminsnr_middle_twog = 10
        self.xb6amaxsnr_middle_twog = 30
        self.xb6aminsnr_high_twog = 10
        self.xb6amaxsnr_high_twog = 30

        self.xb6aminsnr_lower_fiveg = 10
        self.xb6amaxsnr_lower_fiveg = 30
        self.xb6aminsnr_middle_fiveg = 10
        self.xb6amaxsnr_middle_fiveg = 30
        self.xb6aminsnr_high_fiveg = 10
        self.xb6amaxsnr_high_fiveg = 30

        self.xb6tminsnr_lower_twog = 10
        self.xb6tmaxsnr_lower_twog = 30
        self.xb6tminsnr_middle_twog = 10
        self.xb6tmaxsnr_middle_twog = 30
        self.xb6tminsnr_high_twog = 10
        self.xb6tmaxsnr_high_twog = 30

        self.xb6tminsnr_lower_fiveg = 10
        self.xb6tmaxsnr_lower_fiveg = 30
        self.xb6tminsnr_middle_fiveg = 10
        self.xb6tmaxsnr_middle_fiveg = 30
        self.xb6tminsnr_high_fiveg = 10
        self.xb6tmaxsnr_high_fiveg = 30

        self.xb7cminsnr_lower_twog = 10
        self.xb7cmaxsnr_lower_twog = 30
        self.xb7cminsnr_middle_twog = 10
        self.xb7cmaxsnr_middle_twog = 30
        self.xb7cminsnr_high_twog = 10
        self.xb7cmaxsnr_high_twog = 30

        self.xb7cminsnr_lower_fiveg = 10
        self.xb7cmaxsnr_lower_fiveg = 30
        self.xb7cminsnr_middle_fiveg = 10
        self.xb7cmaxsnr_middle_fiveg = 30
        self.xb7cminsnr_high_fiveg = 10
        self.xb7cmaxsnr_high_fiveg = 30

        self.xb7tminsnr_lower_twog = 10
        self.xb7tmaxsnr_lower_twog = 30
        self.xb7tminsnr_middle_twog = 10
        self.xb7tmaxsnr_middle_twog = 30
        self.xb7tminsnr_high_twog = 10
        self.xb7tmaxsnr_high_twog = 30

        self.xb7tminsnr_lower_fiveg = 10
        self.xb7tmaxsnr_lower_fiveg = 30
        self.xb7tminsnr_middle_fiveg = 10
        self.xb7tmaxsnr_middle_fiveg = 30
        self.xb7tminsnr_high_fiveg = 10
        self.xb7tmaxsnr_high_fiveg = 30

        self.xb8tminsnr_lower_twog = 10
        self.xb8tmaxsnr_lower_twog = 30
        self.xb8tminsnr_middle_twog = 10
        self.xb8tmaxsnr_middle_twog = 30
        self.xb8tminsnr_high_twog = 10
        self.xb8tmaxsnr_high_twog = 30

        self.xb8tminsnr_lower_fiveg = 10
        self.xb8tmaxsnr_lower_fiveg = 30
        self.xb8tminsnr_middle_fiveg = 10
        self.xb8tmaxsnr_middle_fiveg = 30
        self.xb8tminsnr_high_fiveg = 10
        self.xb8tmaxsnr_high_fiveg = 30

        self.xb8tminsnr_lower_sixg = 10
        self.xb8tmaxsnr_lower_sixg = 30
        self.xb8tminsnr_middle_sixg = 10
        self.xb8tmaxsnr_middle_sixg = 30
        self.xb8tminsnr_high_sixg = 10
        self.xb8tmaxsnr_high_sixg = 30

        self.maxutil2G=100
        self.minutil2G=60
        self.maxutil5G=100
        self.minutil5G=60
        self.maxutil6G=100
        self.minutil6G=60

        self.maxreboot=6
        self.minreboot=0

        self.rssi_coeff=0.30
        self.snr_coeff=0.25
        self.reboot_coeff=0.10
        self.ret_coeff=0.5
        self.rx_coeff=0.10
        self.tx_coeff=0.10
        self.util_coeff=0.10

        self.higher_modulation_higher_usage_devices_weight=0.40
        self.higher_modulation_lower_usage_devices_weight=0.25
        self.lower_modulation_higher_usage_devices_weight=0.30
        self.lower_modulation_lower_usage_devices_weight=0.10

    def __score__(self, run_date, hour):
        end_date = datetime.strptime(str(run_date) + "T" + str(hour) + ":00:00.000+0000", "%Y-%m-%dT%H:%M:%S.%f%z")
        start_date = end_date - timedelta(hours=24)
        print(start_date)
        print(end_date)

        self.spark.sql("DROP TABLE IF EXISTS default.ch_whix_staging_raw")

        wifi_happiness_index = self.obj.get_data("default.iptv_whix_raw_staging_two_historical", ["accountSourceId",
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
                                                                                                  "retransmissions",
                                                                                                  "channel_utilization",
                                                                                                  "tag",
                                                                                                  "reboot",
                                                                                                  "total_clients",
                                                                                                  "client_type",
                                                                                                  "deviceType",
                                                                                                  "deviceModel",
                                                                                                  "streamingDevice",
                                                                                                  "brand"]). \
            filter((col("received_hour") <= lit(end_date)) & (col("received_hour") >= lit(start_date))). \
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "AccountId",
                    "GW_MODEL",
                    "make",
                    "postalCode",
                    "model",
                    "clientMac",
                    "total_clients"). \
            agg(func.percentile_approx(col("rssi"), 0.5).alias("med_rssi"),
                func.max(col("channel_width")).alias("channel_width"),
                func.percentile_approx(col("noise"), 0.5).alias("noise"),
                func.sum(col("packets_sent")).alias("sum_usage"),
                func.percentile_approx(col("snr"), 0.5).alias("med_snr"),
                func.percentile_approx(col("maxtx"), 0.5).alias("med_maxtx"),
                func.percentile_approx(col("tx"), 0.5).alias("med_tx"),
                func.percentile_approx(col("maxrx"), 0.5).alias("med_maxrx"),
                func.percentile_approx(col("rx"), 0.5).alias("med_rx"),
                func.sum(col("retransmissions")).alias("sum_retransmissions"),
                func.percentile_approx(col("channel_utilization"), 0.5).alias("channel_utilization"),
                func.max(col("tag")).alias("tag"),
                func.sum(col("reboot")).alias("sum_reboot"),
                func.max(col("client_type")).alias("client_type"),
                func.first(col("deviceType")).alias("deviceType"),
                func.first(col("deviceModel")).alias("deviceModel"),
                func.first(col("streamingDevice")).alias("streamingDevice"),
                func.first(col("brand")).alias("brand")). \
            withColumn("device_class", func.when(col("med_maxrx") <= 144, "Type 1 Device"). \
                       otherwise(func.when((col("med_maxrx") > 144) & (col("med_maxrx") <= 444), "Type 11 Device"). \
                                 otherwise(
            func.when((col("med_maxrx") > 444), "Type 111 Device").otherwise("unknown")))). \
            withColumn("deviceType",
                       func.when((col("deviceType").isNull()) | (col("deviceType") == ""), col("deviceType")). \
                       otherwise("unknown")). \
            withColumn("deviceModel",
                       func.when((col("deviceModel").isNull()) | (col("deviceModel") == ""), lit("unknown")). \
                       otherwise(col("deviceModel"))). \
            withColumn("streamingDevice",
                       func.when((col("streamingDevice").isNotNull()) | (col("streamingDevice") == ""), lit("unknown")). \
                       otherwise(col("streamingDevice").cast(StringType()))). \
            withColumn("brand", func.when((col("brand").isNull()) | (col("brand") == ""), lit("unknown")). \
                       otherwise(col("brand"))). \
            withColumn("received_hour", lit(end_date)). \
            withColumn("norm_rssi", func.when((col("GW_MODEL") == 'XB6-A') &
                                              (col("med_maxrx") <= 144) &
                                              (col("tag") == "2G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_lower_twog) / (
                        self.xb6amaxrssi_lower_twog - self.xb6aminrssi_lower_twog), 2)). \
                       otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                           (col("med_maxrx") <= 144) &
                                           (col("tag") == "5G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_lower_fiveg) / (
                        self.xb6amaxrssi_lower_fiveg - self.xb6aminrssi_lower_fiveg), 2)). \
                                 otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                     (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                     (col("tag") == "2G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_middle_twog) / (
                        self.xb6amaxrssi_middle_twog - self.xb6aminrssi_middle_twog), 2)). \
                                           otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                               (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                               (col("tag") == "5G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_middle_fiveg) / (
                        self.xb6amaxrssi_middle_fiveg - self.xb6aminrssi_middle_fiveg), 2)). \
                                                     otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                                         (col("med_maxrx") > 444) &
                                                                         (col("tag") == "2G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_high_twog) / (self.xb6amaxrssi_high_twog - self.xb6aminrssi_high_twog),
            2)). \
                                                               otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                                                   (col("med_maxrx") > 444) &
                                                                                   (col("tag") == "5G"), func.round(
            (col("med_rssi") - self.xb6aminrssi_high_fiveg) / (
                        self.xb6amaxrssi_high_fiveg - self.xb6aminrssi_high_fiveg), 2)). \
                                                                         otherwise(
            func.when((col("GW_MODEL") == 'XB6-T') &
                      (col("med_maxrx") <= 144) &
                      (col("tag") == "2G"), func.round((col("med_rssi") - self.xb6tminrssi_lower_twog) / (
                        self.xb6tmaxrssi_lower_twog - self.xb6tminrssi_lower_twog), 2)). \
            otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                (col("med_maxrx") <= 144) &
                                (col("tag") == "5G"), func.round((col("med_rssi") - self.xb6tminrssi_lower_fiveg) / (
                        self.xb6tmaxrssi_lower_fiveg - self.xb6tminrssi_lower_fiveg), 2)). \
                      otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                          (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                          (col("tag") == "2G"), func.round(
                (col("med_rssi") - self.xb6tminrssi_middle_twog) / (
                            self.xb6tmaxrssi_middle_twog - self.xb6tminrssi_middle_twog), 2)). \
                                otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                                    (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                    (col("tag") == "5G"), func.round(
                (col("med_rssi") - self.xb6tminrssi_middle_fiveg) / (
                            self.xb6tmaxrssi_middle_fiveg - self.xb6tminrssi_middle_fiveg), 2)). \
                                          otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                                              (col("med_maxrx") > 444) &
                                                              (col("tag") == "2G"), func.round(
                (col("med_rssi") - self.xb6tminrssi_high_twog) / (
                            self.xb6tmaxrssi_high_twog - self.xb6tminrssi_high_twog), 2)). \
                                                    otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                                                        (col("med_maxrx") > 444) &
                                                                        (col("tag") == "5G"), func.round(
                (col("med_rssi") - self.xb6tminrssi_high_fiveg) / (
                            self.xb6tmaxrssi_high_fiveg - self.xb6tminrssi_high_fiveg), 2)). \
                                                              otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                                                  (col("med_maxrx") <= 144) &
                                                                                  (col("tag") == "2G"), func.round(
                (col("med_rssi") - self.xb7cminrssi_lower_twog) / (
                            self.xb7cmaxrssi_lower_twog - self.xb7cminrssi_lower_twog), 2)). \
                                                                        otherwise(
                func.when((col("GW_MODEL") == 'XB7-C') &
                          (col("med_maxrx") <= 144) & (col("tag") == "5G"), func.round(
                    (col("med_rssi") - self.xb7cminrssi_lower_fiveg) / (
                                self.xb7cmaxrssi_lower_fiveg - self.xb7cminrssi_lower_fiveg), 2)). \
                otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                    (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                    (col("tag") == "2G"), func.round(
                    (col("med_rssi") - self.xb7cminrssi_middle_twog) / (
                                self.xb7cmaxrssi_middle_twog - self.xb7cminrssi_middle_twog), 2)). \
                          otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                              (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                              (col("tag") == "5G"), func.round(
                    (col("med_rssi") - self.xb7cminrssi_middle_fiveg) / (
                                self.xb7cmaxrssi_middle_fiveg - self.xb7cminrssi_middle_fiveg), 2)). \
                                    otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                        (col("med_maxrx") > 444) &
                                                        (col("tag") == "2G"), func.round(
                    (col("med_rssi") - self.xb7cminrssi_high_twog) / (
                                self.xb7cmaxrssi_high_twog - self.xb7cminrssi_high_twog), 2)). \
                                              otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                                  (col("med_maxrx") > 444) & (col("tag") == "5G"),
                                                                  func.round((col(
                                                                      "med_rssi") - self.xb7cminrssi_high_fiveg) / (
                                                                                         self.xb7cmaxrssi_high_fiveg - self.xb7cminrssi_high_fiveg),
                                                                             2)). \
                                                        otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                            (col("med_maxrx") <= 144) & (
                                                                                        col("tag") == "5G"), func.round(
                    (col("med_rssi") - self.xb7tminrssi_lower_fiveg) / (
                                self.xb7tmaxrssi_lower_fiveg - self.xb7tminrssi_lower_fiveg), 2)). \
                                                                  otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                                      (col("med_maxrx") <= 144) & (
                                                                                                  col("tag") == "2G"),
                                                                                      func.round((col(
                                                                                          "med_rssi") - self.xb7tminrssi_lower_twog) / (
                                                                                                             self.xb7tmaxrssi_lower_twog - self.xb7tminrssi_lower_twog),
                                                                                                 2)). \
                                                                            otherwise(
                    func.when((col("GW_MODEL") == 'XB7-T') &
                              (col("med_maxrx") <= 144) & (col("tag") == "5G"), func.round(
                        (col("med_rssi") - self.xb7tminrssi_lower_fiveg) / (
                                    self.xb7tmaxrssi_lower_fiveg - self.xb7tminrssi_lower_fiveg), 2)). \
                    otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                        (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                        (col("tag") == "2G"), func.round(
                        (col("med_rssi") - self.xb7tminrssi_middle_twog) / (
                                    self.xb7tmaxrssi_middle_twog - self.xb7tminrssi_middle_twog), 2)). \
                              otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                  (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                  (col("tag") == "5G"), func.round(
                        (col("med_rssi") - self.xb7tminrssi_middle_fiveg) / (
                                    self.xb7tmaxrssi_middle_fiveg - self.xb7tminrssi_middle_fiveg), 2)). \
                                        otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                            (col("med_maxrx") > 444) &
                                                            (col("tag") == "2G"), func.round(
                        (col("med_rssi") - self.xb7tminrssi_high_twog) / (
                                    self.xb7tmaxrssi_high_twog - self.xb7tminrssi_high_twog), 2)). \
                                                  otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                      (col("med_maxrx") > 444) &
                                                                      (col("tag") == "5G"), func.round(
                        (col("med_rssi") - self.xb7tminrssi_high_fiveg) / (
                                    self.xb7tmaxrssi_high_fiveg - self.xb7tminrssi_high_fiveg), 2)). \
                                                            otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                                (col("med_maxrx") <= 144) & (
                                                                                            col("tag") == "2G"),
                                                                                func.round((col(
                                                                                    "med_rssi") - self.xb8tminrssi_lower_twog) / (
                                                                                                       self.xb8tmaxrssi_lower_twog - self.xb8tminrssi_lower_twog),
                                                                                           2)). \
                                                                      otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                                          (col("med_maxrx") <= 144) &
                                                                                          (col("tag") == "5G"),
                                                                                          func.round((col(
                                                                                              "med_rssi") - self.xb8tminrssi_lower_fiveg) / (
                                                                                                                 self.xb8tmaxrssi_lower_fiveg - self.xb8tminrssi_lower_fiveg),
                                                                                                     2)). \
                                                                                otherwise(
                        func.when((col("GW_MODEL") == 'XB8-T') &
                                  (col("med_maxrx") <= 144) & (col("tag") == "6G"), func.round(
                            (col("med_rssi") - self.xb8tminrssi_lower_sixg) / (
                                        self.xb8tmaxrssi_lower_sixg - self.xb8tminrssi_lower_sixg), 2)). \
                        otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                            (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                            (col("tag") == "2G"), func.round(
                            (col("med_rssi") - self.xb8tminrssi_middle_twog) / (
                                        self.xb8tmaxrssi_middle_twog - self.xb8tminrssi_middle_twog), 2)). \
                                  otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                      (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                      (col("tag") == "5G"), func.round(
                            (col("med_rssi") - self.xb8tminrssi_middle_fiveg) / (
                                        self.xb8tmaxrssi_middle_fiveg - self.xb8tminrssi_middle_fiveg), 2)). \
                                            otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                                (col("tag") == "6G"), func.round(
                            (col("med_rssi") - self.xb8tminrssi_middle_sixg) / (
                                        self.xb8tmaxrssi_middle_sixg - self.xb8tminrssi_middle_sixg), 2)). \
                                                      otherwise(
                            func.when((col("GW_MODEL") == 'XB8-T') & (col("med_maxrx") > 444) &
                                      (col("tag") == "2G"), func.round(
                                (col("med_rssi") - self.xb8tminrssi_high_twog) / (
                                            self.xb8tmaxrssi_high_twog - self.xb8tminrssi_high_twog), 2)). \
                            otherwise(func.when((col("GW_MODEL") == 'XB8-T') & (col("med_maxrx") > 444) &
                                                (col("tag") == "5G"), func.round(
                                (col("med_rssi") - self.xb8tminrssi_high_fiveg) / (
                                            self.xb8tmaxrssi_high_fiveg - self.xb8tminrssi_high_fiveg), 2)). \
                                      otherwise(func.when((col("GW_MODEL") == 'XB8-T') & (col("med_maxrx") > 444) &
                                                          (col("tag") == "6G"), func.round(
                                (col("med_rssi") - self.xb8tminrssi_high_sixg) / (
                                            self.xb8tmaxrssi_high_sixg - self.xb8tminrssi_high_sixg), 2)). \
                                                otherwise(0))))))))))))))))))))))))))))))))))). \
            withColumn("norm_rssi", func.when(col("client_type") == "pod", func.round(
            1 - ((col("med_rssi") - self.podsminrssi) / (self.podsmaxrssi - self.podsminrssi)), 2)).otherwise(
            col("norm_rssi"))). \
            withColumn("norm_snr", func.when((col("GW_MODEL") == 'XB6-A') &
                                             (col("med_maxrx") <= 144) &
                                             (col("tag") == "2G"), func.round(
            (col("med_snr") - self.xb6aminsnr_lower_twog) / (self.xb6amaxsnr_lower_twog - self.xb6aminsnr_lower_twog),
            2)). \
                       otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                           (col("med_maxrx") <= 144) &
                                           (col("tag") == "5G"), func.round(
            (col("med_snr") - self.xb6aminsnr_lower_fiveg) / (
                        self.xb6amaxsnr_lower_fiveg - self.xb6aminsnr_lower_fiveg), 2)). \
                                 otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                     (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                     (col("tag") == "2G"), func.round(
            (col("med_snr") - self.xb6aminsnr_middle_twog) / (
                        self.xb6amaxsnr_middle_twog - self.xb6aminsnr_middle_twog), 2)). \
                                           otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                               (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                               (col("tag") == "5G"), func.round(
            (col("med_snr") - self.xb6aminsnr_middle_fiveg) / (
                        self.xb6amaxsnr_middle_fiveg - self.xb6aminsnr_middle_fiveg), 2)). \
                                                     otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                                         (col("med_maxrx") > 444) &
                                                                         (col("tag") == "2G"), func.round(
            (col("med_snr") - self.xb6aminsnr_high_twog) / (self.xb6amaxsnr_high_twog - self.xb6aminsnr_high_twog), 2)). \
                                                               otherwise(func.when((col("GW_MODEL") == 'XB6-A') &
                                                                                   (col("med_maxrx") > 444) &
                                                                                   (col("tag") == "5G"), func.round(
            (col("med_snr") - self.xb6aminsnr_high_fiveg) / (self.xb6amaxsnr_high_fiveg - self.xb6aminsnr_high_fiveg),
            2)). \
                                                                         otherwise(
            func.when((col("GW_MODEL") == 'XB6-T') &
                      (col("med_maxrx") <= 144) &
                      (col("tag") == "2G"), func.round((col("med_snr") - self.xb6tminsnr_lower_twog) / (
                        self.xb6tmaxsnr_lower_twog - self.xb6tminsnr_lower_twog), 2)). \
            otherwise(func.when((col("GW_MODEL") == 'XB6-T') & (col("med_maxrx") <= 144) &
                                (col("tag") == "5G"), func.round((col("med_snr") - self.xb6tminsnr_lower_fiveg) / (
                        self.xb6tmaxsnr_lower_fiveg - self.xb6tminsnr_lower_fiveg), 2)). \
                      otherwise(
                func.when((col("GW_MODEL") == 'XB6-T') & (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                          (col("tag") == "2G"), func.round((col("med_snr") - self.xb6tminsnr_middle_twog) / (
                            self.xb6tmaxsnr_middle_twog - self.xb6tminsnr_middle_twog), 2)). \
                otherwise(
                    func.when((col("GW_MODEL") == 'XB6-T') & (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                              (col("tag") == "5G"), func.round((col("med_snr") - self.xb6tminsnr_middle_fiveg) / (
                                self.xb6tmaxsnr_middle_fiveg - self.xb6tminsnr_middle_fiveg), 2)). \
                    otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                        (col("med_maxrx") > 444) &
                                        (col("tag") == "2G"), func.round(
                        (col("med_snr") - self.xb6tminsnr_high_twog) / (
                                    self.xb6tmaxsnr_high_twog - self.xb6tminsnr_high_twog), 2)). \
                              otherwise(func.when((col("GW_MODEL") == 'XB6-T') &
                                                  (col("med_maxrx") > 444) &
                                                  (col("tag") == "5G"), func.round(
                        (col("med_snr") - self.xb6tminsnr_high_fiveg) / (
                                    self.xb6tmaxsnr_high_fiveg - self.xb6tminsnr_high_fiveg), 2)). \
                                        otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                            (col("med_maxrx") <= 144) &
                                                            (col("tag") == "2G"), func.round(
                        (col("med_snr") - self.xb7cminsnr_lower_twog) / (
                                    self.xb7cmaxsnr_lower_twog - self.xb7cminsnr_lower_twog), 2)). \
                                                  otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                                      (col("med_maxrx") <= 144) & (
                                                                              col("tag") == "5G"), func.round(
                        (col("med_snr") - self.xb7cminsnr_lower_fiveg) / (
                                    self.xb7cmaxsnr_lower_fiveg - self.xb7cminsnr_lower_fiveg), 2)). \
                                                            otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                                                (col("med_maxrx") > 144) &
                                                                                (col("med_maxrx") <= 444) &
                                                                                (col("tag") == "2G"), func.round(
                        (col("med_snr") - self.xb7cminsnr_middle_twog) / (
                                    self.xb7cmaxsnr_middle_twog - self.xb7cminsnr_middle_twog), 2)). \
                                                                      otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                                                                          (col("med_maxrx") > 144) & (
                                                                                                      col(
                                                                                                          "med_maxrx") <= 444) &
                                                                                          (col("tag") == "5G"),
                                                                                          func.round((col(
                                                                                              "med_snr") - self.xb7cminsnr_middle_fiveg) / (
                                                                                                                 self.xb7cmaxsnr_middle_fiveg - self.xb7cminsnr_middle_fiveg),
                                                                                                     2)). \
                                                                                otherwise(
                        func.when((col("GW_MODEL") == 'XB7-C') &
                                  (col("med_maxrx") > 444) &
                                  (col("tag") == "2G"), func.round((col("med_snr") - self.xb7cminsnr_high_twog) / (
                                    self.xb7cmaxsnr_high_twog - self.xb7cminsnr_high_twog), 2)). \
                        otherwise(func.when((col("GW_MODEL") == 'XB7-C') &
                                            (col("med_maxrx") > 444) &
                                            (col("tag") == "5G"), func.round(
                            (col("med_snr") - self.xb7cminsnr_high_fiveg) / (
                                        self.xb7cmaxsnr_high_fiveg - self.xb7cminsnr_high_fiveg), 2)). \
                                  otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                      (col("med_maxrx") <= 144) &
                                                      (col("tag") == "5G"), func.round(
                            (col("med_snr") - self.xb7tminsnr_lower_fiveg) / (
                                        self.xb7tmaxsnr_lower_fiveg - self.xb7tminsnr_lower_fiveg), 2)). \
                                            otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                (col("med_maxrx") <= 144) &
                                                                (col("tag") == "2G"), func.round(
                            (col("med_snr") - self.xb7tminrssi_lower_twog) / (
                                        self.xb7tmaxrssi_lower_twog - self.xb7tminrssi_lower_twog), 2)). \
                                                      otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                          (col("med_maxrx") <= 144) &
                                                                          (col("tag") == "5G"), func.round(
                            (col("med_snr") - self.xb7tminsnr_lower_fiveg) / (
                                        self.xb7tmaxsnr_lower_fiveg - self.xb7tminsnr_lower_fiveg), 2)). \
                                                                otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                                                    (col("med_maxrx") > 144) & (col(
                            "med_maxrx") <= 444) &
                                                                                    (col("tag") == "2G"), func.round(
                            (col("med_snr") - self.xb7tminsnr_middle_twog) / (
                                        self.xb7tmaxsnr_middle_twog - self.xb7tminsnr_middle_twog), 2)). \
                                                                          otherwise(
                            func.when((col("GW_MODEL") == 'XB7-T') &
                                      (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                      (col("tag") == "5G"), func.round(
                                (col("med_snr") - self.xb7tminsnr_middle_fiveg) / (
                                            self.xb7tmaxsnr_middle_fiveg - self.xb7tminsnr_middle_fiveg), 2)). \
                            otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                (col("med_maxrx") > 444) &
                                                (col("tag") == "2G"), func.round(
                                (col("med_snr") - self.xb7tminsnr_high_twog) / (
                                            self.xb7tmaxsnr_high_twog - self.xb7tminsnr_high_twog), 2)). \
                                      otherwise(func.when((col("GW_MODEL") == 'XB7-T') &
                                                          (col("med_maxrx") > 444) &
                                                          (col("tag") == "5G"), func.round(
                                (col("med_snr") - self.xb7tminsnr_high_fiveg) / (
                                            self.xb7tmaxsnr_high_fiveg - self.xb7tminsnr_high_fiveg), 2)). \
                                                otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                    (col("med_maxrx") <= 144) &
                                                                    (col("tag") == "2G"), func.round(
                                (col("med_snr") - self.xb8tminsnr_lower_twog) / (
                                            self.xb8tmaxsnr_lower_twog - self.xb8tminsnr_lower_twog), 2)). \
                                                          otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                              (col("med_maxrx") <= 144) &
                                                                              (col("tag") == "5G"), func.round(
                                (col("med_snr") - self.xb8tminsnr_lower_fiveg) / (
                                            self.xb8tmaxsnr_lower_fiveg - self.xb8tminsnr_lower_fiveg), 2)). \
                                                                    otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                                        (col("med_maxrx") <= 144) & (
                                                                                                col("tag") == "6G"),
                                                                                        func.round((col(
                                                                                            "med_snr") - self.xb8tminsnr_lower_sixg) / (
                                                                                                               self.xb8tmaxsnr_lower_sixg - self.xb8tminsnr_lower_sixg),
                                                                                                   2)). \
                                                                              otherwise(
                                func.when((col("GW_MODEL") == 'XB8-T') &
                                          (col("med_maxrx") > 144) &
                                          (col("med_maxrx") <= 444) &
                                          (col("tag") == "2G"), func.round(
                                    (col("med_snr") - self.xb8tminsnr_middle_twog) / (
                                                self.xb8tmaxsnr_middle_twog - self.xb8tminsnr_middle_twog), 2)). \
                                otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                    (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                    (col("tag") == "5G"), func.round(
                                    (col("med_snr") - self.xb8tminsnr_middle_fiveg) / (
                                                self.xb8tmaxsnr_middle_fiveg - self.xb8tminsnr_middle_fiveg), 2)). \
                                          otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                              (col("med_maxrx") > 144) & (col("med_maxrx") <= 444) &
                                                              (col("tag") == "6G"), func.round(
                                    (col("med_snr") - self.xb8tminsnr_middle_sixg) / (
                                                self.xb8tmaxsnr_middle_sixg - self.xb8tminsnr_middle_sixg), 2)). \
                                                    otherwise(func.when((col("GW_MODEL") == 'XB8-T') &
                                                                        (col("med_maxrx") > 444) &
                                                                        (col("tag") == "2G"), func.round(
                                    (col("med_snr") - self.xb8tminsnr_high_twog) / (
                                                self.xb8tmaxsnr_high_twog - self.xb8tminsnr_high_twog), 2)). \
                                                              otherwise(
                                    func.when((col("GW_MODEL") == 'XB8-T') & (col("med_maxrx") > 444) &
                                              (col("tag") == "5G"), func.round(
                                        (col("med_snr") - self.xb8tminsnr_high_fiveg) / (
                                                    self.xb8tmaxsnr_high_fiveg - self.xb8tminsnr_high_fiveg), 2)). \
                                    otherwise(func.when((col("GW_MODEL") == 'XB8-T') & (col("med_maxrx") > 444) &
                                                        (col("tag") == "6G"), func.round(
                                        (col("med_snr") - self.xb8tminsnr_high_sixg) / (
                                                    self.xb8tmaxsnr_high_sixg - self.xb8tminsnr_high_sixg), 2)). \
                                              otherwise(0))))))))))))))))))))))))))))))))))). \
            withColumn("norm_rssi", func.when(col("norm_rssi") <= 0, 0).otherwise(
            func.when(col("norm_rssi") >= 1, 1).otherwise(col("norm_rssi")))). \
            withColumn("norm_snr", func.when(col("norm_snr") <= 0, 0).otherwise(
            func.when(col("norm_snr") >= 1, 1).otherwise(col("norm_rssi")))). \
            withColumn("norm_rx", func.round((col("med_tx") / col("med_maxtx")), 2)). \
            withColumn("norm_tx", func.round((col("med_rx") / col("med_maxrx")), 2)). \
            withColumn("ret_perc", func.when(round(1 - (col("sum_retransmissions") / col("sum_usage")), 2) > 1, 1). \
                       otherwise(
            func.when(func.round(1 - (col("sum_retransmissions") / col("sum_usage")), 2) < 0, 0).otherwise(
                func.round(1 - (col("sum_retransmissions") / col("sum_usage")), 2)))). \
            withColumn("norm_reboot",
                       func.round(1 - ((col("sum_reboot") - self.minreboot) / (self.maxreboot - self.minreboot)), 2)). \
            withColumn("norm_reboot", func.when((col("norm_reboot") <= 0) | (col("norm_reboot").isNull()), 0).otherwise(
            func.when(col("norm_reboot") >= 1, 1).otherwise(col("norm_reboot")))). \
            withColumn("norm_rssi", func.when(col("norm_rssi").isNotNull(), col("norm_rssi")).otherwise(0)). \
            withColumn("norm_snr", func.when(col("norm_snr").isNotNull(), col("norm_snr")).otherwise(0)). \
            withColumn("norm_rx", func.when(col("norm_rx").isNotNull(), col("norm_rx")).otherwise(0)). \
            withColumn("norm_tx", func.when(col("norm_tx").isNotNull(), col("norm_tx")).otherwise(0)). \
            withColumn("norm_reboot", func.when(col("norm_reboot").isNotNull(), col("norm_reboot")).otherwise(0))

        median_utilization = wifi_happiness_index.select("accountSourceId",
                                                         "deviceSourceId",
                                                         "AccountId",
                                                         "channel_utilization",
                                                         "tag",
                                                         "received_hour"). \
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "AccountId",
                    "tag"). \
            agg(func.percentile_approx(col("channel_utilization"), 0.5).alias("channel_utilization"))
        combined_wifi_happiness_index = self.obj.join_two_frames(wifi_happiness_index.drop("channel_utilization"),
                                                                 median_utilization, "inner", ["accountSourceId",
                                                                                               "deviceSourceId",
                                                                                               "AccountId",
                                                                                               "tag"]). \
            withColumn("norm_util", func.when(col("tag") == "2G", (func.round(
            1 - (col("channel_utilization").cast(IntegerType()) - self.minutil2G) / (self.maxutil2G - self.minutil2G),
            2))). \
                       otherwise(func.when(col("tag") == "5G", (func.round(
            1 - (col("channel_utilization").cast(IntegerType()) - self.minutil5G) / (self.maxutil5G - self.minutil5G),
            2))). \
                                 otherwise(func.when(col("tag") == "6G", (func.round(
            1 - (col("channel_utilization").cast(IntegerType()) - self.minutil6G) / (self.maxutil6G - self.minutil6G),
            2)))))). \
            withColumn("norm_util", func.when((col("norm_util") <= 0), 0).otherwise(
            func.when((col("norm_util") >= 1) | (col("norm_util").isNull()), 1).otherwise(col("norm_util")))). \
            withColumn("norm_util", func.when(col("norm_util").isNotNull(), col("norm_util")).otherwise(0)). \
            withColumn("WIFI_HAPPINESS_INDEX", func.round(100 * ((col("norm_rssi") * self.rssi_coeff) +
                                                                 (col("norm_snr") * self.snr_coeff) +
                                                                 (col("norm_rx") * self.rx_coeff) +
                                                                 (col("norm_tx") * self.tx_coeff) +
                                                                 (col("norm_util") * self.util_coeff) +
                                                                 (col("norm_reboot") * self.reboot_coeff)), 2)). \
            withColumn("WIFI_HAPPINESS_INDEX",
                       func.when(col("client_type") == "pod", func.round(100 * (col("norm_rssi") * 1))).otherwise(
                           col("WIFI_HAPPINESS_INDEX")))

        packets_percentage = wifi_happiness_index.groupBy("accountSourceId",
                                                          "deviceSourceId",
                                                          "AccountId",
                                                          "GW_MODEL").agg(func.sum("sum_usage").alias("highest_usage"))

        combined_wifi_packets = self.obj.join_two_frames(combined_wifi_happiness_index, packets_percentage, "inner",
                                                         ["accountSourceId",
                                                          "deviceSourceId",
                                                          "AccountId",
                                                          "GW_MODEL"]). \
            withColumn("packets_perc", func.round((col("sum_usage") / col("highest_usage")), 2)). \
            withColumn("higher_modulation_higher_usage_devices",
                       func.when((col("packets_perc") >= self.usage_threshold) & (col("med_maxtx") > 444),
                                 lit("1")).otherwise(0)). \
            withColumn("lower_modulation_lower_usage_devices",
                       func.when((col("packets_perc") < self.usage_threshold) & (col("med_maxtx") <= 444),
                                 lit("1")).otherwise(0)). \
            withColumn("higher_modulation_lower_usage_devices",
                       func.when((col("packets_perc") < self.usage_threshold) & (col("med_maxtx") > 444),
                                 lit("1")).otherwise(0)). \
            withColumn("lower_modulation_higher_usage_devices",
                       func.when((col("packets_perc") >= self.usage_threshold) & (col("med_maxtx") <= 444),
                                 lit("1")).otherwise(0))

        count_of_type_devices = combined_wifi_packets.groupBy("accountSourceId",
                                                              "deviceSourceId",
                                                              "AccountId",
                                                              "GW_MODEL",
                                                              "make",
                                                              "postalCode",
                                                              "model"). \
            agg(
            func.sum("higher_modulation_higher_usage_devices").alias("higher_modulation_higher_usage_devices_count"),
            func.sum("lower_modulation_lower_usage_devices").alias("lower_modulation_lower_usage_devices_count"),
            func.sum("higher_modulation_lower_usage_devices").alias("higher_modulation_lower_usage_devices_count"),
            func.sum("lower_modulation_higher_usage_devices").alias("lower_modulation_higher_usage_devices_count")). \
            withColumn("higher_modulation_higher_usage_devices_count",
                       func.when(col("higher_modulation_higher_usage_devices_count").isNotNull(),
                                 col("higher_modulation_higher_usage_devices_count")).otherwise(0)). \
            withColumn("higher_modulation_lower_usage_devices_count",
                       func.when(col("higher_modulation_lower_usage_devices_count").isNotNull(),
                                 col("higher_modulation_lower_usage_devices_count")).otherwise(0)). \
            withColumn("lower_modulation_higher_usage_devices_count",
                       func.when(col("lower_modulation_higher_usage_devices_count").isNotNull(),
                                 col("lower_modulation_higher_usage_devices_count")).otherwise(0)). \
            withColumn("lower_modulation_lower_usage_devices_count",
                       func.when(col("lower_modulation_lower_usage_devices_count").isNotNull(),
                                 col("lower_modulation_lower_usage_devices_count")).otherwise(0)). \
            withColumn("hmhu_flag",
                       func.when(col("higher_modulation_higher_usage_devices_count") == 0, 0).otherwise(1)). \
            withColumn("hmlu_flag", func.when(col("higher_modulation_lower_usage_devices_count") == 0, 0).otherwise(1)). \
            withColumn("lmhu_flag", func.when(col("lower_modulation_higher_usage_devices_count") == 0, 0).otherwise(1)). \
            withColumn("lmlu_flag", func.when(col("lower_modulation_lower_usage_devices_count") == 0, 0).otherwise(1)). \
            withColumn("distributor", func.when(col("higher_modulation_higher_usage_devices_count") == 0,
                                                lit(self.higher_modulation_higher_usage_devices_weight)).otherwise(0)). \
            withColumn("distributor", func.when(col("lower_modulation_lower_usage_devices_count") == 0,
                                                col("distributor") + lit(
                                                    self.lower_modulation_lower_usage_devices_weight)).otherwise(
            col("distributor"))). \
            withColumn("distributor", func.when(col("higher_modulation_lower_usage_devices_count") == 0,
                                                col("distributor") + lit(
                                                    self.higher_modulation_lower_usage_devices_weight)).otherwise(
            col("distributor"))). \
            withColumn("distributor", func.when(col("lower_modulation_higher_usage_devices_count") == 0,
                                                col("distributor") + lit(
                                                    self.lower_modulation_higher_usage_devices_weight)).otherwise(
            col("distributor"))). \
            withColumn("distributor", col("distributor") / (
                    col("hmhu_flag") + col("hmlu_flag") + col("lmhu_flag") + col("lmlu_flag"))). \
            withColumn("higher_modulation_higher_usage_weight", func.when(((
                                                                                       self.higher_modulation_higher_usage_devices_weight + col(
                                                                                   "distributor")) / func.col(
            "higher_modulation_higher_usage_devices_count")).isNull(), 0). \
                       otherwise(((self.higher_modulation_higher_usage_devices_weight + col("distributor")) / func.col(
            "higher_modulation_higher_usage_devices_count")))). \
            withColumn("higher_modulation_lower_usage_weight", func.when(((
                                                                                      self.higher_modulation_lower_usage_devices_weight + col(
                                                                                  "distributor")) / func.col(
            "higher_modulation_lower_usage_devices_count")).isNull(), 0). \
                       otherwise(((self.higher_modulation_lower_usage_devices_weight + col("distributor")) / func.col(
            "higher_modulation_lower_usage_devices_count")))). \
            withColumn("lower_modulation_lower_usage_weight", func.when(((
                                                                                     self.lower_modulation_lower_usage_devices_weight + col(
                                                                                 "distributor")) / func.col(
            "lower_modulation_lower_usage_devices_count")).isNull(), 0). \
                       otherwise(((self.lower_modulation_lower_usage_devices_weight + col("distributor")) / func.col(
            "lower_modulation_lower_usage_devices_count")))). \
            withColumn("lower_modulation_higher_usage_weight", func.when(((
                                                                                      self.lower_modulation_higher_usage_devices_weight + col(
                                                                                  "distributor")) / func.col(
            "lower_modulation_higher_usage_devices_count")).isNull(), 0). \
                       otherwise(((self.lower_modulation_higher_usage_devices_weight + col("distributor")) / func.col(
            "lower_modulation_higher_usage_devices_count")))). \
            withColumn("total_weights", (col("higher_modulation_higher_usage_devices_count") * col(
            "higher_modulation_higher_usage_weight")) +
                       (col("higher_modulation_lower_usage_devices_count") * col(
                           "higher_modulation_lower_usage_weight")) +
                       (col("lower_modulation_higher_usage_devices_count") * col(
                           "lower_modulation_higher_usage_weight")) +
                       (col("lower_modulation_lower_usage_devices_count") * col("lower_modulation_lower_usage_weight")))

        household_whix_pre = self.obj.join_two_frames(combined_wifi_packets, count_of_type_devices, "inner",
                                                      ["accountSourceId",
                                                       "deviceSourceId",
                                                       "AccountId",
                                                       "GW_MODEL",
                                                       "make",
                                                       "postalCode",
                                                       "model"]). \
            select("accountSourceId",
                   "deviceSourceId",
                   "AccountId",
                   "GW_MODEL",
                   "make",
                   "postalCode",
                   "model",
                   "clientMac",
                   "med_rssi",
                   "channel_width",
                   "noise",
                   "sum_usage",
                   "med_snr",
                   "med_maxtx",
                   "med_tx",
                   "med_maxrx",
                   "med_rx",
                   "sum_retransmissions",
                   "channel_utilization",
                   "tag",
                   "sum_reboot",
                   "total_clients",
                   "client_type",
                   "devicetype",
                   "deviceModel",
                   "streamingDevice",
                   "brand",
                   "device_class",
                   "received_hour",
                   "WIFI_HAPPINESS_INDEX",
                   "highest_usage",
                   "packets_perc",
                   "higher_modulation_higher_usage_weight",
                   "higher_modulation_lower_usage_weight",
                   "lower_modulation_lower_usage_weight",
                   "lower_modulation_higher_usage_weight",
                   "total_weights"). \
            withColumn("dot_product", func.when((col("med_maxtx") < 444) & (col("packets_perc") < 0.60), (
                    (col("WIFI_HAPPINESS_INDEX") / 100) * col("lower_modulation_lower_usage_weight"))). \
                       otherwise(func.when((col("med_maxtx") < 444) & (col("packets_perc") >= 0.60), (
                    (col("WIFI_HAPPINESS_INDEX") / 100) * col("lower_modulation_higher_usage_weight"))). \
                                 otherwise(func.when((col("med_maxtx") >= 444) & (col("packets_perc") < 0.60), (
                    (col("WIFI_HAPPINESS_INDEX") / 100) * col("higher_modulation_lower_usage_weight"))). \
                                           otherwise(
            func.when((col("med_maxtx") >= 444) & (col("packets_perc") >= 0.60),
                      ((col("WIFI_HAPPINESS_INDEX") / 100) * col("higher_modulation_higher_usage_weight"))))))). \
            groupBy("accountSourceId",
                    "deviceSourceId",
                    "AccountId",
                    "GW_MODEL",
                    "make",
                    "postalCode"). \
            agg(func.sum(col("dot_product")).alias("HOUSEHOLD_WHIX")). \
            withColumn("HOUSEHOLD_WHIX", func.round((func.col("HOUSEHOLD_WHIX") * 100), 2))

        combined_all = self.obj.join_three_frames(combined_wifi_packets, household_whix_pre, count_of_type_devices,
                                                  "inner", ["accountSourceId",
                                                            "deviceSourceId",
                                                            "AccountId",
                                                            "GW_MODEL",
                                                            "make",
                                                            "postalCode"]).drop("model"). \
            withColumn("HOUSEHOLD_WHIX", func.when(func.col("HOUSEHOLD_WHIX") >= 100, 100). \
                       otherwise(func.when(col("HOUSEHOLD_WHIX") < 0, 0).otherwise(col("HOUSEHOLD_WHIX"))))

        combined_all.select("accountSourceId",
                            "deviceSourceId",
                            "AccountId",
                            "GW_MODEL",
                            "make",
                            "postalCode",
                            "clientMac",
                            col("med_rssi").alias("rssi"),
                            col("channel_width").alias("channel_width"),
                            col("noise").alias("noise"),
                            col("sum_usage").alias("usage"),
                            col("med_snr").alias("snr"),
                            col("med_maxtx").alias("maxtx"),
                            col("med_tx").alias("tx"),
                            col("med_maxrx").alias("maxrx"),
                            col("sum_retransmissions").alias("retransmissions"),
                            "channel_utilization",
                            "tag",
                            col("sum_reboot").alias("reboot"),
                            "total_clients",
                            "client_type",
                            "deviceType",
                            "deviceModel",
                            "streamingDevice",
                            "brand",
                            "WIFI_HAPPINESS_INDEX",
                            "HOUSEHOLD_WHIX",
                            "received_hour").write. \
            saveAsTable("default.ch_whix_staging_raw")

        self.spark.sql(
            "INSERT INTO default.ch_whix_raw SELECT * from default.ch_whix_staging_raw")

        return True
