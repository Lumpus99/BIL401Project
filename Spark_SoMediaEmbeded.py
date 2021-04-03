import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, udf
import pandas
from SparkMl_NGrams import cleanData
import findspark

findspark.init()


def readfile(filename):
    file_df = spark.read \
        .format("csv") \
        .option("header", "false") \
        .load(filename)

    file_df = file_df \
        .withColumn("index", monotonically_increasing_id()) \
        .withColumnRenamed("_c0", "target") \
        .withColumnRenamed("_c5", "text")
    file_df = file_df.select("index", "text", "target")

    file_df.show(5)

    convert_udf = udf(lambda z: cleanData(z))
    file_df = file_df.select("index", convert_udf(col("text")).alias("text"), "target")

    file_df.show(5)

    print("Number of entries: ", file_df.count())
    return file_df


if __name__ == '__main__':
    conf = ps.SparkConf() \
        .setMaster("local[*]") \
        .setAppName("Merging")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    while True:
        permission = input("Would you like to inspect a CSV File or download a file(CSV/DATA)? ")
        if permission == "CSV":
            csv_file = input("Please enter your csv file")
            # training.1600000.processed.noemoticon

            csv_data = readfile(csv_file)

            break
        elif permission == "DATA":
            while True:
                yot = input("Would you like to gather YouTube or Twitter a file(YOUTUBE/TWITTER)? ")
                if yot == "YOUTUBE":

                    break
                elif yot == "TWITTER":
                    break
                else:
                    print("Please enter YOUTUBE or TWITTER")
            break
        else:
            print("Please enter CSV or DATA")
