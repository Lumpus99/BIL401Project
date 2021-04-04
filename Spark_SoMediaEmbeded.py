import json
import csv
import pyspark as ps
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, udf, avg
from SparkMl_NGrams import cleanData
import findspark
import shutil

from main import youtubeQueryExample, twitterQueryExample

findspark.init()


def readfile(filename):
    try:
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
    except:
        print("File cannot found!!")


if __name__ == '__main__':

    with open('api_tokens.json') as f:
        tokens = json.load(f)
    while True:
        permission = input("Would you like to inspect a CSV File or download a file(CSV/DATA)? ")
        if permission == "CSV":
            conf = ps.SparkConf() \
                .setMaster("local[*]") \
                .setAppName("Merging")
            spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
            csv_file = input("Please enter your csv file=  ") #training.1600000.processed.noemoticon.csv

            csv_data = readfile("D:\\PyCharmProjects\\BIL401Project\\csv_files\\" + csv_file)
            if csv_data is None:
                print("Failed to File Open...")
            else:
                (train_set, test_set) = csv_data.randomSplit([0.9, 0.1], seed=2000)

                model = PipelineModel.load('finalized_model')

                predictions = model.transform(test_set)
                predictions.select(avg("prediction")).show()
            break
        elif permission == "DATA":
            while True:
                yot = input("Would you like to gather YouTube or Twitter a file(YOUTUBE/TWITTER)? ")
                if yot == "YOUTUBE":  # SdODv77CtIA
                    while True:
                        link = input("Enter your link's part after ?v=(DON'T INCLUDE IT)!!  ")
                        y_array = youtubeQueryExample(link, tokens['youtube_api_key'])
                        if y_array is None:
                            print("Writing to CSV failed...")
                        else:
                            fieldnames = ['index', 'text', 'target']
                            with open('youtube_file.csv', 'w') as csvfile:
                                filewriter = csv.writer(csvfile)
                                filewriter.writerow(['index', 'text', 'target'])
                                for i in range(0, len(y_array)):
                                    filewriter.writerow([i, y_array[i], 0])
                            shutil.move("D:\\PyCharmProjects\\BIL401Project\\youtube_file.csv",
                                        "D:\\PyCharmProjects\\BIL401Project\\csv_files\\youtube_file.csv")
                        break
                    break
                elif yot == "TWITTER":
                    while True:
                        anyth = input("Enter any keyword= ")
                        t_array = twitterQueryExample(anyth, tokens['twitter_bearer_token'])
                        if t_array is None:
                            print("Writing to CSV failed...")
                        else:
                            fieldnames = ['index', 'text', 'target']
                            with open('twitter_file.csv', 'w') as csvfile:
                                filewriter = csv.writer(csvfile)
                                filewriter.writerow(['index', 'text', 'target'])
                                for i in range(0, len(t_array)):
                                    filewriter.writerow([i, t_array[i], 0])
                            shutil.move("D:\\PyCharmProjects\\BIL401Project\\twitter_file.csv",
                                        "D:\\PyCharmProjects\\BIL401Project\\csv_files\\twitter_file.csv")

                        break
                    break
                else:
                    print("Please enter YOUTUBE or TWITTER")
            break
        else:
            print("Please enter CSV or DATA")
