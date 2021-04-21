import csv
import json
import re
import shutil

import pyspark as ps
from main import youtubeQueryExample, twitterQueryExample
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg


def cleanData(text):
    usr = re.sub(r'@[^\s]+', '', text)  # remove users
    link = re.sub(r"http\S+", '', usr)  # remove links
    punctuations = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~1234567890"
    lowercase_str = link.lower()
    for char in punctuations:
        lowercase_str = lowercase_str.replace(char, '')
    return ' '.join(lowercase_str.split()).strip()


def readfile(filename):
    try:
        file_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .load(filename)
        file_df = file_df.select("index", "text", "target")

        file_df.show(5)

        convert_udf = udf(lambda z: cleanData(z))
        file_df = file_df.select("index", convert_udf(col("text")).alias("text"), "target")

        file_df.show(50)

        print("Number of entries: ", file_df.count())
        return file_df
    except:
        print("File cannot found!!")


def runSpark():
    with open('api_tokens.json') as f:
        tokens = json.load(f)
    while True:
        permission = input("Would you like to inspect a CSV File or download a file.\n1:Read a CSV\n2:Download Data\n?")
        if permission == "1":

            csv_file = input("Please enter your csv file name=  ")  # training.1600000.processed.noemoticon.csv

            csv_data = readfile("D:\\PyCharmProjects\\BIL401Project\\csv_files\\" + csv_file)
            if csv_data is None:
                print("Failed to File Open...")
            else:

                model = PipelineModel.load('finalized_model')
                predictions = model.transform(csv_data)
                predictions.select("text", "prediction").sort('prediction', ascending=False)\
                    .toPandas().to_csv("D:\\PyCharmProjects\\BIL401Project\\results_csv_files\\results_" + csv_file)
                predictions.select(avg("prediction")).show()


            break
        elif permission == "2":
            while True:
                yot = input("Would you like to gather YouTube or Twitter a file\n1:YOUTUBE\n2:TWITTER\n?")
                num = input("How many data you want to query?")

                if yot == "1":  # SdODv77CtIA Youtube
                    while True:
                        link = input("Enter youtube video id:")
                        y_array = youtubeQueryExample(link, tokens['youtube_api_key'], num)
                        if y_array is None:
                            print("Writing to CSV failed...")
                        else:
                            write_csv("youtube_file.csv", y_array)
                        break
                    break
                elif yot == "2":  # Twitter
                    while True:
                        anyth = input("Enter twitter search query:")
                        t_array = twitterQueryExample(anyth, tokens['twitter_bearer_token'], num)
                        if t_array is None:
                            print("Writing to CSV failed...")
                        else:
                            write_csv("twitter_file.csv", t_array)
                        break
                    break
                else:
                    print("Please enter 1 for YOUTUBE or 2 for TWITTER")
            break
        else:
            print("Please enter 1 for CSV or 2 for DATA")


def write_csv(filename, data):
    with open(filename, 'w', newline='') as csvfile:
        filewriter = csv.writer(csvfile)
        filewriter.writerow(['index', 'text', 'target'])
        for i in range(0, len(data)):
            filewriter.writerow([i, data[i], 0])
    shutil.move("D:\\PyCharmProjects\\BIL401Project\\" + filename,
                "D:\\PyCharmProjects\\BIL401Project\\csv_files\\" + filename)


if __name__ == '__main__':
    conf = ps.SparkConf() \
        .setMaster("local[*]") \
        .setAppName("Merging")
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    runSpark()
