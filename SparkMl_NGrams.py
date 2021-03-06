import re
from timeit import default_timer as timer

import pyspark as ps
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IDF, Tokenizer, NGram, VectorAssembler, CountVectorizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, udf


def build_ngrams_wocs(n=3):
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    ngrams = [
        NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i))
        for i in range(1, n + 1)
    ]

    cv = [
        CountVectorizer(vocabSize=6000, inputCol="{0}_grams".format(i),
                        outputCol="{0}_termFrequency".format(i))
        for i in range(1, n + 1)
    ]
    idf = [IDF(inputCol="{0}_termFrequency".format(i), outputCol="{0}_termFrequencyIdf".format(i), minDocFreq=6) for i in range(1, n + 1)]

    assembler = [VectorAssembler(
        inputCols=["{0}_termFrequencyIdf".format(i) for i in range(1, n + 1)],
        outputCol="features"
    )]

    lr = [LogisticRegression(labelCol="target")]
    return Pipeline(stages=tokenizer + ngrams + cv + idf + assembler + lr)


def cleanData(text):
    usr = re.sub(r'@[^\s]+', '', text)  # remove users
    link = re.sub(r"http\S+", '', usr)  # remove links
    return ' '.join(link.split()).strip()


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


def fit_data(spark, train=1.0, test=0.0):
    start = timer()

    file_df = readfile("training.1600000.processed.noemoticon.csv")
    file_df = file_df.withColumn("target", file_df.target.astype("int"))

    if train == 1.0:
        train_set = file_df
        trigram_pipelinefit = build_ngrams_wocs().fit(train_set)

    else:
        (train_set, test_set) = file_df.randomSplit([train, test], seed=2000)

        print("Number of train data: ", train_set.count())
        print("Number of test data: ", test_set.count())

        trigram_pipelinefit = build_ngrams_wocs().fit(train_set)
        predictions = trigram_pipelinefit.transform(test_set)
        predictions.sort('target', ascending=False).show(50)
        accuracy = predictions.filter(predictions.target == predictions.prediction).count() / float(test_set.count())
        print("Accuracy Score: {0:.4f}".format(accuracy))

    # save the model to disk
    trigram_pipelinefit.write().overwrite().save('finalized_model')

    end = timer()
    print("Total time elapsed: {0:.2f} seconds.".format(end - start))


if __name__ == '__main__':
    conf = ps.SparkConf() \
        .setMaster("local[*]") \
        .setAppName("TrainData")

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    fit_data(spark, 0.8, 0.2)
    """
    Accuracy Score: 0.8068
    ROC-AUC: 0.8819
    Total time elapsed: 332.03 seconds.


    Logistic:
    Accuracy Score: 0.7994
    Total time elapsed: 226.37 seconds.
    
    

    Accuracy Score: 0.3847
    ROC-AUC: 0.8779
    Total time elapsed: 392.58 seconds.
    """
    """
    # do something with the model
    file_df = readfile("training.1600000.processed.noemoticon.csv")
    (train_set, test_set) = file_df.randomSplit([0.8, 0.2], seed=2000)

    model = PipelineModel.load('finalized_model')

    predictions = model.transform(test_set)
    predictions.show(50)
    """