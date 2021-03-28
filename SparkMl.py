import pyspark as ps
from pyspark.sql import SparkSession, functions
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# https://towardsdatascience.com/sentiment-analysis-with-pyspark-bc8e83f80c35
# https://www.kaggle.com/kazanova/sentiment140
if __name__ == "__main__":
    conf = ps.SparkConf().setMaster("local[*]").setAppName("WordCount")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    textfile_df = spark.read.format("csv").option("header", "false").load("training.1600000.processed.noemoticon.csv")
    textfile_df = textfile_df \
        .withColumn("index", functions.monotonically_increasing_id()) \
        .withColumnRenamed("_c0", "target") \
        .withColumnRenamed("_c5", "value")
    textfile_df = textfile_df.select("index", "value", "target")

    textfile_df.show(5)

    print("Number of entries: ", textfile_df.count())
    (train_set, val_set, test_set) = textfile_df.randomSplit([0.98, 0.01, 0.01], seed=2000)
    print("Number of train data: ", train_set.count())
    print("Number of eval data: ", val_set.count())
    print("Number of test data: ", test_set.count())

    tokenizer = Tokenizer(inputCol="value", outputCol="words")
    hashtf = HashingTF(numFeatures=2 ** 17, inputCol="words", outputCol='tf')
    idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5)  # minDocFreq: remove sparse terms
    label_stringIdx = StringIndexer(inputCol="target", outputCol="label")
    pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])

    pipelineFit = pipeline.fit(train_set)
    train_df = pipelineFit.transform(train_set)
    val_df = pipelineFit.transform(val_set)
    train_df.show(10)


    lr = LogisticRegression(maxIter=100)
    lrModel = lr.fit(train_df)
    predictions = lrModel.transform(val_df)
    predictions.show()
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    evaluator.evaluate(predictions)

    accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
    print(accuracy)
