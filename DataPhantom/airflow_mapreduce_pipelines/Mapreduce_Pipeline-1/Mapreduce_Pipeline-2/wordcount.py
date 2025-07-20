from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("LocalWordCountJob")
        .master("local[*]")  # ✅ Local execution
        .getOrCreate()
    )

    sc = spark.sparkContext

    rdd = sc.textFile(input_path)
    words = rdd.flatMap(lambda line: line.split())
    wordPairs = words.map(lambda word: (word, 1))
    wordCounts = wordPairs.reduceByKey(lambda a, b: a + b)
    wordCounts.saveAsTextFile(output_path)

    spark.stop()