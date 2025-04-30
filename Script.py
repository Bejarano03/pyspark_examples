from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

spark = SparkSession.builder.appName("demo").getOrCreate()

df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
    ],
    ["first_name","age"]
)

print("Original Dataframe")
df.show()

df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)

print("Added column")
df1.show()

print("Filter dataframe")
df1.where(col("life_stage").isin(["teenager", "adult"])).show()

print("Average Age")
df1.select(avg("age")).show()

print("SQL Query")
spark.sql("select avg(age) from {df1}", df1=df1).show()

print("Life stage average using SQL")
spark.sql("select life_stage, avg(age) from {df1} group by life_stage", df1=df1).show()

print("Parquet Table")
df1.write.saveAsTable("some_people")
spark.sql("select * from some_people").show()

print("Inserting row")
spark.sql("INSERT INTO some_people VALUES ('frank', 4, 'child')")
spark.sql("select * from some_people").show()

print("Query to return teenagers")
spark.sql("select * from some_people where life_stage='teenager'").show()

text_file = spark.sparkContext.textFile("some_words.txt")

counts = (
    text_file.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

print("RDD Results")
print(counts.collect())