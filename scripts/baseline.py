"""
This is train test split and baseline solution using ALS apprach
"""
#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics





# In[2]:


WAREHOUSE = "/user/team20/project/hive/warehouse"
TEAM = "team20"

spark = SparkSession.builder.appName(f"{TEAM} - spark ML")\
    .master("yarn").config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", WAREHOUSE)\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .enableHiveSupport().getOrCreate()


# In[3]:


spark.sql("SHOW DATABASES").show()
spark.sql("USE team20_projectdb").show()
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM anime_part_buck LIMIT 10").show()


# In[4]:


print(spark.catalog.listTables("team20_projectdb"))


# In[5]:


spark.sql("SELECT count(*) FROM users_scores").show()


# In[6]:


# data = spark.sql("SELECT user_id, anime_id, rating FROM users_scores")
data = spark.sql("SELECT user_id, anime_id, rating FROM users_scores ORDER BY user_id LIMIT 5000")

data.show()


# inspired by
# https://medium.com/@brunoborges_38708/recommender-system-using-als-in-pyspark-10329e1d1ee1

user_window = Window.partitionBy("user_id").orderBy(F.col("anime_id").desc())

df_rec_filtered = data.withColumn("num_items", F.expr("count(*) over (partition by user_id)"))
df_rec_filtered = df_rec_filtered.filter(F.col("num_items")>=5)


# In[8]:





# For example, 30% of items will be masked
PERCENT_ITEMS_TO_MASK = 0.3


# Determine the number of items to mask for each user
df_rec_final = df_rec_filtered.withColumn("num_items_to_mask", (F\
    .col("num_items") * PERCENT_ITEMS_TO_MASK)\
    .cast("int"))

# Masks items for each user
# _tmp_window = Window.partitionBy("user_id").orderBy(F.rand(seed))
df_rec_final = df_rec_final.withColumn("item_rank", F.rank().over(user_window))

# Create a StringIndexer model to index the user ID column
# indexer_user = StringIndexer(inputCol='user_id', outputCol='userIndex').setHandleInvalid("keep")
# indexer_item = StringIndexer(inputCol='anime_id', outputCol='itemIndex').setHandleInvalid("keep")

# # Fit the indexer model to the data and transform the DataFrame
# df_rec_final = indexer_user.fit(df_rec_final).transform(df_rec_final)
# df_rec_final = indexer_item.fit(df_rec_final).transform(df_rec_final)

# Convert the userIndex column to integer type
# df_rec_final = df_rec_final.withColumn('userIndex', df_rec_final['userIndex'].cast('integer')) \
#     .withColumn('itemIndex', df_rec_final['itemIndex'].cast('integer'))

# Filter train and test DataFrames
train_df_rec = df_rec_final.filter(F.col("item_rank") > F.col("num_items_to_mask"))
test_df_rec = df_rec_final.filter(F.col("item_rank") <= F.col("num_items_to_mask"))
print(train_df_rec.count(), test_df_rec.count())


# In[9]:


train_df_rec.write.json("/user/team20/project/data/train.json")
test_df_rec.write.json("/user/team20/project/data/test.json")


# In[10]:


top_n_user = train_df_rec.groupBy("user_id").agg(F.count("rating").alias("n_watched"))\
    .orderBy(F.desc("n_watched")).limit(1000)
train_df_rec = train_df_rec.join(top_n_user, on="user_id", how="inner")
test_df_rec = test_df_rec.join(top_n_user, on="user_id", how="inner")

print(train_df_rec.count(), test_df_rec.count())


# In[11]:


top_n_user.show()


# In[12]:

# In[13]:


als = ALS(userCol='user_id', itemCol='anime_id', ratingCol='rating',
          coldStartStrategy='drop')

# param_grid = ParamGridBuilder()\
#              .addGrid(als.rank, [1, 20, 30])\
#              .addGrid(als.maxIter, [10, 20])\
#              .addGrid(als.regParam, [.05, .15])\
#              .build()

param_grid = ParamGridBuilder()\
    .addGrid(als.rank, [5, 20]).addGrid(als.maxIter, [10, 20])\
        .addGrid(als.regParam, [.05, .15]).build()

evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')

cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

model = cv.fit(train_df_rec)
best_model = model.bestModel



als = ALS(rank=best_model.rank, maxIter=best_model._java_obj.parent()\
    .getMaxIter(), regParam=best_model._java_obj.parent()\
    .getRegParam(), userCol="user_id", itemCol="anime_id")
model = als.fit(train_df_rec)


# In[15]:


predictions = model.recommendForAllUsers(10)
predictions.select("recommendations").show()


# In[16]:


NUMBER2RECOMMEND = best_model.rank

most_rated_anime = df_rec_filtered.groupBy("anime_id")\
    .agg(F.mean("rating").alias("avg_rating"), F\
    .count("rating")\
    .alias("count")).where("avg_rating > 8")\
    .orderBy(F.desc("count")).limit(NUMBER2RECOMMEND)
most_rated_anime = most_rated_anime.select("anime_id")


# In[17]:


predictions = predictions.withColumn("recommendations", F\
    .transform(F.col('recommendations'), lambda x: x.anime_id))
predictions.show()


# In[18]:


test_rec_list = test_df_rec.select("user_id", "anime_id", "rating")\
    .orderBy("user_id", F.desc("rating")).groupBy("user_id")\
    .agg(F.collect_list("anime_id").alias("gt"))
test_rec_list.show()


recomendations = test_rec_list.join(predictions, on="user_id", how="inner")
recomendations.show()


# In[21]:


recomendations.write.parquet("/user/team20/project/output/model1_predictions")


# In[22]:


model.save("/user/team20/project/models/model1.parquet")


# In[23]:




metrics = RankingMetrics(recomendations.select("gt", "recommendations").rdd)


# In[24]:



schema = StructType([
    StructField("model", StringType(), True),
    StructField("precision@10", FloatType(), True),
    StructField("recall@10", FloatType(), True),
    StructField("ndcg@10", FloatType(), True),
    StructField("precision@5", FloatType(), True),
    StructField("recall@5", FloatType(), True),
    StructField("ndcg@5", FloatType(), True)
])

# Create a list of tuples with sample data
data = [(
    "avg_synopsis_emb",
    metrics.precisionAt(10),
    metrics.recallAt(10),
    metrics.ndcgAt(10),
    metrics.precisionAt(5),
    metrics.recallAt(5),
    metrics.ndcgAt(5)
)]

df = spark.createDataFrame(data, schema)
df.write.parquet("/user/team20/project/output/evaluation", mode="append")


# In[32]:


with open("/home/team20/team20/bigdata-final-project-iu-2024.git/output/evaluation.csv", "w",\
    encoding="utf-8") as f:
    f.write("model,precision@10,recall@10,ndcg@10,precision@5,recall@5,ndcg@5\n")
    f.write("ALS,")
    for v in [10, 5]:
        f.write(",".join(map(str, [metrics.precisionAt(v), metrics.recallAt(v), metrics\
        .ndcgAt(v)])))
        if v == 10:
            f.write(",")
    f.write("\n")
