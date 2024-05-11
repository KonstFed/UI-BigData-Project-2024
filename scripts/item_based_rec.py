'''
This is the file with item_based'''

#!/usr/bin/env python
# coding: utf-8

# ### Imports

# In[15]:


import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, array_contains, row_number, size
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import RankingMetrics


# ## Setup

# ### Load spark data

# In[16]:


# try:
#     spark.stop()
# except:
#     print('No spark is running')

WAREHOUSE = "/user/team20/project/hive/warehouse"
TEAM = "team20"

spark = SparkSession.builder.appName("f{TEAM} - spark ML")\
    .master("yarn")\
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", WAREHOUSE)\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .enableHiveSupport().getOrCreate()
spark.sql("USE team20_projectdb")


# ## Train

# In[17]:


# read synopsis embeddings
schema = StructType([
    StructField('anime_id',
                IntegerType(), False),
    StructField('synopsis_emb_str',
                StringType(), False)
])
synopsis_df = spark.read.csv(
    "/user/team20/project/data/synopsis_embs.csv",
    header=True,
    mode="DROPMALFORMED",
    schema=schema
)


def read_embedding(emb: str):
    '''
    read_embedding'''
    emb = [float(i) for i in emb[1:-1].split(', ')]
    return emb


read_embedding_udf = udf(read_embedding, ArrayType(FloatType()))

synopsis_df = synopsis_df\
    .withColumn('synopsis_emb', read_embedding_udf(synopsis_df['synopsis_emb_str']))
synopsis_df = synopsis_df.drop('synopsis_emb_str')
synopsis_df.show(5)


# In[18]:


# read train user_scores
scores_df = spark.read.json("/user/team20/project/data/train.json")
scores_df = scores_df.select(
    scores_df["user_id"].cast(IntegerType()),
    scores_df["anime_id"].cast(IntegerType()),
    scores_df["rating"].cast(IntegerType())
)
scores_df.show(5)


# ### Calculate average embedding per user

# In[8]:


# join tables
user_df = synopsis_df.join(scores_df, "anime_id")

# split array
emb_size = user_df.selectExpr("max(size(synopsis_emb))").collect()[0][0]
col_names = []
for i in range(emb_size):
    col_name = f"synopsis_emb_{i}"
    col_names.append(col_name)
    user_df = user_df.withColumn(col_name, col("synopsis_emb").getItem(i))
user_df.drop('synopsis_emb')

# average features
user_df = user_df.groupBy("user_id").agg(*[avg(col_name).alias(col_name) for col_name in col_names])

# collect array
assembler = VectorAssembler(inputCols=col_names, outputCol="synopsis_emb_agr")
user_df = assembler.transform(user_df)
user_df = user_df.drop(*col_names)

user_df.show(5)


# ### Save user embeddings as json

# In[ ]:


user_df.write.parquet("/user/team20/project/models/model2.parquet")


# In[30]:


user_df.count()


# ## Test

# ### Load model

# In[19]:


user_df = spark.read.parquet("/user/team20/project/models/model2.parquet")
user_df.show(5)


# ### Load test data

# In[20]:


test_scores_df = spark.read.json("/user/team20/project/data/test.json")
test_scores_df = test_scores_df.select(
    test_scores_df["user_id"].cast(IntegerType()),
    test_scores_df["anime_id"].cast(IntegerType()),
    test_scores_df["rating"].cast(IntegerType())
)
# test_scores_df.groupBy("user_id").agg(count('rating')).agg({'count(rating)': 'avg'}).show()
test_scores_df.show(5)


# In[21]:


test_rec_list = test_scores_df.select("user_id", "anime_id", "rating")\
    .orderBy("user_id", "rating", ascending=False)\
    .groupBy("user_id")\
    .agg(F.collect_list("anime_id").alias("gt"))
test_rec_list.show()


# ### Predict 100 anime titles for each user

# In[22]:


train_rec_list = scores_df\
    .select("user_id", "anime_id", "rating")\
    .orderBy("user_id", "rating", ascending=False)\
    .groupBy("user_id")\
    .agg(F.collect_list("anime_id").alias("watched"))
train_rec_list = train_rec_list.join(
    user_df,
    train_rec_list['user_id'] == user_df['user_id'],
    'inner'
).drop(user_df.user_id)
train_rec_list = train_rec_list\
    .withColumn('watched_count', size('watched'))\
    .orderBy('watched_count', ascending=False)
# train_rec_list.show(5)


# In[23]:


# pick top n users based on number of scores to optimize test
N = 1000
top_n_user_embs = train_rec_list.limit(N)
top_n_user_embs.show()


# In[27]:


def cosine_similarity(v_1, v_2):
    '''
    find cosine_similarity'''
    # assert False, f"{type(v_1)} {type(v_2)}"
    a_1, b_1 = np.array(v_1), np.array(v_2)
    # check for nulls
    if not np.any(a_1) or not np.any(b_1):
        return -1.0

    cos_sim = np.dot(a_1, b_1) / (np.linalg.norm(a_1) * np.linalg.norm(b_1))
    return float(cos_sim)


pred_df = top_n_user_embs.crossJoin(synopsis_df)
# filter watched animes
pred_df = pred_df.filter(~array_contains(pred_df.watched, pred_df.anime_id))
pred_df.show(5)


# ### Pipeline

# In[30]:


# find similarity
cosine_similarity_udf = udf(cosine_similarity, FloatType())
pred_df = pred_df.withColumn(
    "similarity",
    cosine_similarity_udf(pred_df.synopsis_emb_agr, pred_df.synopsis_emb)
).select("user_id", "anime_id", "similarity")
pred_df.show(5)


# pick top 50 anime for each user
k = 50

window_spec = Window.partitionBy("user_id").orderBy(col("similarity").desc())
# Add a row number column to each partition
pred_df_ranked = pred_df.withColumn("rank", row_number().over(window_spec))
# Filter out rows with row_number <= 10 to get the top 10 values for each user_id
pred_df_top_k = pred_df_ranked.filter(col("rank") <= k)
pred_df_top_k = pred_df_top_k\
    .groupby('user_id')\
    .agg(F.collect_list("anime_id")\
    .alias("recommendations"))
pred_df_top_k.show(10)


# ### Evaluate

# In[33]:


recommendations = test_rec_list.join(
    pred_df_top_k,
    test_rec_list.user_id == pred_df_top_k.user_id,
    'inner'
).drop(pred_df_top_k.user_id)
recommendations.show(10)


# In[35]:


# save recommendations
recommendations.write.parquet("/user/team20/project/output/model2_predictions")


# In[37]:


# Define the schema for the DataFrame
schema = StructType([
    StructField("model", StringType(), True),
    StructField("precision@10", FloatType(), True),
    StructField("recall@10", FloatType(), True),
    StructField("ndcg@10", FloatType(), True),
    StructField("precision@5", FloatType(), True),
    StructField("recall@5", FloatType(), True),
    StructField("ndcg@5", FloatType(), True)
])

metrics = RankingMetrics(recommendations.select("gt", "recommendations").rdd)
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

# Create a DataFrame with the defined schema and data
df = spark.createDataFrame(data, schema)
df.show()


# In[40]:


df.write.parquet("/user/team20/project/output/evaluation", mode="append")


# In[50]:


with open("/home/team20/team20/bigdata-final-project-iu-2024.git/output/evaluation.csv"\
    , "a", encoding="utf-8") as f:
    f.write(','.join([str(i) for i in data[0]]))
    f.write("\n")
    