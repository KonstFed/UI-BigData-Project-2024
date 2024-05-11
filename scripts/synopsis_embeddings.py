'''
this is the file'''
#!/usr/bin/env python
# coding: utf-8

# ### Imports

# In[1]:

from typing import List
from tqdm.auto import tqdm
from transformers import AutoTokenizer, AutoModel
from pyspark.sql import SparkSession
import torch


# ### Load HF model

# In[2]:


# Load model from HuggingFace Hub
tokenizer = AutoTokenizer.from_pretrained('BAAI/bge-small-en-v1.5')
model = AutoModel.from_pretrained('BAAI/bge-small-en-v1.5')
model.eval()


def batch_embeddings(batch: List[str]):
    '''
    batch-embeddings'''
    encoded_input = tokenizer(batch, padding=True, truncation=True, return_tensors='pt')
    with torch.no_grad():
        model_output = model(**encoded_input)
        # Perform pooling. In this case, cls pooling.
        sentence_embeddings = model_output[0][:, 0]
    # normalize embeddings
    sentence_embeddings = torch.nn.functional.normalize(sentence_embeddings, p=2, dim=1)
    return sentence_embeddings


# ### Read anime ids and synopsises

# In[3]:


WAREHOUSE = "/user/team20/project/hive/warehouse"
TEAM = "team20"

spark = SparkSession.builder\
    .appName(f"{TEAM} - spark ML")\
    .master("yarn")\
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", WAREHOUSE)\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .enableHiveSupport()\
    .getOrCreate()
spark.sql("USE team20_projectdb")


# In[4]:


spark_df = spark.sql("SELECT id AS anime_id, synopsis FROM anime_part_buck WHERE synopsis != '-'")
df = spark_df.toPandas()
df.head()


# ### Get anime embeddings

# In[5]:


BATCH_SIZE = 4
synopsis_list = df['synopsis'].tolist()
synopsis_embs = []
i = 0

for i in tqdm(range(0, len(synopsis_list) // BATCH_SIZE)):
    batch_synopsises = synopsis_list[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
    batch_embs = batch_embeddings(batch_synopsises)
    synopsis_embs.extend(batch_embs)

if (i + 1) * BATCH_SIZE < len(synopsis_list):
    batch_synopsises = synopsis_list[(i + 1) * BATCH_SIZE:len(synopsis_list)]
    batch_embs = batch_embeddings(batch_synopsises)
    synopsis_embs.extend(batch_embs)


# ### Write anime embeddings to csv om HDFS

# In[6]:


assert len(synopsis_embs) == len(synopsis_list), (len(synopsis_embs), len(synopsis_list))

df['synopsis_emb'] = [emb.tolist() for emb in synopsis_embs]
df.drop('synopsis', axis=1)\
    .to_csv('/home/team20/team20/bigdata-final-project-iu-2024.git/synopsis_embs.csv', \
    index=False)
