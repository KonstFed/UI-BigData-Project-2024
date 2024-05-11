
# Introduction To Big Data Project. Anime recommendation.
In this project, retrieval task is solved on the partition of [anime dataset](https://www.kaggle.com/datasets/dsfelix/animes-dataset-2023?select=anime-exploratory-dataset-2023.csv), using 2 approaches: ALS and item-based (synopsis embedding) recommendation.

## Authors
- Boris Zarubin (b.zarubin@innopolis.university)
- Konstantin Fedorov (k.fedorov@innopolis.university)
- Nikita Bogdankov (n.bogdankov@innopolis.university)

## Cluster location
You could find our repository in `/home/team20/team20/bigdata-final-project-iu-2024.git`.
Launch all scripts from repository root, as it uses relative paths.

## Run
Data fetching, preprocessing, training and evaluation could be run as follows:
```
sh scripts/main.sh
```
If you want to run those stages independently, use: `stage1.sh` for data fetching, `stage2.sh` for preprocessing and `stage3.sh` for training and evaluation.

## Models
### ALS
ALS checkpoint is saved in `models/model1.parquet` and contains [ALS](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.recommendation.ALS.html) object.
### Item-based recommendation
Synopsis based model is saved in `models/model2.parquet` and in form of hdfs table, containing average synopsis embeddings for each user. Embeddings were computed, using [BAAI/bge-small-en-v1.5](https://huggingface.co/BAAI/bge-small-en-v1.5) model.

## Evaluation
Results are stored in `output/evaluation.csv`:
```
| model            | precision@10 | recall@10 | ndcg@10 | precision@5 | recall@5 | ndcg@5 |
|------------------|--------------|-----------|---------|-------------|----------|--------|
| ALS              | 0.0083       | 0.0083    | 0.0091  | 0.0111      | 0.0056   | 0.0106 |
| avg_synopsis_emb | 0.0222       | 0.0044    | 0.0236  | 0.0389      | 0.0039   | 0.0331 |
```
