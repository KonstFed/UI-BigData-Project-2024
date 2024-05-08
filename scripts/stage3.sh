# delete old stuff
hdfs dfs -rm -r /user/team20/project/data
hdfs dfs -rm -r /user/team20/project/output/evaluation
hdfs dfs -rm -r /user/team20/project/models

# make dirs
hdfs dfs -mkdir /user/team20/project/data
hdfs dfs -mkdir /user/team20/project/models

# run python things
spark-submit --master yarn scripts/synopsis_embeddings.py
hdfs dfs -put /home/team20/team20/bigdata-final-project-iu-2024.git/synopsis_embs.csv /user/team20/project/data/synopsis_embs.csv
spark-submit --master yarn scripts/baseline.py
spark-submit --master yarn scripts/item_based_rec.py

# move model to repository
mkdir models
hdfs dfs -get /user/team20/project/models/model1.parquet /home/team20/team20/bigdata-final-project-iu-2024.git/models/model1.parquet
hdfs dfs -get /user/team20/project/models/model2.parquet /home/team20/team20/bigdata-final-project-iu-2024.git/models/model2.parquet

hdfs dfs -get /user/team20/project/output/model1_predictions /home/team20/team20/bigdata-final-project-iu-2024.git/output/model1_predictions
hdfs dfs -get /user/team20/project/output/model2_predictions /home/team20/team20/bigdata-final-project-iu-2024.git/output/model2_predictions

