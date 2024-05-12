bash scripts/install_dependecies.sh

rm data/*
hdfs dfs -rm -r /user/team20/project

kaggle datasets download -d dsfelix/animes-dataset-2023
unzip -o animes-dataset-2023.zip -d data
echo "unzip done"
rm animes-dataset-2023.zip
rm -r data/assets
rm -r data/reports
rm data/anime-dataset-2023.csv
rm data/anime-transformed-dataset-2023.csv
rm data/users-score-2023.csv
rm data/users-details-2023.csv

password=$(head -n 1 secrets/psql.pass)

python3 scripts/build_projectdb.py

hdfs dfs -mkdir /user/team20/project
hdfs dfs -mkdir /user/team20/project/warehouse
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team20_projectdb --username team20 --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=/user/team20/project/warehouse --m 1 --password $password
