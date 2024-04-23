password=$(head -n 1 secrets/hive.pass)


# task 1
hdfs dfs -mkdir -p /user/team20/project/warehouse/avsc
mv *.avsc output/
mv *.java output/
cd ~/team20/bigdata-final-project-iu-2024.git
hdfs dfs -put output/*.avsc /user/team20/project/warehouse/avsc

# task 2
hdfs dfs -mkdir /user/team20/project/hive
hdfs dfs -mkdir /user/team20/project/hive/warehouse

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/db.hql  > output/hive_results.txt
