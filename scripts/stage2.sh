password=$(head -n 1 secrets/hive.pass)

hdfs dfs -rm -r /user/team20/project/hive

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
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/partition.hql &> output/hive_partitioning_results.txt

# output creation
hdfs dfs -rm -r /user/team20/project/output
hdfs dfs -mkdir /user/team20/project/output

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/q1.hql
echo "mean_score" > output/q1.csv
hdfs dfs -cat /user/team20/project/output/q1/* >> output/q1.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/q2.hql
echo "score" > output/q2.csv
hdfs dfs -cat /user/team20/project/output/q2/* >> output/q2.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/q3.hql
echo "type" > output/q3.csv
hdfs dfs -cat /user/team20/project/output/q3/* >> output/q3.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/q4.hql
echo "rating,avg_score" > output/q4.csv
hdfs dfs -cat /user/team20/project/output/q4/* >> output/q4.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team20 -p $password -f sql/q5.hql
echo "user_id,scores_count" > output/q5.csv
hdfs dfs -cat /user/team20/project/output/q5/* >> output/q5.csv