USE team20_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE EXTERNAL TABLE q5_results(
    user_id INT,
    scores_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/team20/project/hive/warehouse/q5'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q5_results
SELECT user_id, COUNT(anime_id) AS scores_count
FROM users_scores
GROUP BY user_id;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q5' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q5_results;

SELECT * FROM q5_results;

