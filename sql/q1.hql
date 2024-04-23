USE team20_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE EXTERNAL TABLE q1_results(
    mean_score FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/team20/project/hive/warehouse/q1'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT mean_score
FROM users_details;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q1' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q1_results;

SELECT * FROM q1_results;