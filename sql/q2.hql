USE team20_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE EXTERNAL TABLE q2_results(
    score FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/team20/project/hive/warehouse/q2'; 

SET hive.resultset.use.unique.column.names = false;


INSERT INTO q2_results
SELECT score
FROM anime_part_buck;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q2' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ','
SELECT * FROM q2_results;

SELECT * FROM q2_results;
