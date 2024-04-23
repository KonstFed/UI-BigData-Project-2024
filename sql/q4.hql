USE team20_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE EXTERNAL TABLE q4_results(
    rating STRING,
    avg_score FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/team20/project/hive/warehouse/q4'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT rating, score
FROM anime_part_buck;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q4' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q4_results;

SELECT * FROM q4_results;