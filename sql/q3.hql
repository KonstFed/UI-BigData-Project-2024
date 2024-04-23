USE team20_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE EXTERNAL TABLE q3_results(
    type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/team20/project/hive/warehouse/q3'; 

SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
SELECT type
FROM anime_part_buck;

INSERT OVERWRITE DIRECTORY '/user/team20/project/output/q3' 
ROW FORMAT DELIMITED FIELDS 
TERMINATED BY ',' 
SELECT * FROM q3_results;

SELECT type FROM q3_results;