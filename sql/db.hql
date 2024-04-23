DROP DATABASE IF EXISTS team20_projectdb CASCADE;

CREATE DATABASE team20_projectdb LOCATION "user/team20/project/hive/warehouse";
USE team20_projectdb;

CREATE EXTERNAL TABLE anime STORED AS AVRO LOCATION 'user/team20/project/hive/warehouse/anime'
TBLPROPERTIES ('avro.schema.url'='/user/team20/project/warehouse/avsc/anime.avsc');

CREATE EXTERNAL TABLE users_details STORED AS AVRO LOCATION 'user/team20/project/hive/warehouse/users_details'
TBLPROPERTIES ('avro.schema.url'='/user/team20/project/warehouse/avsc/users_details.avsc');

CREATE EXTERNAL TABLE users_scores STORED AS AVRO LOCATION 'user/team20/project/hive/warehouse/users_scores'
TBLPROPERTIES ('avro.schema.url'='/user/team20/project/warehouse/avsc/users_scores.avsc');