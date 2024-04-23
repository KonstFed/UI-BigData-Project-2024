use team20_projectdb;

CREATE EXTERNAL TABLE anime_part_buck(
    id int, 
    title varchar(512), 
    score float, 
    genres varchar(4096), 
    synopsis varchar(4096), 
    type varchar(8), 
    episodes int,
    is_airing boolean,
    producers varchar(2048),
    licensors varchar(2048),
    studios varchar(1024),
    source varchar(64),
    duration varchar(64),
    rank int,
    popularity int,
    favorites int,
    scored_by int,
  members int
  ) 
    PARTITIONED BY (rating varchar(64)) 
    CLUSTERED BY (id) into 7 buckets
    STORED AS AVRO LOCATION '/user/team20/project/hive/warehouse/anime_part_buck' 
    TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE anime_part_buck PARTITION (rating)
SELECT
    id,
    title,
    score,
    genres,
    synopsis,
    type,
    episodes,
    is_airing,
    producers,
    licensors,
    studios,
    source,
    duration,
    rank,
    popularity,
    favorites,
    scored_by,
    members,
    rating
FROM anime;

DROP TABLE IF EXISTS anime;
