ALTER TABLE anime_temp ADD COLUMN is_airing BOOLEAN;
UPDATE anime_temp
SET is_airing = CASE WHEN status = 'currently airing' THEN TRUE ELSE FALSE END;

-- ALTER TABLE anime_temp ADD COLUMN genres_array text[];
-- UPDATE anime_temp
-- SET genres_array = STRING_TO_ARRAY(substr(genres, 3, length(genres) - 4), $$', '$$);

INSERT INTO anime
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
	rating,
	rank,
	popularity,
	favorites,
	scored_by,
	members
FROM anime_temp;

DROP TABLE anime_temp;