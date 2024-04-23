-- anime-exploratory-dataset-2023.csv
DROP TABLE IF EXISTS users_scores;

DROP TABLE IF EXISTS anime_temp;
DROP TABLE IF EXISTS anime;
DROP TABLE IF EXISTS users_details;


CREATE TABLE IF NOT EXISTS anime_temp(
	id INTEGER PRIMARY KEY,
    title VARCHAR (512),
    -- score NUMERIC (4, 2),
	score REAL,
	genres VARCHAR (4096),
    synopsis VARCHAR (4096),
    type VARCHAR (8),
    episodes SMALLINT,
	status VARCHAR (20),
	producers VARCHAR (2048),
	licensors VARCHAR (2048),
	studios VARCHAR (1024),
	source VARCHAR (64),
	duration VARCHAR (64),
	rating VARCHAR (64),
	rank INTEGER,
	popularity INTEGER,
	favorites INTEGER,
	scored_by INTEGER,
	members INTEGER,
	image_url VARCHAR (128),
	genre_award_winning BOOLEAN,
	genre_slice_of_life BOOLEAN,
	genre_fantasy BOOLEAN,
	genre_sci_fi BOOLEAN,
	genre_erotica BOOLEAN,
	genre_romance BOOLEAN,
	genre_horror BOOLEAN,
	genre_boys_love BOOLEAN,
	genre_girls_love BOOLEAN,
	genre_sports BOOLEAN,
	genre_comedy BOOLEAN,
	genre__ BOOLEAN,
	genre_gourmet BOOLEAN,
	genre_suspense BOOLEAN,
	genre_supernatural BOOLEAN,
	genre_avant_garde BOOLEAN,
	genre_hentai BOOLEAN,
	genre_drama BOOLEAN,
	genre_mystery BOOLEAN,
	genre_adventure BOOLEAN,
	genre_ecchi BOOLEAN,
	genre_action BOOLEAN
);

CREATE TABLE IF NOT EXISTS anime(
    id INTEGER PRIMARY KEY,
    title VARCHAR (512),
    -- score NUMERIC (4, 2),
	score REAL,
	genres VARCHAR (4096),
    synopsis VARCHAR (4096),
    type VARCHAR (8),
    episodes SMALLINT,
	is_airing BOOLEAN,
	producers VARCHAR (2048),
	licensors VARCHAR (2048),
	studios VARCHAR (1024),
	source VARCHAR (64),
	duration VARCHAR (64),
	rating VARCHAR (64),
	rank INTEGER,
	popularity INTEGER,
	favorites INTEGER,
	scored_by INTEGER,
	members INTEGER
);

-- users-details-transformed-2023.csv

CREATE TABLE IF NOT EXISTS users_details(
    id INTEGER PRIMARY KEY,
    user_name VARCHAR (512),
    gender VARCHAR (20),
    joined timestamp,
    -- days_watched NUMERIC (10, 1),
	days_watched REAL,
    -- mean_score NUMERIC (4, 2),
	mean_score REAL,
    watching INTEGER,
    completed INTEGER,
    on_hold INTEGER,
    dropped INTEGER,
    plan_to_watch INTEGER,
    total_entries INTEGER,
    rewatched INTEGER,
    episodes_watched INTEGER
);

-- users-scores-transformed-2023.csv

CREATE TABLE IF NOT EXISTS users_scores(
    user_id INTEGER references users_details(id),
    username VARCHAR (512),
    anime_id INTEGER references anime(id),
    anime_title VARCHAR (512),
    rating INTEGER,
    PRIMARY KEY (user_id, anime_id)
);