package db

import doobie._
import doobie.implicits._

object Schema {

  val createMoviesClean: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movies_clean (
        id BIGINT PRIMARY KEY,

        adult VARCHAR(10),
        belongs_to_collection LONGTEXT,

        budget DOUBLE,
        genres LONGTEXT,
        homepage LONGTEXT,
        imdb_id VARCHAR(30),

        original_language VARCHAR(10),
        original_title LONGTEXT,
        overview LONGTEXT,

        popularity DOUBLE,
        poster_path LONGTEXT,

        production_companies LONGTEXT,
        production_countries LONGTEXT,

        release_date VARCHAR(20),

        revenue DOUBLE,
        runtime DOUBLE,

        spoken_languages LONGTEXT,
        status VARCHAR(50),
        tagline LONGTEXT,
        title LONGTEXT,
        video VARCHAR(10),

        vote_average DOUBLE,
        vote_count DOUBLE,

        release_year INT,
        release_month INT,
        release_day INT,

        roi DOUBLE
      )
    """.update

  val truncateMoviesClean: Update0 =
    sql"DELETE FROM movies_clean".update
}
