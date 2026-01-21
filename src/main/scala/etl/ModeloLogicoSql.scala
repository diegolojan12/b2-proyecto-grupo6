package etl

object ModeloLogicoSql {

  def generarDDL: String =
    """-- =============================================================
      |-- MODELO LOGICO NORMALIZADO (DDL)
      |-- =============================================================
      |-- Este script crea tablas normalizadas.
      |-- Los campos JSON (genres, production_companies, etc.) se
      |-- separan en tablas catálogo con relaciones N:M.
      |
      |SET FOREIGN_KEY_CHECKS = 0;
      |
      |-- =============================================================
      |-- TABLAS CATÁLOGO
      |-- =============================================================
      |
      |CREATE TABLE IF NOT EXISTS collections (
      |  collection_id BIGINT PRIMARY KEY,
      |  name LONGTEXT,
      |  poster_path LONGTEXT,
      |  backdrop_path LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS genres (
      |  genre_id BIGINT PRIMARY KEY,
      |  name LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS production_companies (
      |  company_id BIGINT PRIMARY KEY,
      |  name LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS production_countries (
      |  iso_3166_1 VARCHAR(10) PRIMARY KEY,
      |  name LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS spoken_languages (
      |  iso_639_1 VARCHAR(10) PRIMARY KEY,
      |  name LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS keywords (
      |  keyword_id BIGINT PRIMARY KEY,
      |  name LONGTEXT
      |) ENGINE=InnoDB;
      |
      |-- =============================================================
      |-- TABLA PRINCIPAL: MOVIES (SIN CAMPOS JSON)
      |-- =============================================================
      |-- Nota: Los campos JSON originales (genres, production_companies, etc.)
      |-- se eliminan porque ahora están normalizados en tablas separadas.
      |
      |CREATE TABLE IF NOT EXISTS movies (
      |  id BIGINT PRIMARY KEY,
      |
      |  -- Campos escalares
      |  adult VARCHAR(10),
      |  budget DOUBLE,
      |  homepage LONGTEXT,
      |  imdb_id VARCHAR(30),
      |  original_language VARCHAR(10),
      |  original_title LONGTEXT,
      |  overview LONGTEXT,
      |  popularity DOUBLE,
      |  poster_path LONGTEXT,
      |  release_date VARCHAR(20),
      |  revenue DOUBLE,
      |  runtime DOUBLE,
      |  status VARCHAR(50),
      |  tagline LONGTEXT,
      |  title LONGTEXT,
      |  video VARCHAR(10),
      |  vote_average DOUBLE,
      |  vote_count DOUBLE,
      |
      |  -- Campos calculados
      |  release_year INT,
      |  release_month INT,
      |  release_day INT,
      |  roi DOUBLE
      |) ENGINE=InnoDB;
      |
      |-- =============================================================
      |-- TABLAS PUENTE (RELACIONES N:M)
      |-- =============================================================
      |
      |CREATE TABLE IF NOT EXISTS movie_genres (
      |  movie_id BIGINT NOT NULL,
      |  genre_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, genre_id),
      |  CONSTRAINT fk_movie_genres_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_genres_genre
      |    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_production_companies (
      |  movie_id BIGINT NOT NULL,
      |  company_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, company_id),
      |  CONSTRAINT fk_movie_companies_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_companies_company
      |    FOREIGN KEY (company_id) REFERENCES production_companies(company_id)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_production_countries (
      |  movie_id BIGINT NOT NULL,
      |  iso_3166_1 VARCHAR(10) NOT NULL,
      |  PRIMARY KEY (movie_id, iso_3166_1),
      |  CONSTRAINT fk_movie_countries_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_countries_country
      |    FOREIGN KEY (iso_3166_1) REFERENCES production_countries(iso_3166_1)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_spoken_languages (
      |  movie_id BIGINT NOT NULL,
      |  iso_639_1 VARCHAR(10) NOT NULL,
      |  PRIMARY KEY (movie_id, iso_639_1),
      |  CONSTRAINT fk_movie_languages_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_languages_language
      |    FOREIGN KEY (iso_639_1) REFERENCES spoken_languages(iso_639_1)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_collections (
      |  movie_id BIGINT NOT NULL,
      |  collection_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, collection_id),
      |  CONSTRAINT fk_movie_collections_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_collections_collection
      |    FOREIGN KEY (collection_id) REFERENCES collections(collection_id)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_keywords (
      |  movie_id BIGINT NOT NULL,
      |  keyword_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, keyword_id),
      |  CONSTRAINT fk_movie_keywords_movie
      |    FOREIGN KEY (movie_id) REFERENCES movies(id)
      |    ON DELETE CASCADE,
      |  CONSTRAINT fk_movie_keywords_keyword
      |    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
      |    ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |SET FOREIGN_KEY_CHECKS = 1;
      |
      |-- =============================================================
      |-- NOTAS IMPORTANTES:
      |-- =============================================================
      |-- 1. La tabla 'movies' NO contiene campos JSON (genres, production_companies, etc.)
      |-- 2. Esos datos ahora están en tablas separadas con relaciones N:M
      |-- 3. Para obtener todos los géneros de una película:
      |--    SELECT g.* FROM genres g
      |--    JOIN movie_genres mg ON g.genre_id = mg.genre_id
      |--    WHERE mg.movie_id = ?
      |-- 4. Este modelo se puebla automáticamente ejecutando la opción [5] del menú
      |""".stripMargin
}