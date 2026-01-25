package etl

object ModeloLogicoSql {

  def generarDDL: String =
    """
      |-- =============================================================
      |-- DROP DE TODAS LAS TABLAS DEL MODELO NORMALIZADO
      |-- =============================================================
      |
      |SET FOREIGN_KEY_CHECKS = 0;
      |
      |-- Primero eliminar tablas puente (relaciones)
      |DROP TABLE IF EXISTS movie_cast;
      |DROP TABLE IF EXISTS movie_crew;
      |DROP TABLE IF EXISTS movie_keywords;
      |DROP TABLE IF EXISTS movie_collections;
      |DROP TABLE IF EXISTS movie_spoken_languages;
      |DROP TABLE IF EXISTS movie_production_countries;
      |DROP TABLE IF EXISTS movie_production_companies;
      |DROP TABLE IF EXISTS movie_genres;
      |
      |-- Luego eliminar tablas principales y catálogos
      |DROP TABLE IF EXISTS movies;
      |DROP TABLE IF EXISTS cast_members;
      |DROP TABLE IF EXISTS crew_members;
      |DROP TABLE IF EXISTS keywords;
      |DROP TABLE IF EXISTS collections;
      |DROP TABLE IF EXISTS spoken_languages;
      |DROP TABLE IF EXISTS production_countries;
      |DROP TABLE IF EXISTS production_companies;
      |DROP TABLE IF EXISTS genres;
      |
      |-- También eliminar la tabla staging si quieres limpiar todo
      |DROP TABLE IF EXISTS movies_clean;
      |
      |SET FOREIGN_KEY_CHECKS = 1;
      |-- =============================================================
      |-- MODELO LOGICO NORMALIZADO (DDL)
      |-- =============================================================
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
      |-- TABLAS DE CAST Y CREW - EXACTAMENTE COMO EN EL JSON
      |-- =============================================================
      |
      |CREATE TABLE IF NOT EXISTS cast_members (
      |  cast_id BIGINT PRIMARY KEY,
      |  character_name LONGTEXT,
      |  credit_id VARCHAR(100),
      |  gender INT,
      |  name LONGTEXT,
      |  cast_order INT,
      |  profile_path LONGTEXT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS crew_members (
      |  credit_id VARCHAR(100) PRIMARY KEY,
      |  department LONGTEXT,
      |  gender INT,
      |  job LONGTEXT,
      |  name LONGTEXT,
      |  profile_path LONGTEXT
      |) ENGINE=InnoDB;
      |
      |-- =============================================================
      |-- TABLA PRINCIPAL: MOVIES
      |-- =============================================================
      |
      |CREATE TABLE IF NOT EXISTS movies (
      |  id BIGINT PRIMARY KEY,
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
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_production_companies (
      |  movie_id BIGINT NOT NULL,
      |  company_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, company_id),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (company_id) REFERENCES production_companies(company_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_production_countries (
      |  movie_id BIGINT NOT NULL,
      |  iso_3166_1 VARCHAR(10) NOT NULL,
      |  PRIMARY KEY (movie_id, iso_3166_1),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (iso_3166_1) REFERENCES production_countries(iso_3166_1) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_spoken_languages (
      |  movie_id BIGINT NOT NULL,
      |  iso_639_1 VARCHAR(10) NOT NULL,
      |  PRIMARY KEY (movie_id, iso_639_1),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (iso_639_1) REFERENCES spoken_languages(iso_639_1) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_collections (
      |  movie_id BIGINT NOT NULL,
      |  collection_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, collection_id),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (collection_id) REFERENCES collections(collection_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_keywords (
      |  movie_id BIGINT NOT NULL,
      |  keyword_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, keyword_id),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_cast (
      |  movie_id BIGINT NOT NULL,
      |  cast_id BIGINT NOT NULL,
      |  PRIMARY KEY (movie_id, cast_id),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (cast_id) REFERENCES cast_members(cast_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |CREATE TABLE IF NOT EXISTS movie_crew (
      |  movie_id BIGINT NOT NULL,
      |  credit_id VARCHAR(100) NOT NULL,
      |  PRIMARY KEY (movie_id, credit_id),
      |  FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
      |  FOREIGN KEY (credit_id) REFERENCES crew_members(credit_id) ON DELETE RESTRICT
      |) ENGINE=InnoDB;
      |
      |SET FOREIGN_KEY_CHECKS = 1;
      |""".stripMargin
}