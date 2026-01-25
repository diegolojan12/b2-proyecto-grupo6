package etl

import cats.effect.IO
import cats.implicits.*
import doobie.*
import doobie.implicits.*
import io.circe.parser.*
import io.circe.generic.auto.*
import models.*
import config.Database

object ModeloNormalizadoEtl {

  // ============================================================================
  // PASO 1: CREAR TODAS LAS TABLAS - SOLO CAMPOS DEL JSON
  // ============================================================================

  val createCastMembers: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS cast_members (
        cast_id BIGINT PRIMARY KEY,
        character_name LONGTEXT,
        credit_id VARCHAR(100),
        gender INT,
        name LONGTEXT,
        cast_order INT,
        profile_path LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createCrewMembers: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS crew_members (
        credit_id VARCHAR(100) PRIMARY KEY,
        department LONGTEXT,
        gender INT,
        job LONGTEXT,
        name LONGTEXT,
        profile_path LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createMovieCast: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_cast (
        movie_id BIGINT NOT NULL,
        cast_id BIGINT NOT NULL,
        PRIMARY KEY (movie_id, cast_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (cast_id) REFERENCES cast_members(cast_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieCrew: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_crew (
        movie_id BIGINT NOT NULL,
        credit_id VARCHAR(100) NOT NULL,
        PRIMARY KEY (movie_id, credit_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (credit_id) REFERENCES crew_members(credit_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createCollections: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS collections (
        collection_id BIGINT PRIMARY KEY,
        name LONGTEXT,
        poster_path LONGTEXT,
        backdrop_path LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createGenres: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS genres (
        genre_id BIGINT PRIMARY KEY,
        name LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createProductionCompanies: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS production_companies (
        company_id BIGINT PRIMARY KEY,
        name LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createProductionCountries: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS production_countries (
        iso_3166_1 VARCHAR(10) PRIMARY KEY,
        name LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createSpokenLanguages: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS spoken_languages (
        iso_639_1 VARCHAR(10) PRIMARY KEY,
        name LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createKeywords: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS keywords (
        keyword_id BIGINT PRIMARY KEY,
        name LONGTEXT
      ) ENGINE=InnoDB
    """.update

  val createMovies: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movies (
        id BIGINT PRIMARY KEY,
        adult VARCHAR(10),
        budget DOUBLE,
        homepage LONGTEXT,
        imdb_id VARCHAR(30),
        original_language VARCHAR(10),
        original_title LONGTEXT,
        overview LONGTEXT,
        popularity DOUBLE,
        poster_path LONGTEXT,
        release_date VARCHAR(20),
        revenue DOUBLE,
        runtime DOUBLE,
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
      ) ENGINE=InnoDB
    """.update

  val createMovieGenres: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_genres (
        movie_id BIGINT NOT NULL,
        genre_id BIGINT NOT NULL,
        PRIMARY KEY (movie_id, genre_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieProductionCompanies: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_production_companies (
        movie_id BIGINT NOT NULL,
        company_id BIGINT NOT NULL,
        PRIMARY KEY (movie_id, company_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (company_id) REFERENCES production_companies(company_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieProductionCountries: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_production_countries (
        movie_id BIGINT NOT NULL,
        iso_3166_1 VARCHAR(10) NOT NULL,
        PRIMARY KEY (movie_id, iso_3166_1),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (iso_3166_1) REFERENCES production_countries(iso_3166_1) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieSpokenLanguages: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_spoken_languages (
        movie_id BIGINT NOT NULL,
        iso_639_1 VARCHAR(10) NOT NULL,
        PRIMARY KEY (movie_id, iso_639_1),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (iso_639_1) REFERENCES spoken_languages(iso_639_1) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieCollections: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_collections (
        movie_id BIGINT NOT NULL,
        collection_id BIGINT NOT NULL,
        PRIMARY KEY (movie_id, collection_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (collection_id) REFERENCES collections(collection_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  val createMovieKeywords: Update0 =
    sql"""
      CREATE TABLE IF NOT EXISTS movie_keywords (
        movie_id BIGINT NOT NULL,
        keyword_id BIGINT NOT NULL,
        PRIMARY KEY (movie_id, keyword_id),
        FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id) ON DELETE RESTRICT
      ) ENGINE=InnoDB
    """.update

  // ============================================================================
  // PASO 2: PARSEAR JSON
  // ============================================================================

  def cleanJson(json: String): String = {
    json.trim
      .replaceAll("'", "\"")
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replaceAll("""\\x[0-9a-fA-F]{2}""", "") // ← NUEVO
      .replaceAll("""[\u0000-\u001F]""", "") // ← NUEVO
      .replaceAll("[\r\n]+", " ") // ← NUEVO
  }

  def parseCollection(json: String): Option[Collection] = {
    if (json.trim.isEmpty || json == "{}" || json == "[]") None
    else {
      val cleaned = cleanJson(json)
      decode[Collection](cleaned).toOption
    }
  }

  def parseGenres(json: String): List[Genre] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      val cleaned = cleanJson(json)
      decode[List[Genre]](cleaned).getOrElse(Nil)
    }
  }

  def parseProductionCompanies(json: String): List[ProductionCompany] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      val cleaned = cleanJson(json)
      decode[List[ProductionCompany]](cleaned).getOrElse(Nil)
    }
  }

  def parseProductionCountries(json: String): List[ProductionCountry] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      val cleaned = cleanJson(json)
      decode[List[ProductionCountry]](cleaned).getOrElse(Nil)
    }
  }

  def parseSpokenLanguages(json: String): List[SpokenLanguage] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      val cleaned = cleanJson(json)
      decode[List[SpokenLanguage]](cleaned).getOrElse(Nil)
    }
  }

  def parseKeywords(json: String): List[Keyword] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      val cleaned = cleanJson(json)
      decode[List[Keyword]](cleaned).getOrElse(Nil)
    }
  }

  def parseCast(json: String): List[CastMember] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      try {
        val cleaned = cleanJson(json)
        decode[List[CastMember]](cleaned).getOrElse(Nil)
      } catch {
        case _: Exception => Nil
      }
    }
  }

  def parseCrew(json: String): List[CrewMember] = {
    if (json.trim.isEmpty || json == "[]" || json == "{}") Nil
    else {
      try {
        val cleaned = cleanJson(json)
        decode[List[CrewMember]](cleaned).getOrElse(Nil)
      } catch {
        case _: Exception => Nil
      }
    }
  }

  // ============================================================================
  // PASO 3: INSERTAR DATOS - SOLO CAMPOS DEL JSON
  // ============================================================================

  def insertCollection(c: Collection): ConnectionIO[Int] =
    c.id match {
      case Some(id) =>
        sql"""
          INSERT IGNORE INTO collections (collection_id, name, poster_path, backdrop_path)
          VALUES (${id}, ${c.name.getOrElse("")}, ${c.poster_path.getOrElse("")}, ${c.backdrop_path.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertGenre(g: Genre): ConnectionIO[Int] =
    g.id match {
      case Some(id) =>
        sql"""
          INSERT IGNORE INTO genres (genre_id, name)
          VALUES (${id}, ${g.name.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertProductionCompany(pc: ProductionCompany): ConnectionIO[Int] =
    pc.id match {
      case Some(id) =>
        sql"""
          INSERT IGNORE INTO production_companies (company_id, name)
          VALUES (${id}, ${pc.name.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertProductionCountry(pc: ProductionCountry): ConnectionIO[Int] =
    pc.iso_3166_1 match {
      case Some(iso) =>
        sql"""
          INSERT IGNORE INTO production_countries (iso_3166_1, name)
          VALUES (${iso}, ${pc.name.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertSpokenLanguage(sl: SpokenLanguage): ConnectionIO[Int] =
    sl.iso_639_1 match {
      case Some(iso) =>
        sql"""
          INSERT IGNORE INTO spoken_languages (iso_639_1, name)
          VALUES (${iso}, ${sl.name.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertKeyword(k: Keyword): ConnectionIO[Int] =
    k.id match {
      case Some(id) =>
        sql"""
          INSERT IGNORE INTO keywords (keyword_id, name)
          VALUES (${id}, ${k.name.getOrElse("")})
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertCastMember(c: CastMember): ConnectionIO[Int] =
    c.cast_id match {
      case Some(castId) =>
        sql"""
          INSERT IGNORE INTO cast_members (
            cast_id, character_name, credit_id, gender, name, cast_order, profile_path
          )
          VALUES (
            ${castId},
            ${c.character.getOrElse("")},
            ${c.credit_id.getOrElse("")},
            ${c.gender.getOrElse(0)},
            ${c.name.getOrElse("")},
            ${c.order.getOrElse(0)},
            ${c.profile_path.getOrElse("")}
          )
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  def insertCrewMember(c: CrewMember): ConnectionIO[Int] =
    c.credit_id match {
      case Some(creditId) =>
        sql"""
          INSERT IGNORE INTO crew_members (
            credit_id, department, gender, job, name, profile_path
          )
          VALUES (
            ${creditId},
            ${c.department.getOrElse("")},
            ${c.gender.getOrElse(0)},
            ${c.job.getOrElse("")},
            ${c.name.getOrElse("")},
            ${c.profile_path.getOrElse("")}
          )
        """.update.run
      case None => 0.pure[ConnectionIO]
    }

  // ============================================================================
  // PASO 4: INSERTAR PELÍCULA Y RELACIONES
  // ============================================================================

  def insertMovie(m: Movie): ConnectionIO[Int] =
    sql"""
      INSERT IGNORE INTO movies (
        id, adult, budget, homepage, imdb_id, original_language, original_title,
        overview, popularity, poster_path, release_date, revenue, runtime,
        status, tagline, title, video, vote_average, vote_count,
        release_year, release_month, release_day, roi
      ) VALUES (
        ${m.id.toLong}, ${m.adult}, ${m.budget}, ${m.homepage}, ${m.imdb_id},
        ${m.original_language}, ${m.original_title}, ${m.overview}, ${m.popularity},
        ${m.poster_path}, ${m.release_date}, ${m.revenue}, ${m.runtime},
        ${m.status}, ${m.tagline}, ${m.title}, ${m.video}, ${m.vote_average},
        ${m.vote_count}, ${m.release_year.toInt}, ${m.release_month.toInt},
        ${m.release_day.toInt}, ${m.`return`}
      )
    """.update.run

  def linkMovieGenre(movieId: Long, genreId: Long): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_genres (movie_id, genre_id) VALUES ($movieId, $genreId)".update.run

  def linkMovieCompany(movieId: Long, companyId: Long): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_production_companies (movie_id, company_id) VALUES ($movieId, $companyId)".update.run

  def linkMovieCountry(movieId: Long, iso: String): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_production_countries (movie_id, iso_3166_1) VALUES ($movieId, $iso)".update.run

  def linkMovieLanguage(movieId: Long, iso: String): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_spoken_languages (movie_id, iso_639_1) VALUES ($movieId, $iso)".update.run

  def linkMovieCollection(movieId: Long, collectionId: Long): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_collections (movie_id, collection_id) VALUES ($movieId, $collectionId)".update.run

  def linkMovieKeyword(movieId: Long, keywordId: Long): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_keywords (movie_id, keyword_id) VALUES ($movieId, $keywordId)".update.run

  def linkMovieCast(movieId: Long, castId: Long): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_cast (movie_id, cast_id) VALUES ($movieId, $castId)".update.run

  def linkMovieCrew(movieId: Long, creditId: String): ConnectionIO[Int] =
    sql"INSERT IGNORE INTO movie_crew (movie_id, credit_id) VALUES ($movieId, $creditId)".update.run

  // ============================================================================
  // PASO 5: PROCESO COMPLETO POR PELÍCULA
  // ============================================================================

  def procesarPelicula(m: Movie): ConnectionIO[Unit] = {
    val movieId = m.id.toLong

    val collection = parseCollection(m.belongs_to_collection)
    val genres = parseGenres(m.genres)
    val companies = parseProductionCompanies(m.production_companies)
    val countries = parseProductionCountries(m.production_countries)
    val languages = parseSpokenLanguages(m.spoken_languages)
    val keywords = parseKeywords(m.keywords)
    val cast = parseCast(m.cast)
    val crew = parseCrew(m.crew)

    for {
      _ <- insertMovie(m)

      // Insertar catálogos
      _ <- collection.traverse(insertCollection)
      _ <- genres.traverse(insertGenre)
      _ <- companies.traverse(insertProductionCompany)
      _ <- countries.traverse(insertProductionCountry)
      _ <- languages.traverse(insertSpokenLanguage)
      _ <- keywords.traverse(insertKeyword)
      _ <- cast.traverse(insertCastMember)
      _ <- crew.traverse(insertCrewMember)

      // Crear relaciones
      _ <- collection.traverse(c => c.id.traverse(linkMovieCollection(movieId, _)))
      _ <- genres.traverse(g => g.id.traverse(linkMovieGenre(movieId, _)))
      _ <- companies.traverse(c => c.id.traverse(linkMovieCompany(movieId, _)))
      _ <- countries.traverse(c => c.iso_3166_1.traverse(linkMovieCountry(movieId, _)))
      _ <- languages.traverse(l => l.iso_639_1.traverse(linkMovieLanguage(movieId, _)))
      _ <- keywords.traverse(k => k.id.traverse(linkMovieKeyword(movieId, _)))
      _ <- cast.traverse(c => c.cast_id.traverse(id => linkMovieCast(movieId, id)))
      _ <- crew.traverse(c => c.credit_id.traverse(linkMovieCrew(movieId, _)))
    } yield ()
  }

  def procesarPeliculaConLog(m: Movie, idx: Int): ConnectionIO[Unit] = {
    val movieId = m.id.toLong

    val collection = parseCollection(m.belongs_to_collection)
    val genres = parseGenres(m.genres)
    val companies = parseProductionCompanies(m.production_companies)
    val countries = parseProductionCountries(m.production_countries)
    val languages = parseSpokenLanguages(m.spoken_languages)
    val keywords = parseKeywords(m.keywords)
    val cast = parseCast(m.cast)
    val crew = parseCrew(m.crew)

    if (idx < 3) {
      FC.delay {
        println(s"\n[DEBUG] Película #$idx: ${m.title}")
        println(s"  - Collection: ${if (collection.isDefined) "✓" else "✗"}")
        println(s"  - Genres: ${genres.size} items")
        println(s"  - Companies: ${companies.size} items")
        println(s"  - Countries: ${countries.size} items")
        println(s"  - Languages: ${languages.size} items")
        println(s"  - Keywords: ${keywords.size} items")
        println(s"  - Cast: ${cast.size} items")
        println(s"  - Crew: ${crew.size} items")
        if (cast.nonEmpty) {
          println(s"    Ejemplo cast: ${cast.head.name.getOrElse("N/A")} como ${cast.head.character.getOrElse("N/A")}")
        }
        if (crew.nonEmpty) {
          println(s"    Ejemplo crew: ${crew.head.name.getOrElse("N/A")} - ${crew.head.job.getOrElse("N/A")}")
        }
      } *> procesarPelicula(m)
    } else {
      procesarPelicula(m)
    }
  }

  // ============================================================================
  // PASO 6: CARGAR TODAS LAS PELÍCULAS
  // ============================================================================

  def cargarModeloNormalizado(movies: List[Movie]): IO[Unit] = {
    Database.init()
    Database.transactor.use { xa =>
      val program = for {
        // Crear tablas catálogo
        _ <- createCollections.run
        _ <- createGenres.run
        _ <- createProductionCompanies.run
        _ <- createProductionCountries.run
        _ <- createSpokenLanguages.run
        _ <- createKeywords.run
        _ <- createCastMembers.run
        _ <- createCrewMembers.run
        _ <- createMovies.run

        // Crear tablas puente
        _ <- createMovieGenres.run
        _ <- createMovieProductionCompanies.run
        _ <- createMovieProductionCountries.run
        _ <- createMovieSpokenLanguages.run
        _ <- createMovieCollections.run
        _ <- createMovieKeywords.run
        _ <- createMovieCast.run
        _ <- createMovieCrew.run

        // Procesar películas en lotes
        _ <- movies.zipWithIndex.grouped(100).toList.traverse_ { batch =>
          batch.traverse_ { case (movie, idx) => procesarPeliculaConLog(movie, idx) } *>
            FC.delay(print("."))
        }
      } yield ()

      program.transact(xa) *> IO.println(s"\n✓ Modelo normalizado cargado: ${movies.length} películas")
    }
  }
}