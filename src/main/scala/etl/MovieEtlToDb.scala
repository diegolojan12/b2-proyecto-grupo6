package etl

import cats.effect.IO
import cats.implicits.*
import doobie.*
import doobie.implicits.*
import models.Movie
import config.Database

import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JFiles, Paths}

object MovieEtlToDb:

  // ---- Modelo interno SOLO para la BD (tipos compatibles con MySQL) ----
  private final case class MovieDbRow(
                                       id: Long,
                                       adult: String,
                                       belongs_to_collection: String,
                                       budget: Double,
                                       genres: String,
                                       homepage: String,
                                       imdb_id: String,
                                       original_language: String,
                                       original_title: String,
                                       overview: String,
                                       popularity: Double,
                                       poster_path: String,
                                       production_companies: String,
                                       production_countries: String,
                                       release_date: String,
                                       revenue: Double,
                                       runtime: Double,
                                       spoken_languages: String,
                                       status: String,
                                       tagline: String,
                                       title: String,
                                       video: String,
                                       vote_average: Double,
                                       vote_count: Double,
                                       release_year: Int,
                                       release_month: Int,
                                       release_day: Int,
                                       roi: Double
                                     )

  private def toDbRow(m: Movie): MovieDbRow =
    MovieDbRow(
      id = m.id.toLong,
      adult = m.adult,
      belongs_to_collection = m.belongs_to_collection,
      budget = m.budget,
      genres = m.genres,
      homepage = m.homepage,
      imdb_id = m.imdb_id,
      original_language = m.original_language,
      original_title = m.original_title,
      overview = m.overview,
      popularity = m.popularity,
      poster_path = m.poster_path,
      production_companies = m.production_companies,
      production_countries = m.production_countries,
      release_date = m.release_date,
      revenue = m.revenue,
      runtime = m.runtime,
      spoken_languages = m.spoken_languages,
      status = m.status,
      tagline = m.tagline,
      title = m.title,
      video = m.video,
      vote_average = m.vote_average,
      vote_count = m.vote_count,
      release_year = m.release_year.toInt,
      release_month = m.release_month.toInt,
      release_day = m.release_day.toInt,
      roi = m.`return`
    )

  // ---- DDL ----
  private val createTable: Update0 =
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

  private val truncateTable: Update0 =
    sql"TRUNCATE TABLE movies_clean".update

  // ---- INSERT con REPLACE para evitar duplicados ----
  private val insertMany: Update[MovieDbRow] =
    Update[MovieDbRow](
      """
      REPLACE INTO movies_clean (
        id, adult, belongs_to_collection, budget, genres, homepage, imdb_id,
        original_language, original_title, overview, popularity, poster_path,
        production_companies, production_countries, release_date, revenue, runtime,
        spoken_languages, status, tagline, title, video, vote_average, vote_count,
        release_year, release_month, release_day, roi
      ) VALUES (
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?
      )
      """
    )

  // ---- Script SQL (evidencia) ----
  def generarSqlScript(movies: List[Movie]): String =
    def esc(s: String): String =
      Option(s).getOrElse("")
        .replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\r", "\\r")
        .replace("\n", "\\n")

    val ddl =
      """-- === DDL ===
        |CREATE TABLE IF NOT EXISTS movies_clean (
        |  id BIGINT PRIMARY KEY,
        |  adult VARCHAR(10),
        |  belongs_to_collection LONGTEXT,
        |  budget DOUBLE,
        |  genres LONGTEXT,
        |  homepage LONGTEXT,
        |  imdb_id VARCHAR(30),
        |  original_language VARCHAR(10),
        |  original_title LONGTEXT,
        |  overview LONGTEXT,
        |  popularity DOUBLE,
        |  poster_path LONGTEXT,
        |  production_companies LONGTEXT,
        |  production_countries LONGTEXT,
        |  release_date VARCHAR(20),
        |  revenue DOUBLE,
        |  runtime DOUBLE,
        |  spoken_languages LONGTEXT,
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
        |);
        |
        |-- Limpiar tabla antes de insertar
        |TRUNCATE TABLE movies_clean;
        |""".stripMargin

    // Eliminar duplicados antes de generar el SQL
    val moviesUnicos = movies.groupBy(_.id).map(_._2.head).toList

    val inserts = moviesUnicos.map { m =>
      val r = toDbRow(m)
      s"""INSERT INTO movies_clean (
         |  id, adult, belongs_to_collection, budget, genres, homepage, imdb_id,
         |  original_language, original_title, overview, popularity, poster_path,
         |  production_companies, production_countries, release_date, revenue, runtime,
         |  spoken_languages, status, tagline, title, video, vote_average, vote_count,
         |  release_year, release_month, release_day, roi
         |) VALUES (
         |  ${r.id},
         |  '${esc(r.adult)}',
         |  '${esc(r.belongs_to_collection)}',
         |  ${r.budget},
         |  '${esc(r.genres)}',
         |  '${esc(r.homepage)}',
         |  '${esc(r.imdb_id)}',
         |  '${esc(r.original_language)}',
         |  '${esc(r.original_title)}',
         |  '${esc(r.overview)}',
         |  ${r.popularity},
         |  '${esc(r.poster_path)}',
         |  '${esc(r.production_companies)}',
         |  '${esc(r.production_countries)}',
         |  '${esc(r.release_date)}',
         |  ${r.revenue},
         |  ${r.runtime},
         |  '${esc(r.spoken_languages)}',
         |  '${esc(r.status)}',
         |  '${esc(r.tagline)}',
         |  '${esc(r.title)}',
         |  '${esc(r.video)}',
         |  ${r.vote_average},
         |  ${r.vote_count},
         |  ${r.release_year},
         |  ${r.release_month},
         |  ${r.release_day},
         |  ${r.roi}
         |);
         |""".stripMargin
    }.mkString("\n")

    ddl + "\n-- === DML ===\n" + inserts

  def guardarSql(script: String, ruta: String): IO[Unit] =
    IO {
      val p = Paths.get(ruta)
      val parent = p.getParent
      if (parent != null) JFiles.createDirectories(parent)
      JFiles.write(p, script.getBytes(StandardCharsets.UTF_8))
    }

  def cargarEnDb(
                  movies: List[Movie],
                  batchSize: Int = 2000,
                  recargarDesdeCero: Boolean = false
                ): IO[Unit] =
    Database.init()
    Database.transactor.use { xa =>
      // Eliminar duplicados ANTES de convertir a DbRow
      val moviesUnicos = movies.groupBy(_.id).map(_._2.head).toList
      val rows = moviesUnicos.map(toDbRow)
      val chunks = rows.grouped(batchSize).toList

      val duplicadosEliminados = movies.length - moviesUnicos.length

      val program: ConnectionIO[Int] =
        for
          _ <- createTable.run
          _ <- if recargarDesdeCero then truncateTable.run else 0.pure[ConnectionIO]
          total <- chunks.foldLeft(0.pure[ConnectionIO]) { (acc, chunk) =>
            for
              soFar <- acc
              n <- insertMany.updateMany(chunk)
              _ <- FC.delay(print(".")) // Mostrar progreso
            yield soFar + n
          }
        yield total

      program.transact(xa).flatMap { total =>
        IO.println("") >>
          IO.println(s"✓ Carga completada: $total filas insertadas en movies_clean.") >>
          (if (duplicadosEliminados > 0)
            IO.println(s"ℹ️  Se eliminaron $duplicadosEliminados registros duplicados del dataset.")
          else IO.unit)
      }
    }

  def exportarYSubir(
                      movies: List[Movie],
                      rutaSql: String = "src/main/resources/sql/movies_clean.sql",
                      batchSize: Int = 2000,
                      recargarDesdeCero: Boolean = false
                    ): IO[Unit] =
    val script = generarSqlScript(movies)
    guardarSql(script, rutaSql) >>
      cargarEnDb(movies, batchSize = batchSize, recargarDesdeCero = recargarDesdeCero)