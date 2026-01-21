package etl

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*
import config.Database

object DiagnosticoETL extends IOApp.Simple {

  def contarRegistros(tabla: String): ConnectionIO[Long] =
    Fragment.const(s"SELECT COUNT(*) FROM $tabla").query[Long].unique

  def verificarEstructura(tabla: String): ConnectionIO[List[String]] =
    sql"SHOW COLUMNS FROM ${Fragment.const(tabla)}".query[String].to[List]

  def mostrarEjemploGenres: ConnectionIO[List[(Long, String)]] =
    sql"SELECT genre_id, name FROM genres LIMIT 5".query[(Long, String)].to[List]

  def mostrarEjemploMovies: ConnectionIO[List[(Long, String, Double)]] =
    sql"SELECT id, title, budget FROM movies LIMIT 5".query[(Long, String, Double)].to[List]

  def run: IO[Unit] = {
    Database.init()
    Database.transactor.use { xa =>
      val diagnostico = for {
        // Contar registros en todas las tablas
        countMoviesClean <- contarRegistros("movies_clean")
        countMovies <- contarRegistros("movies")
        countGenres <- contarRegistros("genres")
        countCompanies <- contarRegistros("production_companies")
        countCountries <- contarRegistros("production_countries")
        countLanguages <- contarRegistros("spoken_languages")
        countCollections <- contarRegistros("collections")
        countKeywords <- contarRegistros("keywords")

        // Contar relaciones
        countMovieGenres <- contarRegistros("movie_genres")
        countMovieCompanies <- contarRegistros("movie_production_companies")
        countMovieCountries <- contarRegistros("movie_production_countries")
        countMovieLanguages <- contarRegistros("movie_spoken_languages")
        countMovieCollections <- contarRegistros("movie_collections")

        // Ejemplos
        ejemploGenres <- mostrarEjemploGenres
        ejemploMovies <- mostrarEjemploMovies

      } yield (
        countMoviesClean, countMovies, countGenres, countCompanies,
        countCountries, countLanguages, countCollections, countKeywords,
        countMovieGenres, countMovieCompanies, countMovieCountries,
        countMovieLanguages, countMovieCollections,
        ejemploGenres, ejemploMovies
      )

      diagnostico.transact(xa).flatMap { case (
        moviesClean, movies, genres, companies, countries, languages,
        collections, keywords, mvGenres, mvCompanies, mvCountries,
        mvLanguages, mvCollections, ejGenres, ejMovies
        ) =>
        IO.println("\n" + "=" * 80) >>
          IO.println("                    DIAGNÓSTICO DEL ETL") >>
          IO.println("=" * 80) >>
          IO.println("\n📊 TABLAS PRINCIPALES:") >>
          IO.println(f"  movies_clean (staging):        ${moviesClean}%,10d registros") >>
          IO.println(f"  movies (normalizada):          ${movies}%,10d registros") >>
          IO.println("\n📚 TABLAS CATÁLOGO:") >>
          IO.println(f"  genres:                        ${genres}%,10d registros") >>
          IO.println(f"  production_companies:          ${companies}%,10d registros") >>
          IO.println(f"  production_countries:          ${countries}%,10d registros") >>
          IO.println(f"  spoken_languages:              ${languages}%,10d registros") >>
          IO.println(f"  collections:                   ${collections}%,10d registros") >>
          IO.println(f"  keywords:                      ${keywords}%,10d registros") >>
          IO.println("\n🔗 TABLAS PUENTE (RELACIONES):") >>
          IO.println(f"  movie_genres:                  ${mvGenres}%,10d relaciones") >>
          IO.println(f"  movie_production_companies:    ${mvCompanies}%,10d relaciones") >>
          IO.println(f"  movie_production_countries:    ${mvCountries}%,10d relaciones") >>
          IO.println(f"  movie_spoken_languages:        ${mvLanguages}%,10d relaciones") >>
          IO.println(f"  movie_collections:             ${mvCollections}%,10d relaciones") >>
          IO.println("\n" + "=" * 80) >>
          {
            if (moviesClean > 0 && movies == 0) {
              IO.println("\n⚠️  PROBLEMA DETECTADO:") >>
                IO.println("  • movies_clean tiene datos pero movies está vacía") >>
                IO.println("  • El ETL normalizado NO se está ejecutando correctamente") >>
                IO.println("\n💡 SOLUCIÓN:") >>
                IO.println("  1. Verifica que ModeloNormalizadoEtl.scala esté actualizado") >>
                IO.println("  2. Verifica los logs de errores durante la carga") >>
                IO.println("  3. Ejecuta la opción [5] del menú nuevamente") >>
                IO.println("\n🔍 EJEMPLOS DE DATOS EN movies_clean:") >>
                IO.println("  (Ejecuta en MySQL para ver el contenido)") >>
                IO.println("  SELECT id, title, genres, production_companies FROM movies_clean LIMIT 3;")
            } else if (movies > 0 && genres == 0) {
              IO.println("\n⚠️  PROBLEMA DETECTADO:") >>
                IO.println("  • movies tiene datos pero las tablas catálogo están vacías") >>
                IO.println("  • El parseo de JSON NO está funcionando") >>
                IO.println("\nMOSTRANDO EJEMPLO DE MOVIES:") >>
                ejMovies.traverse_ { case (id, title, budget) =>
                  IO.println(f"  ID: $id%d | $title | Budget: $$${budget}%,.2f")
                } >>
                IO.println("\nCAUSA PROBABLE:") >>
                IO.println("  • Los campos JSON en Movie están vacíos o mal formateados") >>
                IO.println("  • Verifica el contenido de: m.genres, m.production_companies, etc.")
            } else if (movies > 0 && genres > 0) {
              IO.println("\nETL COMPLETADO EXITOSAMENTE") >>
                IO.println("\nEJEMPLO DE GÉNEROS:") >>
                ejGenres.traverse_ { case (id, name) =>
                  IO.println(f"  ID: $id%d | $name")
                } >>
                IO.println("\nEJEMPLO DE PELÍCULAS:") >>
                ejMovies.traverse_ { case (id, title, budget) =>
                  IO.println(f"  ID: $id%d | $title | Budget: $$${budget}%,.2f")
                }
            } else {
              IO.println("\nTODAS LAS TABLAS ESTÁN VACÍAS") >>
                IO.println("  • Ejecuta la opción [5] del menú para cargar datos")
            }
          } >>
          IO.println("\n" + "=" * 80 + "\n")
      }.handleErrorWith { e =>
        IO.println(s"\nError durante el diagnóstico: ${e.getMessage}") >>
          IO.println(s"   Tipo: ${e.getClass.getSimpleName}") >>
          IO(e.printStackTrace()) >>
        IO.unit
      }
    }
  }
}