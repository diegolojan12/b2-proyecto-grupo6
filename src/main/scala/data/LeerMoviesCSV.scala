package data

import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*

import java.io.PrintWriter
import scala.io.Source
import models.*

case class FilaCSVCompleta(
                            datos: Array[String],
                            collectionLimpia: Option[Collection],
                            genresLimpios: List[Genre],
                            productionCompaniesLimpias: List[ProductionCompany],
                            productionCountriesLimpios: List[ProductionCountry],
                            spokenLanguagesLimpios: List[SpokenLanguage],
                            keywordsLimpios: List[Keyword],
                            castLimpio: List[CastMember],
                            crewLimpia: List[CrewMember]
                          )

object LeerMoviesCSV extends App {

  val ruta = "src/main/resources/data/pi-movies-complete-2026-01-08 (1).csv"
  val rutaSalida = "src/main/resources/data/pi-movies-complete-2026-01-08-limpio.csv"

  val source = Source.fromFile(ruta, "UTF-8")
  val lines = source.getLines().toList
  source.close()

  val headers = lines.head.split(";").map(_.trim)

  // Índices de columnas JSON
  val collectionIdx = headers.indexOf("belongs_to_collection")
  val genresIdx = headers.indexOf("genres")
  val prodCompaniesIdx = headers.indexOf("production_companies")
  val prodCountriesIdx = headers.indexOf("production_countries")
  val spokenLangsIdx = headers.indexOf("spoken_languages")
  val keywordsIdx = headers.indexOf("keywords")
  val castIdx = headers.indexOf("cast")
  val crewIdx = headers.indexOf("crew")

  // ============================================================================
  // FUNCIONES DE LIMPIEZA
  // ============================================================================

  def cleanJson(jsonStr: String): String = {
    jsonStr.trim
      .replaceAll("'", "\"")
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replaceAll("""\\""", "")
  }

  def normalizarTexto(texto: String): Option[String] = {
    val limpio = texto.trim.replaceAll("\\s+", " ")
    if (limpio.isEmpty) None else Some(limpio)
  }

  def parseCSVLine(line: String): Array[String] = {
    val (fields, lastBuilder, _) = line.foldLeft(
      (Vector.empty[String], new StringBuilder, false)
    ) {
      case ((fields, current, inQuotes), char) => char match {
        case '"' =>
          (fields, current, !inQuotes)
        case ';' if !inQuotes =>
          (fields :+ current.toString, new StringBuilder, false)
        case _ =>
          current.append(char)
          (fields, current, inQuotes)
      }
    }
    (fields :+ lastBuilder.toString).toArray
  }

  def escaparCSV(texto: String): String = {
    if (texto.contains(";") || texto.contains("\"") || texto.contains("\n")) {
      "\"" + texto.replace("\"", "\"\"") + "\""
    } else {
      texto
    }
  }

  // ============================================================================
  // FUNCIONES DE PARSING Y LIMPIEZA POR TIPO
  // ============================================================================

  def limpiarCollection(json: String): Option[Collection] = {
    if (json.trim.isEmpty || json.trim == "[]" || json.trim == "{}") return None
    try {
      val limpio = cleanJson(json)
      decode[Collection](limpio).toOption.map(c => c.copy(
        name = c.name.flatMap(normalizarTexto),
        poster_path = c.poster_path.flatMap(normalizarTexto),
        backdrop_path = c.backdrop_path.flatMap(normalizarTexto)
      ))
    } catch {
      case _: Exception => None
    }
  }

  def limpiarGenres(json: String): List[Genre] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[Genre]](limpio).getOrElse(Nil).map(g => g.copy(
        name = g.name.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarProductionCompanies(json: String): List[ProductionCompany] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[ProductionCompany]](limpio).getOrElse(Nil).map(pc => pc.copy(
        name = pc.name.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarProductionCountries(json: String): List[ProductionCountry] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[ProductionCountry]](limpio).getOrElse(Nil).map(pc => pc.copy(
        iso_3166_1 = pc.iso_3166_1.flatMap(normalizarTexto),
        name = pc.name.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarSpokenLanguages(json: String): List[SpokenLanguage] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[SpokenLanguage]](limpio).getOrElse(Nil).map(sl => sl.copy(
        iso_639_1 = sl.iso_639_1.flatMap(normalizarTexto),
        name = sl.name.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarKeywords(json: String): List[Keyword] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[Keyword]](limpio).getOrElse(Nil).map(k => k.copy(
        name = k.name.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarCast(json: String): List[CastMember] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[CastMember]](limpio).getOrElse(Nil).map(c => c.copy(
        character = c.character.flatMap(normalizarTexto),
        credit_id = c.credit_id.flatMap(normalizarTexto),
        name = c.name.flatMap(normalizarTexto),
        profile_path = c.profile_path.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  def limpiarCrew(json: String): List[CrewMember] = {
    if (json.trim.isEmpty || json.trim == "[]") return Nil
    try {
      val limpio = cleanJson(json)
      decode[List[CrewMember]](limpio).getOrElse(Nil).map(c => c.copy(
        credit_id = c.credit_id.flatMap(normalizarTexto),
        name = c.name.flatMap(normalizarTexto),
        department = c.department.flatMap(normalizarTexto),
        job = c.job.flatMap(normalizarTexto),
        profile_path = c.profile_path.flatMap(normalizarTexto)
      )).distinct
    } catch {
      case _: Exception => Nil
    }
  }

  // ============================================================================
  // PROCESAMIENTO PRINCIPAL
  // ============================================================================

  println("Procesando filas...")
  val filasLimpias: List[FilaCSVCompleta] = lines.tail.zipWithIndex.map { case (line, idx) =>
    if (idx % 1000 == 0) println(s"Procesadas $idx filas...")

    val parts = parseCSVLine(line)

    FilaCSVCompleta(
      datos = parts,
      collectionLimpia = if (parts.length > collectionIdx && collectionIdx >= 0)
        limpiarCollection(parts(collectionIdx)) else None,
      genresLimpios = if (parts.length > genresIdx && genresIdx >= 0)
        limpiarGenres(parts(genresIdx)) else Nil,
      productionCompaniesLimpias = if (parts.length > prodCompaniesIdx && prodCompaniesIdx >= 0)
        limpiarProductionCompanies(parts(prodCompaniesIdx)) else Nil,
      productionCountriesLimpios = if (parts.length > prodCountriesIdx && prodCountriesIdx >= 0)
        limpiarProductionCountries(parts(prodCountriesIdx)) else Nil,
      spokenLanguagesLimpios = if (parts.length > spokenLangsIdx && spokenLangsIdx >= 0)
        limpiarSpokenLanguages(parts(spokenLangsIdx)) else Nil,
      keywordsLimpios = if (parts.length > keywordsIdx && keywordsIdx >= 0)
        limpiarKeywords(parts(keywordsIdx)) else Nil,
      castLimpio = if (parts.length > castIdx && castIdx >= 0)
        limpiarCast(parts(castIdx)) else Nil,
      crewLimpia = if (parts.length > crewIdx && crewIdx >= 0)
        limpiarCrew(parts(crewIdx)) else Nil
    )
  }

  // ============================================================================
  // EXPORTAR CSV LIMPIO
  // ============================================================================

  println("\nExportando CSV limpio...")
  val writer = new PrintWriter(rutaSalida, "UTF-8")

  // Encabezados
  writer.println("belongs_to_collection;genres;production_companies;production_countries;spoken_languages;keywords;cast;crew")

  // Datos limpios
  filasLimpias.foreach { fila =>
    val collection = fila.collectionLimpia.map(_.asJson.noSpaces).getOrElse("{}")
    val genres = fila.genresLimpios.asJson.noSpaces
    val companies = fila.productionCompaniesLimpias.asJson.noSpaces
    val countries = fila.productionCountriesLimpios.asJson.noSpaces
    val languages = fila.spokenLanguagesLimpios.asJson.noSpaces
    val keywords = fila.keywordsLimpios.asJson.noSpaces
    val cast = fila.castLimpio.asJson.noSpaces
    val crew = fila.crewLimpia.asJson.noSpaces

    writer.println(s"${escaparCSV(collection)};${escaparCSV(genres)};${escaparCSV(companies)};${escaparCSV(countries)};${escaparCSV(languages)};${escaparCSV(keywords)};${escaparCSV(cast)};${escaparCSV(crew)}")
  }

  writer.close()

  // ============================================================================
  // ESTADÍSTICAS
  // ============================================================================

  val totalFilas = filasLimpias.size

  println("\n" + "=" * 80)
  println("RESUMEN DE PROCESAMIENTO JSON COMPLETO")
  println("=" * 80)
  println(f"Total de filas procesadas: $totalFilas%,d")
  println()

  // Collections
  val filasConCollection = filasLimpias.count(_.collectionLimpia.isDefined)
  println(f"Collections: $filasConCollection%,d filas (${filasConCollection * 100.0 / totalFilas}%.2f%%)")

  // Genres
  val totalGenres = filasLimpias.map(_.genresLimpios.size).sum
  val filasConGenres = filasLimpias.count(_.genresLimpios.nonEmpty)
  println(f"Genres: $totalGenres%,d items en $filasConGenres%,d filas (${filasConGenres * 100.0 / totalFilas}%.2f%%)")

  // Production Companies
  val totalCompanies = filasLimpias.map(_.productionCompaniesLimpias.size).sum
  val filasConCompanies = filasLimpias.count(_.productionCompaniesLimpias.nonEmpty)
  println(f"Companies: $totalCompanies%,d items en $filasConCompanies%,d filas (${filasConCompanies * 100.0 / totalFilas}%.2f%%)")

  // Production Countries
  val totalCountries = filasLimpias.map(_.productionCountriesLimpios.size).sum
  val filasConCountries = filasLimpias.count(_.productionCountriesLimpios.nonEmpty)
  println(f"Countries: $totalCountries%,d items en $filasConCountries%,d filas (${filasConCountries * 100.0 / totalFilas}%.2f%%)")

  // Spoken Languages
  val totalLanguages = filasLimpias.map(_.spokenLanguagesLimpios.size).sum
  val filasConLanguages = filasLimpias.count(_.spokenLanguagesLimpios.nonEmpty)
  println(f"Languages: $totalLanguages%,d items en $filasConLanguages%,d filas (${filasConLanguages * 100.0 / totalFilas}%.2f%%)")

  // Keywords
  val totalKeywords = filasLimpias.map(_.keywordsLimpios.size).sum
  val filasConKeywords = filasLimpias.count(_.keywordsLimpios.nonEmpty)
  println(f"Keywords: $totalKeywords%,d items en $filasConKeywords%,d filas (${filasConKeywords * 100.0 / totalFilas}%.2f%%)")

  // Cast
  val totalCast = filasLimpias.map(_.castLimpio.size).sum
  val filasConCast = filasLimpias.count(_.castLimpio.nonEmpty)
  println(f"Cast: $totalCast%,d items en $filasConCast%,d filas (${filasConCast * 100.0 / totalFilas}%.2f%%)")

  // Crew
  val totalCrew = filasLimpias.map(_.crewLimpia.size).sum
  val filasConCrew = filasLimpias.count(_.crewLimpia.nonEmpty)
  println(f"Crew: $totalCrew%,d items en $filasConCrew%,d filas (${filasConCrew * 100.0 / totalFilas}%.2f%%)")

  println("\n" + "=" * 80)
  println("PROCESAMIENTO COMPLETADO ✓")
  println(s"Archivo guardado en: $rutaSalida")
  println("=" * 80)
}