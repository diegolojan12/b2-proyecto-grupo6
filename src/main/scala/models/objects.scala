
package models
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import models.{Movie, MovieRaw}

object Transformador:
  // Extraer año, mes y día de release_date
  def parsearFecha(fecha: String): (Double, Double, Double) =
    try {
      val partes = fecha.split("-")
      if (partes.length == 3) {
        val year = partes(0).toDouble
        val month = partes(1).toDouble
        val day = partes(2).toDouble
        (year, month, day)
      } else (0.0, 0.0, 0.0)
    } catch {
      case _: Exception => (0.0, 0.0, 0.0)
    }

  // Calcular ROI (Return on Investment)
  def calcularReturn(budget: Double, revenue: Double): Double =
    if (budget > 0) (revenue - budget) / budget else 0.0

  // Convertir MovieRaw a Movie con columnas calculadas
  def procesarMovie(raw: MovieRaw): Movie =
    val (year, month, day) = parsearFecha(raw.release_date)
    val roi = calcularReturn(raw.budget, raw.revenue)

    Movie(
      adult = raw.adult,
      belongs_to_collection = raw.belongs_to_collection,
      budget = raw.budget,
      genres = raw.genres,
      homepage = raw.homepage,
      id = raw.id,
      imdb_id = raw.imdb_id,
      original_language = raw.original_language,
      original_title = raw.original_title,
      overview = raw.overview,
      popularity = raw.popularity,
      poster_path = raw.poster_path,
      production_companies = raw.production_companies,
      production_countries = raw.production_countries,
      release_date = raw.release_date,
      revenue = raw.revenue,
      runtime = raw.runtime,
      spoken_languages = raw.spoken_languages,
      status = raw.status,
      tagline = raw.tagline,
      title = raw.title,
      video = raw.video,
      vote_average = raw.vote_average,
      vote_count = raw.vote_count,
      release_year = year,
      release_month = month,
      release_day = day,
      `return` = roi,
      keywords = raw.keywords, // ← NUEVO
      cast = raw.cast, // ← NUEVO
      crew = raw.crew // ← NUEVO
    )

import models.CalidadColumna

object AnalizadorCalidad:
  def analizarCalidadNumerica(nombre: String, datos: List[Double], total: Int): CalidadColumna =
    val nulos = datos.count(d => d.isNaN || d.isInfinite)
    val ceros = datos.count(_ == 0.0)
    val negativos = datos.count(_ < 0.0)
    val validos = total - nulos - ceros - negativos

    CalidadColumna(
      columna = nombre,
      total = total,
      nulos = nulos,
      ceros = ceros,
      negativos = negativos,
      vacios = 0,
      porcentajeValidos = if (total > 0) (validos.toDouble / total * 100) else 0.0
    )

  def analizarCalidadTexto(nombre: String, datos: List[String], total: Int): CalidadColumna =
    val vacios = datos.count(s => s == null || s.trim.isEmpty)
    val validos = total - vacios

    CalidadColumna(
      columna = nombre,
      total = total,
      nulos = 0,
      ceros = 0,
      negativos = 0,
      vacios = vacios,
      porcentajeValidos = if (total > 0) (validos.toDouble / total * 100) else 0.0
    )


object Decoders:
  given CsvRowDecoder[MovieRaw, String] = deriveCsvRowDecoder[MovieRaw]
  given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]
  given CsvRowDecoder[MovieText, String] = deriveCsvRowDecoder[MovieText]
  given CsvRowDecoder[MovieNumeric, String] = deriveCsvRowDecoder[MovieNumeric]

import models.Movie

object Limpiador:
  // Función auxiliar para normalizar strings vacíos
  private def normalizarString(s: String): String =
    if (s == null || s.trim.isEmpty) "null" else s.trim

  // Función para validar y corregir fechas
  private def validarFecha(fecha: String): String = {
    if (fecha == null || fecha.trim.isEmpty) return "null"

    try {
      // Intentar parsear la fecha en formato YYYY-MM-DD
      val partes = fecha.trim.split("-")
      if (partes.length == 3) {
        val year = partes(0).toInt
        val month = partes(1).toInt
        val day = partes(2).toInt

        // Validar rangos
        if (year < 1888 || year > 2025) return "null"
        if (month < 1 || month > 12) return "null"
        if (day < 1 || day > 31) return "null"

        // Retornar en formato correcto YYYY-MM-DD
        f"$year%04d-$month%02d-$day%02d"
      } else {
        "null"
      }
    } catch {
      case _: Exception => "null"
    }
  }

  // Paso 1: Normalizar valores (NO eliminar registros)
  def eliminarValoresNulos(peliculas: List[Movie]): List[Movie] =
    peliculas.map { m =>
      m.copy(
        adult = normalizarString(m.adult),
        belongs_to_collection = normalizarString(m.belongs_to_collection),
        genres = normalizarString(m.genres),
        homepage = normalizarString(m.homepage),
        imdb_id = normalizarString(m.imdb_id),
        original_language = normalizarString(m.original_language),
        original_title = normalizarString(m.original_title),
        overview = normalizarString(m.overview),
        poster_path = normalizarString(m.poster_path),
        production_companies = normalizarString(m.production_companies),
        production_countries = normalizarString(m.production_countries),
        release_date = validarFecha(m.release_date), // ← APLICAR VALIDACIÓN DE FECHA
        spoken_languages = normalizarString(m.spoken_languages),
        status = normalizarString(m.status),
        tagline = normalizarString(m.tagline),
        title = normalizarString(m.title),
        video = normalizarString(m.video),
        keywords = normalizarString(m.keywords),
        cast = normalizarString(m.cast),
        crew = normalizarString(m.crew),
        budget = if (m.budget < 0) 0.0 else m.budget,
        revenue = if (m.revenue < 0) 0.0 else m.revenue,
        runtime = if (m.runtime < 0) 0.0 else m.runtime,
        popularity = if (m.popularity < 0) 0.0 else m.popularity,
        vote_count = if (m.vote_count < 0) 0.0 else m.vote_count,
        vote_average = if (m.vote_average < 0) 0.0 else m.vote_average
      )
    }

  // Paso 2: Normalizar valores fuera de rango
  def validarRangos(peliculas: List[Movie]): List[Movie] =
    peliculas.map { m =>
      m.copy(
        release_year = if (m.release_year < 1888 || m.release_year > 2025) 0.0 else m.release_year,
        release_month = if (m.release_month < 1 || m.release_month > 12) 0.0 else m.release_month,
        release_day = if (m.release_day < 1 || m.release_day > 31) 0.0 else m.release_day,
        runtime = if (m.runtime > 500) 0.0 else m.runtime,
        vote_average = if (m.vote_average < 0 || m.vote_average > 10) 0.0 else m.vote_average,
        `return` = if (m.`return` < -1.0) 0.0 else m.`return`
      )
    }

  // Paso 3: NO filtrar outliers (mantener todo)
  def filtrarOutliersIQR(peliculas: List[Movie]): List[Movie] =
    peliculas

  // Método flexible: NO filtrar, solo devolver todo
  def filtrarOutliersFlexible(peliculas: List[Movie]): List[Movie] =
    peliculas



object DetectorOutliers:
  // Detección de outliers usando método IQR
  def detectarOutliersIQR(datos: List[Double]): (Double, Double, Int, Int) =
    if (datos.size < 4) return (0.0, 0.0, 0, 0)

    val ordenados = datos.sorted
    val q1 = Estadisticos.calcularCuartil(ordenados, 0.25)
    val q3 = Estadisticos.calcularCuartil(ordenados, 0.75)
    val iqr = q3 - q1

    val limiteInferior = math.max(0, q1 - 1.5 * iqr)
    val limiteSuperior = q3 + 1.5 * iqr

    val outliersInferiores = datos.count(d => d < limiteInferior)
    val outliersSuperiores = datos.count(d => d > limiteSuperior)

    (limiteInferior, limiteSuperior, outliersInferiores, outliersSuperiores)

  // Detección de outliers usando Z-score
  def detectarOutliersZScore(datos: List[Double], umbral: Double = 3.0): Int =
    if (datos.isEmpty) return 0

    val media = datos.sum / datos.size
    val varianza = datos.map(x => math.pow(x - media, 2)).sum / datos.size
    val desviacion = math.sqrt(varianza)

    if (desviacion == 0.0) 0
    else datos.count(d => math.abs((d - media) / desviacion) > umbral)