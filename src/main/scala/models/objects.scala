
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
      `return` = roi
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
  // Paso 1: Eliminar valores nulos y ceros en columnas críticas
  def eliminarValoresNulos(peliculas: List[Movie]): List[Movie] =
    peliculas.filter { m =>
      m.id > 0 &&
        m.budget > 0 &&
        m.revenue > 0 &&
        m.runtime > 0 &&
        m.popularity > 0 &&
        m.vote_count > 0 &&
        !m.title.trim.isEmpty &&
        !m.original_title.trim.isEmpty
    }

  // Paso 2: Validar rangos lógicos
  def validarRangos(peliculas: List[Movie]): List[Movie] =
    peliculas.filter { m =>
      m.release_year >= 1888 && m.release_year <= 2025 &&
        m.release_month >= 1 && m.release_month <= 12 &&
        m.release_day >= 1 && m.release_day <= 31 &&
        m.runtime < 500 && // Películas extremadamente largas
        m.vote_average >= 0 && m.vote_average <= 10 &&
        m.`return` >= -1.0 // ROI no puede ser menor a -100%
    }

  // Paso 3: Filtrar outliers usando IQR
  def filtrarOutliersIQR(peliculas: List[Movie]): List[Movie] =
    if (peliculas.isEmpty) return Nil

    val (bLow, bHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.budget))
    val (rLow, rHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.revenue))
    val (pLow, pHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.popularity))

    peliculas.filter { m =>
      m.budget >= bLow && m.budget <= bHigh &&
        m.revenue >= rLow && m.revenue <= rHigh &&
        m.popularity >= pLow && m.popularity <= pHigh
    }

  // Método flexible: permite 1 outlier por registro
  def filtrarOutliersFlexible(peliculas: List[Movie]): List[Movie] =
    if (peliculas.isEmpty) return Nil

    val (bLow, bHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.budget))
    val (rLow, rHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.revenue))
    val (pLow, pHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.popularity))
    val (retLow, retHigh, _, _) = DetectorOutliers.detectarOutliersIQR(peliculas.map(_.`return`))

    peliculas.filter { m =>
      val fueraDeRango = Seq(
        m.budget < bLow || m.budget > bHigh,
        m.revenue < rLow || m.revenue > rHigh,
        m.popularity < pLow || m.popularity > pHigh,
        m.`return` < retLow || m.`return` > retHigh
      ).count(identity)

      fueraDeRango < 2 // Permite 1 outlier
    }

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