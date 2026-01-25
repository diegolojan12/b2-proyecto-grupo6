package utilities

import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*

import models.{MovieRaw, Movie}
import models.*
import models.Decoders.given

import etl.MovieEtlToDb

object LimpiezaDatos extends IOApp.Simple:

  val filePath = Path("src/main/resources/data/pi-movies-complete-2026-01-08.csv")

  def reportarFechasFaltantes(peliculas: List[Movie]): IO[Unit] =
    val faltantes = peliculas.count(_.release_date == "null")
    val porcentaje = faltantes * 100.0 / peliculas.length
    IO.println(f"  Fechas faltantes:          $faltantes%,d ($porcentaje%.2f%%)")

  def run: IO[Unit] =
    val lecturaCSV: IO[List[Movie]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .map(Transformador.procesarMovie)
      .compile
      .toList
      .handleErrorWith { e =>
        IO.println(s"Error al leer el CSV: ${e.getMessage}") >> IO.pure(Nil)
      }

    lecturaCSV.flatMap { peliculasOriginales =>
      val total = peliculasOriginales.length

      // ANÁLISIS DE CALIDAD DE DATOS
      val calidadBudget = AnalizadorCalidad.analizarCalidadNumerica(
        "budget", peliculasOriginales.map(_.budget), total
      )
      val calidadRevenue = AnalizadorCalidad.analizarCalidadNumerica(
        "revenue", peliculasOriginales.map(_.revenue), total
      )
      val calidadPopularity = AnalizadorCalidad.analizarCalidadNumerica(
        "popularity", peliculasOriginales.map(_.popularity), total
      )
      val calidadTitle = AnalizadorCalidad.analizarCalidadTexto(
        "title", peliculasOriginales.map(_.title), total
      )

      // DETECCIÓN DE OUTLIERS
      val (bLow, bHigh, bOutInf, bOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.budget)
      )
      val (rLow, rHigh, rOutInf, rOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.revenue)
      )
      val (pLow, pHigh, pOutInf, pOutSup) = DetectorOutliers.detectarOutliersIQR(
        peliculasOriginales.map(_.popularity)
      )

      val budgetZScore = DetectorOutliers.detectarOutliersZScore(
        peliculasOriginales.map(_.budget)
      )
      val revenueZScore = DetectorOutliers.detectarOutliersZScore(
        peliculasOriginales.map(_.revenue)
      )

      // PROCESO DE LIMPIEZA POR ETAPAS
      val etapa1 = Limpiador.eliminarValoresNulos(peliculasOriginales)
      val etapa2 = Limpiador.validarRangos(etapa1)
      val etapa3Estricta = Limpiador.filtrarOutliersIQR(etapa2)
      val etapa3Flexible = Limpiador.filtrarOutliersFlexible(etapa2)

      // Estadísticas finales
      val statsBudget = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.budget))
      val statsRevenue = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.revenue))
      val statsPopularity = Estadisticos.calcularEstadisticas(etapa3Flexible.map(_.popularity))

      // REPORTE
      IO.println("=" * 90) >>
        IO.println("              REPORTE DE LIMPIEZA DE DATOS - DATASET DE PELÍCULAS") >>
        IO.println("=" * 90) >>
        IO.println("") >>
        reportarFechasFaltantes(peliculasOriginales) >>
        IO.println("1. ANÁLISIS DE CALIDAD DE DATOS (Valores Nulos, Ceros y Vacíos)") >>
        IO.println("-" * 90) >>
        IO.println(f"Columna           Total    Nulos    Ceros    Negativos  Vacíos   Válidos") >>
        IO.println(f"${calidadBudget.columna}%-15s ${calidadBudget.total}%,7d  ${calidadBudget.nulos}%,7d  ${calidadBudget.ceros}%,7d  ${calidadBudget.negativos}%,10d  ${calidadBudget.vacios}%,7d  ${calidadBudget.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadRevenue.columna}%-15s ${calidadRevenue.total}%,7d  ${calidadRevenue.nulos}%,7d  ${calidadRevenue.ceros}%,7d  ${calidadRevenue.negativos}%,10d  ${calidadRevenue.vacios}%,7d  ${calidadRevenue.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadPopularity.columna}%-15s ${calidadPopularity.total}%,7d  ${calidadPopularity.nulos}%,7d  ${calidadPopularity.ceros}%,7d  ${calidadPopularity.negativos}%,10d  ${calidadPopularity.vacios}%,7d  ${calidadPopularity.porcentajeValidos}%6.2f%%") >>
        IO.println(f"${calidadTitle.columna}%-15s ${calidadTitle.total}%,7d  ${calidadTitle.nulos}%,7d  ${calidadTitle.ceros}%,7d  ${calidadTitle.negativos}%,10d  ${calidadTitle.vacios}%,7d  ${calidadTitle.porcentajeValidos}%6.2f%%") >>
        IO.println("") >>
        IO.println("2. DETECCIÓN DE VALORES ATÍPICOS (OUTLIERS)") >>
        IO.println("-" * 90) >>
        IO.println("Método IQR (Rango Intercuartílico):") >>
        IO.println(f"  Budget:") >>
        IO.println(f"    Límites:       [$bLow%,.2f - $bHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${bOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${bOutSup}%,d registros") >>
        IO.println(f"    Total:         ${bOutInf + bOutSup}%,d (${(bOutInf + bOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println(f"  Revenue:") >>
        IO.println(f"    Límites:       [$rLow%,.2f - $rHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${rOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${rOutSup}%,d registros") >>
        IO.println(f"    Total:         ${rOutInf + rOutSup}%,d (${(rOutInf + rOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println(f"  Popularity:") >>
        IO.println(f"    Límites:       [$pLow%,.2f - $pHigh%,.2f]") >>
        IO.println(f"    Outliers inf.: ${pOutInf}%,d registros") >>
        IO.println(f"    Outliers sup.: ${pOutSup}%,d registros") >>
        IO.println(f"    Total:         ${pOutInf + pOutSup}%,d (${(pOutInf + pOutSup).toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("Método Z-Score (|z| > 3):") >>
        IO.println(f"  Budget:        ${budgetZScore}%,d outliers (${budgetZScore.toDouble / total * 100}%.2f%%)") >>
        IO.println(f"  Revenue:       ${revenueZScore}%,d outliers (${revenueZScore.toDouble / total * 100}%.2f%%)") >>
        IO.println("") >>
        IO.println("3. PROCESO DE LIMPIEZA POR ETAPAS") >>
        IO.println("-" * 90) >>
        IO.println(f"Registros originales:                    ${peliculasOriginales.length}%,7d") >>
        IO.println(f"Después de eliminar nulos/ceros:         ${etapa1.length}%,7d (${(total - etapa1.length)}%,d eliminados)") >>
        IO.println(f"Después de validar rangos:               ${etapa2.length}%,7d (${(etapa1.length - etapa2.length)}%,d eliminados)") >>
        IO.println(f"Después de filtrar outliers (estricto):  ${etapa3Estricta.length}%,7d (${(etapa2.length - etapa3Estricta.length)}%,d eliminados)") >>
        IO.println(f"Después de filtrar outliers (flexible):  ${etapa3Flexible.length}%,7d (${(etapa2.length - etapa3Flexible.length)}%,d eliminados)") >>
        IO.println("") >>
        IO.println(f"Porcentaje de datos conservados:         ${etapa3Flexible.length.toDouble / total * 100}%.2f%%") >>
        IO.println(f"Porcentaje de datos eliminados:          ${(total - etapa3Flexible.length).toDouble / total * 100}%.2f%%") >>
        IO.println("") >>
        IO.println("4. ESTADÍSTICAS DESCRIPTIVAS (DATOS LIMPIOS - Método Flexible)") >>
        IO.println("-" * 90) >>
        IO.println(f"Budget:") >>
        IO.println(f"  Mínimo:       ${statsBudget("min")}%,.2f") >>
        IO.println(f"  Q1:            ${statsBudget("q1")}%,.2f") >>
        IO.println(f"  Mediana:       ${statsBudget("mediana")}%,.2f") >>
        IO.println(f"  Media:         ${statsBudget("media")}%,.2f") >>
        IO.println(f"  Q3:            ${statsBudget("q3")}%,.2f") >>
        IO.println(f"  Máximo:        ${statsBudget("max")}%,.2f") >>
        IO.println(f"  Desv. Est.:    ${statsBudget("desv_std")}%,.2f") >>
        IO.println("") >>
        IO.println(f"Revenue:") >>
        IO.println(f"  Mínimo:        ${statsRevenue("min")}%,.2f") >>
        IO.println(f"  Mediana:       ${statsRevenue("mediana")}%,.2f") >>
        IO.println(f"  Media:         ${statsRevenue("media")}%,.2f") >>
        IO.println(f"  Máximo:        ${statsRevenue("max")}%,.2f") >>
        IO.println(f"  Desv. Est.:    ${statsRevenue("desv_std")}%,.2f") >>
        IO.println("") >>
        IO.println(f"Popularity:") >>
        IO.println(f"  Mínimo:        ${statsPopularity("min")}%,.2f") >>
        IO.println(f"  Mediana:       ${statsPopularity("mediana")}%,.2f") >>
        IO.println(f"  Media:         ${statsPopularity("media")}%,.2f") >>
        IO.println(f"  Máximo:        ${statsPopularity("max")}%,.2f") >>
        IO.println(f"  Desv. Est.:    ${statsPopularity("desv_std")}%,.2f") >>
        IO.println("") >>
        IO.println("=" * 90) >>
        IO.println("✓ Limpieza completada exitosamente") >>
        IO.println("✓ Dataset listo para análisis exploratorio") >>
        IO.println("=" * 90) >>
        IO.println("\nℹ️  TIP: Use la opción [5] del menú para cargar estos datos en MySQL\n")
    }