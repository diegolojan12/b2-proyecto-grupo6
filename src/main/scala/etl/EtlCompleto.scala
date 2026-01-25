package etl

import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import models.{MovieRaw, Movie, Estadisticos, DetectorOutliers}
import models.*
import models.Decoders.given

object EtlCompleto extends IOApp.Simple {

  val csvSucio = Path("src/main/resources/data/pi-movies-complete-2026-01-08 (1).csv")
  val sqlOutputLimpio = Path("src/main/resources/sql/movies_clean.sql")
  val sqlOutputModeloLogico = Path("src/main/resources/sql/modelo_logico_ddl.sql")

  def leerCSVSucio: IO[List[Movie]] =
    Files[IO]
      .readAll(csvSucio)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .map(Transformador.procesarMovie)
      .compile
      .toList
      .handleErrorWith { e =>
        IO.println(s"Error al leer el CSV: ${e.getMessage}") >> IO.pure(Nil)
      }

  def aplicarLimpieza(datos: List[Movie]): IO[List[Movie]] =
    IO {
      val paso1 = Limpiador.eliminarValoresNulos(datos)
      val paso2 = Limpiador.validarRangos(paso1)
      val paso3 = Limpiador.filtrarOutliersFlexible(paso2)
      paso3
    }

  def mostrarResumenLimpieza(originales: Int, limpios: Int): IO[Unit] =
    IO.println("") >>
      IO.println("=" * 80) >>
      IO.println("           RESUMEN DEL PROCESO DE LIMPIEZA") >>
      IO.println("=" * 80) >>
      IO.println(f"  Registros originales:        ${originales}%,8d") >>
      IO.println(f"  Registros después de limpieza: ${limpios}%,8d") >>
      IO.println(f"  Registros eliminados:        ${originales - limpios}%,8d") >>
      IO.println(f"  Porcentaje conservado:       ${(limpios.toDouble / originales * 100)}%6.2f%%") >>
      IO.println("=" * 80) >>
      IO.println("")

  def mostrarEstadisticasFinales(datos: List[Movie]): IO[Unit] = {
    val statsBudget = Estadisticos.calcularEstadisticas(datos.map(_.budget))
    val statsRevenue = Estadisticos.calcularEstadisticas(datos.map(_.revenue))
    val statsPopularity = Estadisticos.calcularEstadisticas(datos.map(_.popularity))

    IO.println("=" * 80) >>
      IO.println("           ESTADÍSTICAS DE DATOS LIMPIOS") >>
      IO.println("=" * 80) >>
      IO.println("\nBUDGET:") >>
      IO.println(f"  Mínimo:        ${statsBudget("min")}%,.2f") >>
      IO.println(f"  Q1:            ${statsBudget("q1")}%,.2f") >>
      IO.println(f"  Mediana:       ${statsBudget("mediana")}%,.2f") >>
      IO.println(f"  Media:         ${statsBudget("media")}%,.2f") >>
      IO.println(f"  Q3:            ${statsBudget("q3")}%,.2f") >>
      IO.println(f"  Máximo:        ${statsBudget("max")}%,.2f") >>
      IO.println(f"  Desv. Est.:    ${statsBudget("desv_std")}%,.2f") >>
      IO.println("\nREVENUE:") >>
      IO.println(f"  Mínimo:        ${statsRevenue("min")}%,.2f") >>
      IO.println(f"  Mediana:       ${statsRevenue("mediana")}%,.2f") >>
      IO.println(f"  Media:         ${statsRevenue("media")}%,.2f") >>
      IO.println(f"  Máximo:        ${statsRevenue("max")}%,.2f") >>
      IO.println(f"  Desv. Est.:    ${statsRevenue("desv_std")}%,.2f") >>
      IO.println("\nPOPULARITY:") >>
      IO.println(f"  Mínimo:        ${statsPopularity("min")}%,.2f") >>
      IO.println(f"  Mediana:       ${statsPopularity("mediana")}%,.2f") >>
      IO.println(f"  Media:         ${statsPopularity("media")}%,.2f") >>
      IO.println(f"  Máximo:        ${statsPopularity("max")}%,.2f") >>
      IO.println(f"  Desv. Est.:    ${statsPopularity("desv_std")}%,.2f") >>
      IO.println("=" * 80)
  }

  def run: IO[Unit] = {
    IO.println("\n╔═══════════════════════════════════════════════════════════════╗") >>
      IO.println("║           ETL COMPLETO: CSV → LIMPIEZA → SQL → MYSQL          ║") >>
      IO.println("╚═══════════════════════════════════════════════════════════════╝\n") >>
      IO.println("[PASO 1/6] Leyendo CSV sucio...") >>
      leerCSVSucio.flatMap { datosOriginales =>
        if (datosOriginales.isEmpty) {
          IO.println("✗ No se pudieron leer datos del CSV. Abortando proceso.")
        } else {
          IO.println(s"✓ CSV leído: ${datosOriginales.length} registros\n") >>
            IO.println("[PASO 2/6] Aplicando pipeline de limpieza...") >>
            aplicarLimpieza(datosOriginales).flatMap { datosLimpios =>
              IO.println(s"✓ Limpieza completada: ${datosLimpios.length} registros válidos\n") >>
                mostrarResumenLimpieza(datosOriginales.length, datosLimpios.length) >>
                mostrarEstadisticasFinales(datosLimpios) >>
                IO.println("\n[PASO 3/6] Generando scripts SQL...") >> {

                val scriptLimpio = MovieEtlToDb.generarSqlScript(datosLimpios)
                val scriptModelo = ModeloLogicoSql.generarDDL

                MovieEtlToDb.guardarSql(scriptLimpio, sqlOutputLimpio.toString) >>
                  MovieEtlToDb.guardarSql(scriptModelo, sqlOutputModeloLogico.toString) >>
                  IO.println(s"✓ SQL limpio generado en: $sqlOutputLimpio") >>
                  IO.println(s"✓ Modelo lógico (DDL) generado en: $sqlOutputModeloLogico\n")
              } >>
                IO.println("[PASO 4/6] Cargando tabla staging (movies_clean)...") >>
                MovieEtlToDb.cargarEnDb(
                  movies = datosLimpios,
                  batchSize = 1000,
                  recargarDesdeCero = true
                ) >>
                IO.println("\n[PASO 5/6] Creando modelo normalizado...") >>
                IO.println("[PASO 6/6] Cargando datos en modelo normalizado (esto puede tardar)...\n") >>
                ModeloNormalizadoEtl.cargarModeloNormalizado(datosLimpios).flatMap { _ =>
                  IO.println("\n╔═══════════════════════════════════════════════════════════════╗") >>
                    IO.println("║                     ✓ ETL COMPLETADO EXITOSAMENTE              ║") >>
                    IO.println("╚═══════════════════════════════════════════════════════════════╝") >>
                    IO.println(s"\n  Total de registros cargados: ${datosLimpios.length}") >>
                    IO.println(s"\n   TABLAS CARGADAS:") >>
                    IO.println(s"     • movies_clean (staging)") >>
                    IO.println(s"     • movies (normalizada)") >>
                    IO.println(s"     • collections, genres, production_companies") >>
                    IO.println(s"     • production_countries, spoken_languages") >>
                    IO.println(s"     • keywords, cast_members, crew_members") >>
                    IO.println(s"     • Todas las tablas puente (movie_*)") >>
                    IO.println(s"\n   ARCHIVOS SQL:") >>
                    IO.println(s"     • $sqlOutputLimpio") >>
                    IO.println(s"     • $sqlOutputModeloLogico") >>
                    IO.println("\n  Use la opción [6] del menú para consultar estadísticas.\n")
                }
            }
        }
      }.handleErrorWith { e =>
        IO.println(s"\nError durante el proceso ETL: ${e.getMessage}") >>
          IO.println(s"   Tipo: ${e.getClass.getSimpleName}") >>
          IO.unit
      }
  }
}