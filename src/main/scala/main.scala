import cats.effect.{IO, IOApp}
import scala.io.StdIn

object Main extends IOApp.Simple {

  def mostrarMenu(): IO[Unit] =
    IO.println(
      """
        |╔═══════════════════════════════════════════════════════════════╗
        |║        PROYECTO INTEGRADOR - ANÁLISIS DE PELÍCULAS            ║
        |╚═══════════════════════════════════════════════════════════════╝
        |
        |  Seleccione una opción:
        |
        |  [1] Análisis de Columnas Numéricas
        |      → Calcula promedio, suma y desviación estándar
        |
        |  [2] Análisis de Distribución de Texto
        |      → Top frecuencias en columnas categóricas
        |
        |  [3] Limpieza de Datos y Detección de Outliers
        |      → Pipeline completo de preprocesamiento
        |
        |  [4] Procesamiento ETL de Crew (JSON)
        |      → Limpieza y transformación de datos crew
        |
        |  [5] ETL Completo: CSV → Limpieza → SQL → MySQL
        |      → Genera SQL limpio + modelo lógico y carga en MySQL
        |
        |  [6] Ver Estadísticas de Datos Cargados en MySQL
        |      → Consulta datos desde la base de datos
        |
        |  [7] Diagnóstico ETL (Verificar qué tablas tienen datos)
        |      → Detecta problemas en el proceso de carga
        |
        |  [0] Salir
        |
        |══════════════════════════════════════════════════════════════════
        |""".stripMargin
    )

  def leerOpcion(): IO[String] =
    IO.print("  Ingrese su opción: ") >> IO(StdIn.readLine())

  def ejecutarOpcion(opcion: String): IO[Boolean] = opcion.trim match {

    case "1" =>
      IO.println("\n[EJECUTANDO] Análisis de Columnas Numéricas...\n") >>
        ColumnasNumericas.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "2" =>
      IO.println("\n[EJECUTANDO] Análisis de Distribución de Texto...\n") >>
        apps.AnalisisTexto.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "3" =>
      IO.println("\n[EJECUTANDO] Limpieza de Datos y Detección de Outliers...\n") >>
        utilities.LimpiezaDatos.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "4" =>
      IO.println("\n[EJECUTANDO] Procesamiento ETL de Crew (JSON)...\n") >>
        IO {
          data.LeerMoviesCSV.main(Array.empty)
        } >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "5" =>
      IO.println(
        """
          |[EJECUTANDO] ETL Completo
          |
          | → Lee el CSV original
          | → Aplica limpieza de datos
          | → Genera:
          |     - movies_clean.sql (SQL limpio)
          |     - modelo_logico_ddl.sql (modelo lógico)
          | → Carga los datos en MySQL
          |""".stripMargin
      ) >>
        etl.EtlCompleto.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "6" =>
      IO.println("\n[EJECUTANDO] Consulta de Estadísticas en MySQL...\n") >>
        etl.ConsultaEstadisticas.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "7" =>
      IO.println("\n[EJECUTANDO] Diagnóstico ETL...\n") >>
        etl.DiagnosticoETL.run >>
        IO.println("\n[COMPLETADO] Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)

    case "0" =>
      IO.println("\n✓ Saliendo del programa. ¡Hasta pronto!\n") >>
        IO.pure(false)

    case _ =>
      IO.println("\n✗ Opción inválida. Por favor intente nuevamente.\n") >>
        IO.println("Presione ENTER para continuar...") >>
        IO(StdIn.readLine()) >>
        IO.pure(true)
  }

  def menuLoop(): IO[Unit] =
    mostrarMenu() >>
      leerOpcion().flatMap { opcion =>
        ejecutarOpcion(opcion).flatMap { continuar =>
          if (continuar) menuLoop() else IO.unit
        }
      }

  def run: IO[Unit] =
    IO.println("") >>
      menuLoop() >>
      IO.println("")
}