package etl

import cats.effect.{IO, IOApp}
import doobie.*
import doobie.implicits.*
import cats.implicits._
import config.Database

object ConsultaEstadisticas extends IOApp.Simple {

  case class ResumenGeneral(
                             total: Long,
                             presupuestoMin: Double,
                             presupuestoMax: Double,
                             presupuestoPromedio: Double,
                             ingresoMin: Double,
                             ingresoMax: Double,
                             ingresoPromedio: Double,
                             popularidadPromedio: Double
                           )

  case class PeliculaTop(
                          title: String,
                          budget: Double,
                          revenue: Double,
                          popularity: Double,
                          vote_average: Double,
                          roi: Double
                        )

  case class EstadisticasPorAnio(
                                  anio: Int,
                                  cantidad: Long,
                                  presupuestoPromedio: Double,
                                  ingresoPromedio: Double,
                                  roiPromedio: Double
                                )

  def obtenerResumenGeneral: ConnectionIO[Option[ResumenGeneral]] =
    sql"""
      SELECT 
        COUNT(*) as total,
        MIN(budget) as presupuesto_min,
        MAX(budget) as presupuesto_max,
        AVG(budget) as presupuesto_promedio,
        MIN(revenue) as ingreso_min,
        MAX(revenue) as ingreso_max,
        AVG(revenue) as ingreso_promedio,
        AVG(popularity) as popularidad_promedio
      FROM movies_clean
    """.query[ResumenGeneral].option

  def obtenerTop10PorIngreso: ConnectionIO[List[PeliculaTop]] =
    sql"""
      SELECT title, budget, revenue, popularity, vote_average, roi
      FROM movies_clean
      ORDER BY revenue DESC
      LIMIT 10
    """.query[PeliculaTop].to[List]

  def obtenerTop10PorROI: ConnectionIO[List[PeliculaTop]] =
    sql"""
      SELECT title, budget, revenue, popularity, vote_average, roi
      FROM movies_clean
      WHERE roi > 0
      ORDER BY roi DESC
      LIMIT 10
    """.query[PeliculaTop].to[List]

  def obtenerEstadisticasPorAnio: ConnectionIO[List[EstadisticasPorAnio]] =
    sql"""
      SELECT 
        release_year,
        COUNT(*) as cantidad,
        AVG(budget) as presupuesto_promedio,
        AVG(revenue) as ingreso_promedio,
        AVG(roi) as roi_promedio
      FROM movies_clean
      WHERE release_year BETWEEN 2000 AND 2025
      GROUP BY release_year
      ORDER BY release_year DESC
      LIMIT 15
    """.query[EstadisticasPorAnio].to[List]

  def mostrarResumenGeneral(resumen: ResumenGeneral): IO[Unit] =
    IO.println("=" * 90) >>
      IO.println("                    RESUMEN GENERAL DE DATOS EN MYSQL") >>
      IO.println("=" * 90) >>
      IO.println(f"\n  Total de películas:           ${resumen.total}%,10d") >>
      IO.println(f"\n  PRESUPUESTO (Budget):") >>
      IO.println(f"    Mínimo:                     $${resumen.presupuestoMin}%%.2f") >>
      IO.println(f"    Máximo:                     $${resumen.presupuestoMax}%%.2f") >>
      IO.println(f"    Promedio:                   $${resumen.presupuestoPromedio}%%.2f") >>
      IO.println(f"\n  INGRESOS (Revenue):") >>
      IO.println(f"    Mínimo:                     $${resumen.ingresoMin}%%.2f") >>
      IO.println(f"    Máximo:                     $${resumen.ingresoMax}%%.2f") >>
      IO.println(f"    Promedio:                   $${resumen.ingresoPromedio}%%.2f") >>
      IO.println(f"\n  POPULARIDAD:") >>
      IO.println(f"    Promedio:                   ${resumen.popularidadPromedio}%,.2f") >>
      IO.println("\n" + "=" * 90)

  def mostrarTop10Ingresos(peliculas: List[PeliculaTop]): IO[Unit] =
    IO.println("\n" + "=" * 90) >>
      IO.println("              TOP 10 PELÍCULAS POR INGRESOS (REVENUE)") >>
      IO.println("=" * 90) >>
      IO.println(f"${"#"}%-3s ${"Título"}%-40s ${"Ingreso"}%15s ${"ROI"}%10s") >>
      IO.println("-" * 90) >>
      peliculas.zipWithIndex.traverse_ { case (p, idx) =>
        val titulo = if (p.title.length > 40) p.title.take(37) + "..." else p.title
        IO.println(f"${idx + 1}%-3d ${titulo}%-40s $$${p.revenue}%,13.0f ${p.roi * 100}%9.1f%%")
      } >>
      IO.println("=" * 90)

  def mostrarTop10ROI(peliculas: List[PeliculaTop]): IO[Unit] =
    IO.println("\n" + "=" * 90) >>
      IO.println("         TOP 10 PELÍCULAS POR RETORNO DE INVERSIÓN (ROI)") >>
      IO.println("=" * 90) >>
      IO.println(f"${"#"}%-3s ${"Título"}%-35s ${"Presup."}%12s ${"Ingreso"}%15s ${"ROI"}%10s") >>
      IO.println("-" * 90) >>
      peliculas.zipWithIndex.traverse_ { case (p, idx) =>
        val titulo = if (p.title.length > 35) p.title.take(32) + "..." else p.title
        IO.println(f"${idx + 1}%-3d ${titulo}%-35s $$${p.budget}%,10.0f $$${p.revenue}%,13.0f ${p.roi * 100}%9.1f%%")
      } >>
      IO.println("=" * 90)

  def mostrarEstadisticasAnuales(stats: List[EstadisticasPorAnio]): IO[Unit] =
    IO.println("\n" + "=" * 90) >>
      IO.println("           ESTADÍSTICAS POR AÑO (Últimos 15 años)") >>
      IO.println("=" * 90) >>
      IO.println(f"${"Año"}%-6s ${"Películas"}%10s ${"Presup. Prom."}%15s ${"Ingreso Prom."}%18s ${"ROI Prom."}%12s") >>
      IO.println("-" * 90) >>
      stats.traverse_ { s =>
        IO.println(f"${s.anio}%-6d ${s.cantidad}%,10d $$${s.presupuestoPromedio}%,13.0f $$${s.ingresoPromedio}%,16.0f ${s.roiPromedio * 100}%11.1f%%")
      } >>
      IO.println("=" * 90)

  def run: IO[Unit] = {
    Database.init()
    Database.transactor.use { xa =>
      val programa = for {
        resumenOpt <- obtenerResumenGeneral
        top10Ingresos <- obtenerTop10PorIngreso
        top10ROI <- obtenerTop10PorROI
        statsAnuales <- obtenerEstadisticasPorAnio
      } yield (resumenOpt, top10Ingresos, top10ROI, statsAnuales)

      programa.transact(xa).flatMap { case (resumenOpt, top10Ing, top10Roi, statsAnio) =>
        resumenOpt match {
          case Some(resumen) if resumen.total > 0 =>
            IO.println("\n╔═══════════════════════════════════════════════════════════════╗") >>
              IO.println("║              ESTADÍSTICAS DE DATOS EN MYSQL                   ║") >>
              IO.println("╚═══════════════════════════════════════════════════════════════╝\n") >>
              mostrarResumenGeneral(resumen) >>
              mostrarTop10Ingresos(top10Ing) >>
              mostrarTop10ROI(top10Roi) >>
              mostrarEstadisticasAnuales(statsAnio) >>
              IO.println("\n✓ Consulta completada exitosamente.\n")

          case Some(resumen) =>
            IO.println("\n⚠️  La tabla 'movies_clean' existe pero está vacía.") >>
              IO.println("   Ejecute la opción [5] para cargar los datos.\n")

          case None =>
            IO.println("\n❌ No se pudo consultar la tabla 'movies_clean'.") >>
              IO.println("   Verifique que la tabla existe y contiene datos.\n")
        }
      }
    }.handleErrorWith { e =>
      IO.println(s"\n❌ Error al consultar la base de datos: ${e.getMessage}") >>
        IO.println("   Verifique la configuración de conexión a MySQL.\n") >>
        IO.unit
    }
  }
}