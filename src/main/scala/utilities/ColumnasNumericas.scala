import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import models.MovieNumeric
import models.Estadisticos
import models.Decoders.given  // ← AGREGAR ESTA LÍNEA

object ColumnasNumericas extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/pi-movies-complete-2026-01-08.csv")

  def imprimirMetricas(nombre: String, datos: List[Double]): IO[Unit] =
    val avg  = Estadisticos.promedio(datos)
    val sum  = Estadisticos.sumaTotal(datos)
    val std  = Estadisticos.desviacionEstandar(datos)
    IO.println(f"  > $nombre%-12s | Promedio: $avg%10.2f | Suma: $sum%15.2f | Desv. Est: $std%10.2f")

  val run: IO[Unit] =
    val lecturaCSV: IO[List[MovieNumeric]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieNumeric](';'))
      .compile
      .toList

    lecturaCSV.flatMap { peliculas =>
      IO.println("=" * 100) >>
        IO.println("              INFORME ACTUALIZADO DE MÉTRICAS") >>
        IO.println("=" * 100) >>
        imprimirMetricas("Budget", peliculas.map(_.budget)) >>
        imprimirMetricas("Revenue", peliculas.map(_.revenue)) >>
        imprimirMetricas("Popularity", peliculas.map(_.popularity)) >>
        imprimirMetricas("Runtime", peliculas.map(_.runtime)) >>
        imprimirMetricas("Vote Avg", peliculas.map(_.vote_average)) >>
        imprimirMetricas("Vote Count", peliculas.map(_.vote_count)) >>
        IO.println("=" * 100) >>
        IO.println(s"  Total registros procesados: ${peliculas.length}") >>
        IO.println("=" * 100)
    }