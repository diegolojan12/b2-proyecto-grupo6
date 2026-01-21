package apps

import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import models.MovieText
import models.EstadisticosTexto
import models.Decoders.given  // ← AGREGAR ESTA LÍNEA

object AnalisisTexto extends IOApp.Simple:
  val filePath = Path("src/main/resources/data/pi-movies-complete-2026-01-08.csv")

  def imprimirFrecuencias(nombreColumna: String, datos: List[String]): IO[Unit] =
    val topFrecuencias = EstadisticosTexto.distribucionFrecuencia(datos)
    IO.println(f"\n--- Top Frecuencias: $nombreColumna ---") >>
      IO.realTime.flatMap { _ =>
        topFrecuencias.zipWithIndex.map { case ((valor, cuenta), i) =>
          IO.println(f"  ${i + 1}. $valor%-30s | Apariciones: $cuenta")
        }.sequence.void
      }

  val run: IO[Unit] =
    val lecturaCSV: IO[List[MovieText]] = Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieText](';'))
      .compile
      .toList

    lecturaCSV.flatMap { peliculas =>
      IO.println("=" * 80) >>
        IO.println("           ANÁLISIS DE DISTRIBUCIÓN DE FRECUENCIA (TEXTO)") >>
        IO.println("=" * 80) >>
        imprimirFrecuencias("Idioma Original", peliculas.map(_.original_language)) >>
        imprimirFrecuencias("Estado (Status)", peliculas.map(_.status)) >>
        imprimirFrecuencias("Colección", peliculas.map(_.belongs_to_collection)) >>
        IO.println("\n" + "=" * 80) >>
        IO.println(s"  Total registros analizados: ${peliculas.length}") >>
        IO.println("=" * 80)
    }