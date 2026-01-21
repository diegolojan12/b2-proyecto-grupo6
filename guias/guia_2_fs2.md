# Guía de fs2-data para trabajar con CSV en Scala

## Introducción

**fs2-data** es una librería de Scala que proporciona herramientas para procesar diversos formatos de datos (CSV, JSON, XML) de manera funcional y orientada a streams usando **fs2**. Esta guía se enfoca en el módulo `fs2-data-csv`.

---

## Configuración del Proyecto

### Dependencias SBT

```scala
libraryDependencies ++= Seq(
  "org.gnieh" %% "fs2-data-csv" % "1.11.1",
  "org.gnieh" %% "fs2-data-csv-generic" % "1.11.1", // Para derivación automática
  "co.fs2" %% "fs2-core" % "3.12.2",
  "co.fs2" %% "fs2-io" % "3.12.2"
)
```

### Imports Básicos

```scala
import fs2._
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._
import cats.effect._
```

---

## Conceptos Clave

### Row vs CsvRow

- **Row**: Representa una fila como una secuencia de campos (sin headers).
- **CsvRow[Header]**: Representa una fila con acceso por nombre de columna.

### Type Classes Importantes

- **CellDecoder[A]**: Decodifica una celda individual a tipo `A`.
- **CellEncoder[A]**: Codifica un tipo `A` a una celda.
- **CsvRowDecoder[A, Header]**: Decodifica una fila completa a tipo `A`.
- **CsvRowEncoder[A, Header]**: Codifica un tipo `A` a una fila CSV.

---

## Lectura de Archivos CSV

### Lectura Simple (sin headers)

```scala
import fs2.io.file.{Files, Path}

val programa: IO[Unit] = 
  Files[IO]
    .readUtf8(Path("datos.csv"))
    .through(decodeUsingHeaders[String]())
    .evalMap(row => IO.println(row))
    .compile
    .drain
```

### Lectura con Case Class

```scala
case class Persona(nombre: String, edad: Int, ciudad: String)

// Derivación automática del decoder
implicit val personaDecoder: CsvRowDecoder[Persona, String] = deriveCsvRowDecoder

val leerPersonas: IO[List[Persona]] =
  Files[IO]
    .readUtf8(Path("personas.csv"))
    .through(decodeUsingHeaders[Persona]())
    .compile
    .toList
```

### Configuración del Parser

```scala
// Configurar separador, quote character, etc.
val config = CsvConfig(
  separator = ';',           // Separador de campos
  quoteCharacter = '"',      // Carácter de quote
  escapeCharacter = '\\'     // Carácter de escape
)

Files[IO]
  .readUtf8(Path("datos.csv"))
  .through(rows[IO, Char](config))
  .through(headers[IO, String])
  .through(decode[IO, Persona])
```

---

## Escritura de Archivos CSV

### Escritura Simple

```scala
case class Producto(id: Int, nombre: String, precio: Double)

implicit val productoEncoder: CsvRowEncoder[Producto, String] = deriveCsvRowEncoder

val productos = List(
  Producto(1, "Laptop", 999.99),
  Producto(2, "Mouse", 29.99),
  Producto(3, "Teclado", 79.99)
)

val escribir: IO[Unit] =
  Stream
    .emits(productos)
    .through(encodeUsingFirstHeaders(fullRows = true))
    .through(fs2.text.utf8.encode)
    .through(Files[IO].writeAll(Path("productos.csv")))
    .compile
    .drain
```

### Escritura con Headers Personalizados

```scala
val headersPersonalizados = NonEmptyList.of("ID", "Nombre del Producto", "Precio USD")

Stream
  .emits(productos)
  .through(encodeWithGivenHeaders(headers = headersPersonalizados))
  .through(fs2.text.utf8.encode)
  .through(Files[IO].writeAll(Path("productos.csv")))
```

---

## Decoders y Encoders Personalizados

### CellDecoder Personalizado

```scala
import java.time.LocalDate
import java.time.format.DateTimeFormatter

implicit val localDateDecoder: CellDecoder[LocalDate] = 
  CellDecoder.instance { cell =>
    Either.catchNonFatal {
      LocalDate.parse(cell, DateTimeFormatter.ISO_LOCAL_DATE)
    }.leftMap(e => DecoderError(s"Error parseando fecha: ${e.getMessage}"))
  }
```

### CellEncoder Personalizado

```scala
implicit val localDateEncoder: CellEncoder[LocalDate] =
  CellEncoder.instance(_.format(DateTimeFormatter.ISO_LOCAL_DATE))
```

### CsvRowDecoder Manual

```scala
case class Empleado(id: Int, nombre: String, salario: BigDecimal)

implicit val empleadoDecoder: CsvRowDecoder[Empleado, String] =
  CsvRowDecoder.instance { row =>
    for {
      id <- row.as[Int]("id")
      nombre <- row.as[String]("nombre")
      salario <- row.as[BigDecimal]("salario")
    } yield Empleado(id, nombre, salario)
  }
```

---

## Manejo de Errores

### Capturar Errores de Decodificación

```scala
val procesarConErrores: IO[Unit] =
  Files[IO]
    .readUtf8(Path("datos.csv"))
    .through(decodeUsingHeaders[Persona]())
    .attempt
    .evalMap {
      case Right(persona) => IO.println(s"OK: $persona")
      case Left(error)    => IO.println(s"Error: ${error.getMessage}")
    }
    .compile
    .drain
```

### Usar `decodeGivenHeaders` con Validación

```scala
val procesarSeguro: IO[List[Either[Throwable, Persona]]] =
  Files[IO]
    .readUtf8(Path("datos.csv"))
    .through(lowlevel.rows[IO, Char]())
    .through(lowlevel.headers[IO, String])
    .map(row => row.to[Persona])
    .compile
    .toList
```

---

## Transformaciones de Datos

### Filtrar Filas

```scala
Files[IO]
  .readUtf8(Path("personas.csv"))
  .through(decodeUsingHeaders[Persona]())
  .filter(_.edad >= 18)
  .compile
  .toList
```

### Transformar y Escribir

```scala
case class PersonaInput(nombre: String, edad: Int)
case class PersonaOutput(nombre: String, edad: Int, esAdulto: Boolean)

implicit val inputDecoder: CsvRowDecoder[PersonaInput, String] = deriveCsvRowDecoder
implicit val outputEncoder: CsvRowEncoder[PersonaOutput, String] = deriveCsvRowEncoder

val transformar: IO[Unit] =
  Files[IO]
    .readUtf8(Path("entrada.csv"))
    .through(decodeUsingHeaders[PersonaInput]())
    .map(p => PersonaOutput(p.nombre, p.edad, p.edad >= 18))
    .through(encodeUsingFirstHeaders(fullRows = true))
    .through(fs2.text.utf8.encode)
    .through(Files[IO].writeAll(Path("salida.csv")))
    .compile
    .drain
```

---

## Ejemplo Completo

```scala
import cats.effect._
import fs2._
import fs2.io.file._
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._

object CsvApp extends IOApp.Simple {

  case class Venta(
    id: Int,
    producto: String,
    cantidad: Int,
    precioUnitario: Double
  )

  case class VentaResumen(
    producto: String,
    total: Double
  )

  implicit val ventaDecoder: CsvRowDecoder[Venta, String] = deriveCsvRowDecoder
  implicit val resumenEncoder: CsvRowEncoder[VentaResumen, String] = deriveCsvRowEncoder

  def run: IO[Unit] = {
    Files[IO]
      .readUtf8(Path("ventas.csv"))
      .through(decodeUsingHeaders[Venta]())
      .map(v => VentaResumen(v.producto, v.cantidad * v.precioUnitario))
      .through(encodeUsingFirstHeaders(fullRows = true))
      .through(fs2.text.utf8.encode)
      .through(Files[IO].writeAll(Path("resumen.csv")))
      .compile
      .drain
      .flatMap(_ => IO.println("Procesamiento completado!"))
  }
}
```

---

## Tips y Buenas Prácticas

1. **Usa derivación automática** cuando sea posible con `deriveCsvRowDecoder` y `deriveCsvRowEncoder`.

2. **Maneja campos opcionales** usando `Option[T]` en tus case classes.

3. **Procesa archivos grandes** sin problemas gracias a la naturaleza de streaming de fs2.

4. **Configura el separador** según tu región (`;` es común en países hispanohablantes).

5. **Valida datos temprano** usando `.attempt` o `.handleErrorWith`.

---

## Recursos Adicionales

- [Documentación oficial de fs2-data](https://fs2-data.gnieh.org/)
- [Repositorio GitHub](https://github.com/gnieh/fs2-data)
- [Documentación de fs2](https://fs2.io/)
