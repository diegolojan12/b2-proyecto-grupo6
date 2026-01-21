# ProyectoIntegrador_PFR

# Proyecto Integrador: Sistema de Análisis de Datos de Películas

## Descripción General

Sistema integral de procesamiento, limpieza y análisis de datos de películas desarrollado en **Scala 3** utilizando programación funcional y procesamiento de streams reactivos. El proyecto implementa un pipeline ETL completo que transforma datos crudos de películas (CSV con columnas JSON anidadas) en información estructurada lista para análisis estadístico.

### Características Principales

- ✅ Procesamiento de datasets masivos (45,000+ películas)
- ✅ Parsing robusto de CSV con columnas JSON embebidas
- ✅ Limpieza automática de datos con detección de outliers
- ✅ Análisis estadístico descriptivo avanzado
- ✅ Transformación de sintaxis Python a JSON estándar
- ✅ Reportes detallados de calidad de datos
- ✅ Procesamiento streaming con manejo eficiente de memoria

---

## Arquitectura del Proyecto

```
ProyectoIntegrador_PFR/
├── src/main/
│   ├── scala/
│   │   ├── apps/
│   │   │   ├── ColumnasNumericas.scala    # Análisis estadístico numérico
│   │   │   └── AnalisisTexto.scala        # Análisis de frecuencias de texto
│   │   ├── models/
│   │   │   ├── Movie.scala                # Modelo principal con campos calculados
│   │   │   ├── MovieRaw.scala             # Modelo de datos crudos (24 cols)
│   │   │   ├── MovieNumeric.scala         # Vista solo columnas numéricas
│   │   │   ├── MovieText.scala            # Vista solo columnas de texto
│   │   │   ├── CrewMember.scala           # Modelo para miembros del equipo
│   │   │   ├── models.scala               # Todos los case classes auxiliares
│   │   │   ├── Decoders.scala             # Decodificadores CSV automáticos
│   │   │   ├── Estadisticos.scala         # Funciones estadísticas puras
│   │   │   ├── EstadisticosTexto.scala    # Análisis de distribución
│   │   │   ├── Limpiador.scala            # Pipeline de limpieza
│   │   │   ├── DetectorOutliers.scala     # Algoritmos IQR y Z-Score
│   │   │   ├── AnalizadorCalidad.scala    # Métricas de calidad
│   │   │   └── Transformador.scala        # Transformaciones de datos
│   │   ├── data/
│   │   │   └── LeerMoviesCSV.scala        # Procesador principal de CSV
│   │   └── utilities/
│   │       └── LimpiezaDatos.scala        # Reporte completo de limpieza
│   └── resources/data/
│       ├── pi-movies-complete-2026-01-08.csv
│       └── pi-movies-complete-2026-01-08-limpio.csv
├── build.sbt
└── README.md
```

---

## Stack Tecnológico

### Lenguaje y Runtime
- **Scala 3.3.1**: Lenguaje principal con soporte completo para programación funcional
- **JDK 11+**: Máquina virtual Java

### Librerías Core (Typelevel Ecosystem)

| Librería | Versión | Propósito |
|----------|---------|-----------|
| **Cats Effect** | 3.5.2 | Manejo de efectos secundarios e IO puro |
| **FS2** | 3.9.3 | Streaming funcional y procesamiento lazy |
| **FS2 Data CSV** | 1.9.1 | Decodificación tipada de archivos CSV |
| **Circe** | 0.14.6 | Serialización/Deserialización JSON |

---

## Configuración del Proyecto

### build.sbt

```scala
name := "ProyectoIntegrador_PFR"
version := "0.1.0"
scalaVersion := "3.3.1"

val circeVersion = "0.14.6"
val fs2Version = "3.9.3"
val catsEffectVersion = "3.5.2"
val fs2DataVersion = "1.9.1"

libraryDependencies ++= Seq(
  // Cats Effect - IO y efectos puros
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  
  // FS2 - Streaming funcional
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io" % fs2Version,
  
  // FS2 Data - Procesamiento CSV
  "org.gnieh" %% "fs2-data-csv" % fs2DataVersion,
  "org.gnieh" %% "fs2-data-csv-generic" % fs2DataVersion,
  
  // Circe - Procesamiento JSON
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation"
)
```

### Prerrequisitos

```bash
# Verificar versión de Java (requiere 11+)
java -version

# Verificar versión de SBT (requiere 1.9+)
sbt --version

# Verificar codificación del archivo CSV (debe ser UTF-8)
file -I src/main/resources/data/pi-movies-complete-2026-01-08.csv
```

---

## Guía de Ejecución

### Instalación

```bash
# Clonar el repositorio
git clone https://github.com/PFR-Computacion-O25F26/b2-proyecto-integrador-a-b2_proyecto_integrador_a_grupo_6.git
cd b2-proyecto-integrador-a-b2_proyecto_integrador_a_grupo_6

# Descargar dependencias y compilar
sbt compile
```

# Tablas de datos (nombre de columna, tipo, propósito y observaciones) - README.md

## Diccionario de Datos


| Nombre de Columna | Tipo de Dato | Propósito | Observaciones |
| --- | --- | --- | --- |
| **`adult`** | booleano | Indica si la película es contenido para adultos. |  |
| **`belongs_to_colection`** | json | Información sobre la franquicia o colección a la que pertenece la película. | **Campos:** `id`, `name`, `poster_path`, `backdrop_path`. |
| **`budget`** | double | El presupuesto de producción de la película. | |
| **`genres`** | double | Lista de los géneros de la película. | **Campos:** `id`, `name`. | 
| **`homepage`** | string | La URL del sitio web oficial de la película. |  |
| **`id`** | int | El identificador único de la película. | Probablemente coincide con el ID de TMDB. |
| **`imdb_id`** | string | El identificador de la película en IMDB. |  |
| **`original_language`** | string | El código ISO del idioma original de la película. |  |
| **`original_title`** | string | El título original de la película. |  |
| **`overview`** | string | Una sinopsis o resumen de la trama. |  |
| **`popularity`** | double | Un índice de popularidad de la película. |  |
| **`poster_path`** | string | La ruta o URL relativa de la imagen del póster. |  |
| **`production_companies`** | json | Lista de compañías productoras. | **Campos:** `id`, `name`. |
| **`production_countries`** | json | Países donde se produjo la película. | **Campos:** `iso_3166_1`, `name`. |
| **`relase_date`** | date | La fecha de estreno de la película. | *(Nota: El nombre de la columna tiene un error tipográfico, usualmente es 'release_date')*. |
| **`revenue`** | double | Los ingresos o recaudación en taquilla. |  |
| **`runtime`** | double | El estado de la película. | *(Nota: La descripción en el origen parece errónea; 'runtime' suele referirse a la duración en minutos)*. |
| **`spoken_languages`** | json | Idiomas hablados en la película. | **Campos:** `iso_639_1`, `name`. |
| **`status`** | string | El estado de la película (ej. Released). |  |
| **`tagline`** | string | La frase promocional o eslogan de la película. |  |
| **`title`** | string | El título de la película. | Generalmente en inglés o el idioma principal del dataset. |
| **`video`** | booleano | Indica si existe un video asociado. | Raramente usado, usualmente `False`. |
| **`vote_avarage`** | double | El promedio de las calificaciones de los usuarios. | *(Nota: Posible error tipográfico en 'avarage', usualmente es 'average')*. |
| **`vote_count`** | int | El número total de votos recibidos. |  |
| **`keywords`** | json | Palabras clave que describen temas de la película. | **Campos:** `id`, `name`. |
| **`cast`** | json | Lista del elenco (actores). | **Campos:** `cast_id`, `character`, `credit_id`, `gender` (1=M, 2=H, 0=Desc), `id`, `name`, `order`, `profile_path`. |
| **`crew`** | json | Lista del equipo de producción. | **Campos:** `credit_id`, `department`, `gender`, `id`, `job`, `name`, `profile_path`. |
| **`ratings`** | json | Lista de calificaciones dadas por usuarios específicos. | **Campos:** `userId`, `rating`, `timestamp`. |

---


# Lectura de columnas numéricas

Aquí tienes una propuesta de **README.md** profesional y detallado para tu proyecto.

Este documento explica qué hace el código, qué tecnologías utiliza y cómo está estructurado.

---

# Análisis Estadístico de Datos de Películas (Scala + FS2)

Este proyecto es una aplicación de consola escrita en **Scala 3** que procesa un archivo CSV masivo de películas para calcular y presentar métricas estadísticas descriptivas (Promedio, Suma Total y Desviación Estándar) sobre diversas variables numéricas.

## Descripción

El script `columnasNumericas.scala` utiliza programación funcional reactiva para leer un dataset de películas, decodificar la información y generar un reporte financiero y de popularidad.

El programa analiza específicamente las siguientes columnas:

* **Budget** (Presupuesto)
* **Revenue** (Ingresos)
* **Popularity** (Popularidad)
* **Runtime** (Duración)
* **Vote Average** (Promedio de votos)
* **Vote Count** (Conteo de votos)

## Tecnologías Utilizadas

El proyecto hace uso del ecosistema Typelevel para un manejo eficiente de efectos y flujos de datos:

* **Scala 3**: Lenguaje base.
* **Cats Effect**: Para el manejo de IO y efectos secundarios puros (`IOApp`).
* **FS2 (Functional Streams for Scala)**: Para la lectura y manipulación de archivos mediante streams.
* **FS2 Data CSV**: Para el parsing (decodificación) de archivos CSV de manera tipada.

## Estructura del Código

El código se divide en tres componentes principales:

### 1. Modelo de Datos (`case class Movie`)

Define la estructura de los datos que se esperan del CSV. Todos los campos se manejan como `Double` para facilitar los cálculos matemáticos.

```scala
case class Movie(id: Double, budget: Double, ...)

```

* **Nota:** Se utiliza un derivador automático (`deriveCsvRowDecoder`) para mapear las columnas del CSV a esta clase.

### 2. Módulo Estadístico (`object Estadisticos`)

Contiene funciones puras para el cálculo de métricas sobre listas de números:

* `promedio`: Calcula la media aritmética.
* `sumaTotal`: Calcula la sumatoria de los valores.
* `desviacionEstandar`: Calcula la variabilidad de los datos respecto a la media.

### 3. Aplicación Principal (`object columnasNumericas`)

Hereda de `IOApp.Simple` y orquesta el flujo de ejecución:

1. **Lectura:** Carga el archivo desde `src/main/resources/data/`.
2. **Decodificación:** Interpreta el CSV utilizando `;` (punto y coma) como separador.
3. **Procesamiento:** Recopila los datos en una lista y aplica las funciones estadísticas a cada columna de interés.
4. **Reporte:** Imprime en consola una tabla formateada con los resultados.

## Requisitos de Ejecución

Para que el programa funcione correctamente, se debe cumplir lo siguiente:

1. **Archivo de Datos:** Debe existir un archivo CSV en la ruta:
`src/main/resources/data/pi-movies-complete-2025-12-04.csv`
2. **Formato CSV:** El archivo debe usar **punto y coma (;)** como separador de columnas, no comas.

## Ejemplo de Salida (Output)

Al ejecutar el programa, verás un reporte similar a este en la consola:

```text
====================================================================================================
              INFORME ACTUALIZADO DE MÉTRICAS
====================================================================================================
  > Budget       | Promedio: 4500000.00 | Suma: 9000000000.00 | Desv. Est: 150000.50
  > Revenue      | Promedio: 12000000.00| Suma: 24000000000.00| Desv. Est: 500000.20
  > Popularity   | Promedio:      15.50 | Suma:      31000.00 | Desv. Est:      5.20
  ...
====================================================================================================
  Total registros procesados: 2000
====================================================================================================

```

## Notas de Implementación

* **Manejo de Memoria:** Actualmente, el script utiliza `.compile.toList`, lo que carga todos los registros en memoria antes de calcular. Para datasets extremadamente grandes (Big Data), se recomendaría refactorizar para realizar los cálculos en el *stream* (pasada única) sin cargar la lista completa.

---

# Análisis de datos en columnas tipo texto (algunas col. - distribución de frecuencia). OJO: no considerar columnas en formato JSON

# Análisis de Frecuencia de Texto en Datos de Películas (Scala + FS2)

Este proyecto es una aplicación de consola escrita en **Scala 3** diseñada para analizar variables categóricas (texto) dentro de un dataset masivo de películas. Su objetivo principal es calcular la **distribución de frecuencia** y mostrar los valores más comunes ("Top N") para campos específicos.

## Descripción

El script `AnalisisTexto.scala` complementa el análisis numérico enfocándose en la calidad y repetición de datos textuales. Utiliza programación funcional para leer, filtrar y agrupar datos de texto para identificar patrones comunes.

El programa analiza las siguientes columnas de texto:

* **Original Language** (Idioma original): Para identificar los idiomas predominantes.
* **Status** (Estado): Para ver cuántas películas están "Released", "Rumored", etc.
* **Belongs to Collection** (Colección): Para encontrar las franquicias o sagas más frecuentes.

## Tecnologías Utilizadas

El stack tecnológico es idéntico al módulo numérico, aprovechando el ecosistema Typelevel:

* **Scala 3**: Lenguaje base.
* **Cats Effect**: Para el control de flujo y efectos (`IOApp`, `IO.println`).
* **FS2 (Functional Streams for Scala)**: Para la carga eficiente de datos.
* **FS2 Data CSV**: Para el parsing tipado del archivo CSV.

## Estructura del Código

El código se organiza en tres componentes lógicos:

### 1. Modelo de Datos (`case class MovieText`)

Define una estructura que mapea exclusivamente las columnas de texto necesarias del CSV.

```scala
case class MovieText(
  belongs_to_collection: String,
  original_language: String,
  status: String,
  ...
)

```

* **Nota:** Al igual que en el módulo numérico, se usa `deriveCsvRowDecoder` para la decodificación automática.

### 2. Módulo Estadístico (`object EstadisticosTexto`)

Contiene la lógica central de análisis de frecuencia (`distribucionFrecuencia`):

1. **Filtrado:** Elimina celdas vacías o espacios en blanco (`filterNot(_.trim.isEmpty)`).
2. **Agrupación:** Agrupa los datos por identidad (`groupBy`).
3. **Conteo:** Calcula cuántas veces aparece cada término.
4. **Ordenamiento:** Ordena de mayor a menor frecuencia.
5. **Top N:** Retorna solo los 5 resultados más comunes (configurable).

### 3. Aplicación Principal (`object AnalisisTexto`)

Orquesta la ejecución del programa:

1. **Lectura y Decodificación:** Lee el archivo `pi-movies-complete-2025-12-04.csv` usando `;` como separador.
2. **Recolección:** Compila el stream en una lista en memoria.
3. **Reporte:** Llama a `imprimirFrecuencias` para cada columna de interés, formateando la salida para mostrar el valor y su conteo de apariciones.

## Requisitos de Ejecución

Para ejecutar este script, asegúrate de cumplir con lo siguiente:

1. **Archivo de Datos:** El archivo debe estar en:
`src/main/resources/data/pi-movies-complete-2025-12-04.csv`
2. **Formato CSV:** El archivo debe usar **punto y coma (;)** como separador.

## Ejemplo de Salida (Output)

Al ejecutar el programa, obtendrás un desglose de las categorías más populares:

```text
================================================================================
           ANÁLISIS DE DISTRIBUCIÓN DE FRECUENCIA (TEXTO)
================================================================================

--- Top Frecuencias: Idioma Original ---
  1. en                             | Apariciones: 1450
  2. fr                             | Apariciones: 120
  3. es                             | Apariciones: 85
  4. it                             | Apariciones: 40
  5. de                             | Apariciones: 35

--- Top Frecuencias: Estado (Status) ---
  1. Released                       | Apariciones: 1980
  2. Post Production                | Apariciones: 15
  3. Rumored                        | Apariciones: 5

--- Top Frecuencias: Colección ---
  1. James Bond Collection          | Apariciones: 26
  2. Star Wars Collection           | Apariciones: 9
  3. Harry Potter Collection        | Apariciones: 8
  ...

================================================================================
  Total registros analizados: 2000
================================================================================

```

## Notas de Implementación

* **Limpieza de Datos:** A diferencia del análisis numérico, este script incluye un paso explícito de limpieza para ignorar cadenas vacías, lo cual es crucial para evitar que un campo "vacío" aparezca como el valor más frecuente en columnas opcionales como "Colección".
* **Rendimiento:** Al igual que el módulo anterior, carga los datos en memoria (`toList`). Si el archivo crece a varios gigabytes, se recomienda cambiar la lógica de `groupBy` para usar una estructura de acumulación en streaming (como `fs2.fold`).

---

# Limpieza de Datos y Detección de Outliers (Scala + FS2)

Este módulo (`limpieza.scala`) es el encargado de la etapa de **Preprocesamiento y Calidad de Datos**. Su objetivo es depurar el dataset crudo antes del análisis final, eliminando registros inconsistentes y suavizando las métricas mediante la remoción de valores atípicos (outliers).

## Descripción

El script implementa reglas de negocio y algoritmos estadísticos para asegurar la fiabilidad del análisis. Aborda dos problemas comunes en datasets de películas:

1. **Datos Faltantes (Zeros):** Películas que tienen presupuesto, ingresos o duración registrados como `0.0`.
2. **Valores Atípicos (Outliers):** Películas con presupuestos extremadamente altos o bajos que distorsionan el promedio general.

Este proceso corresponde a la **Sección 5.5** (Reporte de Limpieza) del informe del proyecto.

## Metodología de Limpieza

El objeto `Limpiador` aplica las siguientes estrategias:

### 1. Filtrado de Valores Nulos/Ceros (`quitarValoresVacios`)

En este dataset, la ausencia de información numérica se representa con `0`. El script elimina cualquier registro que no cumpla con **todas** las siguientes condiciones simultáneamente:

* `Budget > 0`
* `Revenue > 0`
* `Runtime > 0`

### 2. Detección de Outliers - Método IQR (`filtrarOutliers`)

Para eliminar valores extremos (como superproducciones inusuales que sesgan la media), se utiliza el método del **Rango Intercuartílico (IQR)**:

1. Se calculan el primer cuartil (, 25%) y el tercer cuartil (, 75%).
2. Se obtiene el .
3. Se definen los límites estadísticos:
* **Límite Inferior:** 
* **Límite Superior:** 


4. Se descarta cualquier valor que caiga fuera de este rango.

## Tecnologías Utilizadas

* **Scala 3**: Lógica de filtrado funcional.
* **FS2 & Cats Effect**: Lectura de archivos y manejo de efectos.
* **Estadística Descriptiva**: Implementación manual del algoritmo de cuartiles y media aritmética.

## Estructura del Código

* **`object Limpiador`**: Contiene las funciones puras de transformación de listas. Es el núcleo lógico del saneamiento de datos.
* **`object Estadisticos`**: Utilidad simple para calcular promedios y validar cómo cambia la media antes y después de la limpieza.
* **`object AnalisisLimpieza`**:
1. Carga el CSV completo.
2. Genera un dataset "Limpio" sin ceros.
3. Aplica el filtro de outliers específicamente a la columna **Budget** para demostrar la efectividad del algoritmo.
4. Imprime un reporte comparativo.



## Ejemplo de Salida (Output)

Al ejecutar el script, se obtiene un informe que contrasta los datos crudos con los procesados:

```text
================================================================================
              REPORTE DE LIMPIEZA DE DATOS (Sección 5.5)
================================================================================
Registros iniciales:      2000
Registros tras quitar 0s: 1450
--------------------------------------------------------------------------------
ANÁLISIS DE OUTLIERS (Columna Budget):
  > Promedio ORIGINAL:   15500000.00
  > Promedio LIMPIO:      8400000.00
  > Outliers eliminados: 650
--------------------------------------------------------------------------------
Estado de limpieza: Completado.
================================================================================

```

*Nota: Se observa cómo el promedio disminuye significativamente al eliminar las superproducciones que actuaban como ruido estadístico.*

## Notas Importantes

* **Orden de Ejecución:** El script primero elimina los registros con ceros y *luego* calcula los outliers sobre los datos restantes. Esto evita que los ceros (que son datos erróneos, no valores reales bajos) afecten el cálculo de los cuartiles ( y ).
* **Personalización:** Actualmente, el reporte en consola se enfoca en la columna `Budget`, pero la función `filtrarOutliers` es genérica y puede aplicarse a `Revenue` o `Vote Count` fácilmente.

---


# Entregable 2

---

# Guía de Inicio: Circe para Scala

Esta guía proporciona una introducción práctica a **Circe**, una librería de serialización/deserialización de JSON para Scala basada en [Cats](https://typelevel.org/cats/).

## 1. Configuración del Proyecto

Para empezar, añade las siguientes dependencias a tu archivo `build.sbt`. Utilizaremos la derivación automática para facilitar el aprendizaje.

```scala
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core"    % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser"  % circeVersion
)

```

---

## 2. Conceptos Básicos

Circe utiliza tres componentes principales para transformar datos:

1. **Parser:** Convierte un `String` en una estructura `Json`.
2. **Decoder:** Convierte un objeto `Json` en una instancia de una clase de Scala (como un `case class`).
3. **Encoder:** Convierte una instancia de Scala en un objeto `Json`.

---

## 3. Ejemplo Práctico: Deserialización Básica

Imagina que tenemos un JSON pequeño que representa a un usuario.

### El Modelo

Primero, definimos nuestro `case class` e importamos la derivación automática.

```scala
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

case class User(id: Int, name: String, emails: List[String])

```

### El Código

```scala
object Main extends App {
  val jsonString = """
    {
      "id": 1,
      "name": "Juan Perez",
      "emails": ["juan@example.com", "jperez@work.com"]
    }
  """

  // 1. Parsear el String a un objeto de nuestro modelo
  decode[User](jsonString) match {
    case Right(user) => println(s"Usuario cargado: ${user.name} con ${user.emails.size} correos.")
    case Left(error) => println(s"Error al parsear: $error")
  }
}

```

---

## 4. Obtener datos de columnas específicas (Navegación)

A veces no quieres mapear todo el JSON a una clase, sino simplemente **extraer un valor específico** de una "columna" o campo. Para esto usamos los **Cursors**.

### Ejemplo de navegación manual

Si tienes un JSON complejo y solo quieres el nombre del primer correo:

```scala
val complexJson = """
{
  "metadata": {
    "source": "database_01",
    "timestamp": "2023-10-27"
  },
  "data": [
    { "id": 101, "value": "A" },
    { "id": 102, "value": "B" }
  ]
}
"""

val doc: Json = parse(complexJson).getOrElse(Json.Null)

// Navegar por el JSON como si fuera un árbol
val cursor: HCursor = doc.hcursor

val firstId = cursor
  .downField("data")      // Baja al campo 'data'
  .downArray              // Entra en la lista
  .downField("id")        // Baja al campo 'id' del primer elemento
  .as[Int]                // Intenta convertirlo a Int

println(s"El primer ID encontrado es: $firstId") 
// Resultado: Right(101)

```

---

## 5. Resumen de comandos útiles

| Acción | Método |
| --- | --- |
| **String -> JSON** | `parse(jsonString)` |
| **String -> Case Class** | `decode[MyClass](jsonString)` |
| **Case Class -> JSON** | `myInstance.asJson` |
| **JSON -> String** | `json.noSpaces` o `json.spaces2` |
| **Extraer campo** | `cursor.downField("nombre").as[String]` |

---

> **Nota:** Circe es "type-safe". Si intentas extraer un campo que no existe o con un tipo incorrecto, obtendrás un `Left(DecodingFailure)`, lo que evita errores en tiempo de ejecución.


---

# Movie Crew ETL Processor

## Descripción General

Este script en **Scala** implementa un pipeline ETL (Extract, Transform, Load) diseñado para limpiar y estructurar datos complejos de películas. Su objetivo principal es procesar la columna `crew` (equipo de producción) de un archivo CSV raw, la cual contiene datos semi-estructurados (formato string de Python), y transformarla en un formato **JSON válido y tipado**.

El script utiliza la librería **Circe** para el manejo de JSON y técnicas de programación funcional para el parseo de texto.

---

## Componentes Críticos del Código

A continuación, se detallan las dos secciones más complejas e importantes del script:

### 1. El algoritmo de Parseo CSV (`parseCSVLine`)

> *Relacionado con `// Decodificador personalizado de Circe para CSV*`

Los archivos CSV complejos a menudo rompen los parsers tradicionales (`split(";")`) porque el carácter separador (`;`) puede existir legítimamente dentro de un campo de texto (por ejemplo, dentro de un JSON o una descripción).

Para solucionar esto, el código implementa una **Máquina de Estados Finitos** usando `foldLeft`:

```scala
def parseCSVLine(line: String): Array[String] = {
  val (fields, lastBuilder, _) = line.foldLeft(
    (Vector.empty[String], new StringBuilder, false)
  ) { ... }
}

```

**Explicación detallada del algoritmo:**

1. **Acumulador de Estado:** Se inicia con una tupla de tres elementos:
* `fields`: Un vector con las columnas ya procesadas.
* `current`: Un constructor de string (`StringBuilder`) para la celda actual que se está leyendo.
* `inQuotes`: Un booleano (`false`/`true`) que indica si el cursor está actualmente "dentro" de unas comillas.


2. **Iteración Carácter por Carácter:**
* **Caso `"` (Comillas):** No se añaden al texto, simplemente invierten el estado de `inQuotes` (de false a true y viceversa). Esto protege el contenido interno.
* **Caso `;` (Separador):**
* Si `inQuotes` es `true`: El punto y coma se considera texto literal (parte del contenido).
* Si `inQuotes` es `false`: Se considera un separador de columna. Se guarda lo acumulado en `current` dentro de `fields` y se reinicia el constructor.


* **Otros caracteres:** Se añaden al `current` builder.



**Por qué es vital:** Sin esta lógica, el JSON del `crew` (que contiene comillas y comas) se cortaría en pedazos, corrompiendo toda la fila.

---

### 2. Procesamiento y Transformación de Filas

> *Relacionado con `// Procesar cada fila manteniendo la estructura original*`

Esta sección es el núcleo de la transformación de datos. No se limita a leer, sino que "sanea" la información corrupta.

```scala
val filasLimpias: List[FilaCSV] = lines.tail.map { line =>
    // ... Lógica de extracción ...
}

```

**Flujo de trabajo detallado por fila:**

1. **Tokenización:** Llama a `parseCSVLine(line)` para obtener un array de strings seguro.
2. **Localización:** Busca el índice dinámico de la columna `crew` (`parts(crewIndex)`). Esto hace que el código sea robusto incluso si cambian las columnas de orden.
3. **Sanitización de Texto (`cleanCrewJson`):**
* El input original tiene formato de lista de Python: `[{'id': 1, 'name': None}]`.
* Esto **NO** es JSON válido.
* El código aplica Regex para transformar:
* `None` ➔ `null`
* `True/False` ➔ `true/false` (minúsculas)
* `'` (comilla simple) ➔ `"` (comilla doble)




4. **Decodificación (Circe):** Intenta convertir ese string saneado en una lista de objetos Scala `List[Crew]`.
5. **Manejo de Fallos (Try/Catch):**
* Si la fila está corrupta, **no detiene el programa**.
* En su lugar, devuelve una lista vacía `List.empty[Crew]`. Esto garantiza que un error en la línea 50 no impida procesar la línea 50,000.


6. **Deep Cleaning (`limpiarCrew`):** Una vez que tenemos objetos, entramos a cada campo (`Option[String]`) y aplicamos `trim` y normalización de espacios.

---

## Otros Componentes Importantes

* **Case Classes (`Crew`, `FilaCSV`):** Definen el esquema de datos. Al usar `Option[...]`, el código maneja nativamente la ausencia de datos sin lanzar `NullPointerException`.
* **Circe (`io.circe`):** Es la librería encargada de la magia de serialización/deserialización. `generic.auto._` permite que Circe entienda las *Case Classes* sin que tengamos que escribir mapeos manuales campo por campo.
* **Escritura (`writer`):** Al guardar el nuevo CSV, se usa la función `escaparCSV` para asegurar que el nuevo JSON (que contiene muchas comillas dobles) se guarde correctamente escapado (ej: `"{...}"`) conforme al estándar RFC 4180.

---

## Validación: ¿Cómo verificar que funciona correctamente?

El script incluye un módulo de **auditoría interna** al final. Para confirmar que el código ha funcionado correctamente, debes revisar la salida de la consola (logs) basándote en estos indicadores:

### 1. Balance de Masa (Integridad)

Revisa las primeras líneas del reporte:

> * `Total de filas procesadas`: Debe coincidir con el número de líneas de tu archivo original (menos el header).
> * `Filas con crew`: Si este número es muy bajo (cercano a 0), **algo falló** en la función `cleanCrewJson`. Si es alto, el parseo fue exitoso.

### 2. Coherencia de Negocio

Revisa las secciones "Estadísticas por Departamento" y "Trabajos más comunes":

* **Prueba de Éxito:** Deberías ver términos lógicos de cine como "Directing", "Camera", "Production", "Director", "Producer".
* **Indicador de Error:** Si ves fragmentos de código, símbolos extraños o todo agrupado en "null", la decodificación falló.

### 3. Inspección Visual de Muestra

El script imprime: `EJEMPLO DE FILAS LIMPIAS (3 primeras con crew)`.

* Observa el JSON impreso.
* **Correcto:** `"name": "Steven Spielberg", "job": "Director"`
* **Incorrecto:** `'name': 'Steven Spielberg'` (comillas simples) o `None`.

Si el output muestra estructuras JSON limpias con doble comilla y los conteos estadísticos tienen sentido semántico, el código ha funcionado **de manera correcta**.

---

## Resumen Técnico

| Característica | Implementación | Propósito |
| --- | --- | --- |
| **Parsing CSV** | `foldLeft` + `StringBuilder` | Manejar `;` dentro de comillas sin romper columnas. |
| **Parsing JSON** | `io.circe` | Convertir strings a objetos tipados (`List[Crew]`). |
| **Sanitización** | Regex (`replaceAll`) | Transformar sintaxis Python (`None`, `'`) a JSON estándar. |
| **Output** | `PrintWriter` | Escribir un CSV válido escapando caracteres especiales. |


## SALIDA DEL CODIGO
```text
============================================================
RESUMEN DE PROCESAMIENTO CON CIRCE
============================================================
Total de filas procesadas: 3499
Filas con crew: 3227
Total de miembros de crew: 33385
Promedio de crew por fila: 10.345522156801984

Campos con valores null:
  - Credit IDs null: 0
  - Nombres null: 0
  - Departamentos null: 0
  - Gender null: 0
  - IDs null: 0
  - Jobs null: 0
  - Profile paths null: 25649

============================================================
DISTRIBUCIÓN POR GÉNERO
============================================================
  No especificado     : 17.780 registros
  Femenino            :  2.565 registros
  Masculino           : 13.040 registros

============================================================
ESTADÍSTICAS POR DEPARTAMENTO (TOP 10)
============================================================
  Production                    :  6.839 registros
  Writing                       :  5.383 registros
  Directing                     :  4.127 registros
  Sound                         :  3.677 registros
  Art                           :  3.187 registros
  Camera                        :  2.548 registros
  Costume & Make-Up             :  2.337 registros
  Editing                       :  2.265 registros
  Crew                          :  2.104 registros
  Visual Effects                :    637 registros

============================================================
TRABAJOS MÁS COMUNES (TOP 10)
============================================================
  Director                      :  3.521 registros
  Producer                      :  3.371 registros
  Screenplay                    :  1.997 registros
  Writer                        :  1.906 registros
  Editor                        :  1.906 registros
  Director of Photography       :  1.741 registros
  Original Music Composer       :  1.359 registros
  Executive Producer            :  1.134 registros
  Casting                       :  1.075 registros
  Art Direction                 :    928 registros

============================================================
EJEMPLO DE FILAS LIMPIAS (3 primeras con crew)
============================================================

Fila 1:
  Total de crew: 12
  Crew limpia:
[
  {
    "credit_id" : "52fe4217c3a36847f80035c1",
    "department" : "Directing",
    "gender" : 2,
    "id" : 956,
    "job" : "Director",
    "name" : "Guy Ritchie",
    "profile_path" : "/uLpiixgcko2W5GLsqBEvSfluyEs.jpg"
  },
  {
    "credit_id" : "52fe4217c3a36847f80035c7",
    "department" : "Production",
    "gender" : 2,
    "id" : 957,
    "job" : "Producer",
    "name" : "Matthew Vaughn",
    "profile_path" : "/Dnbz3B7yy4u0abixuD5LakZgsy.jpg"
  },
  {
    "credit_id" : "52fe4217c3a36847f80035cd",
    "department" : "Sound",
    "gender" : 2,
    "id" : 959,
    "job" : "Original Music Composer",
    "name" : "David A. Hughes",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035d3",
    "department" : "Sound",
    "gender" : 2,
    "id" : 960,
    "job" : "Original Music Composer",
    "name" : "John Murphy",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035d9",
    "department" : "Camera",
    "gender" : 2,
    "id" : 966,
    "job" : "Director of Photography",
    "name" : "Tim Maurice-Jones",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035df",
    "department" : "Art",
    "gender" : 0,
    "id" : 967,
    "job" : "Production Design",
    "name" : "Iain Andrews",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035e5",
    "department" : "Art",
    "gender" : 0,
    "id" : 968,
    "job" : "Production Design",
    "name" : "Eve Mavrakis",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035eb",
    "department" : "Production",
    "gender" : 1,
    "id" : 970,
    "job" : "Casting",
    "name" : "Celestia Fox",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f80035f1",
    "department" : "Production",
    "gender" : 2,
    "id" : 956,
    "job" : "Casting",
    "name" : "Guy Ritchie",
    "profile_path" : "/uLpiixgcko2W5GLsqBEvSfluyEs.jpg"
  },
  {
    "credit_id" : "52fe4217c3a36847f80035f7",
    "department" : "Editing",
    "gender" : 0,
    "id" : 971,
    "job" : "Editor",
    "name" : "Niven Howie",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe4217c3a36847f8003651",
    "department" : "Writing",
    "gender" : 2,
    "id" : 956,
    "job" : "Screenplay",
    "name" : "Guy Ritchie",
    "profile_path" : "/uLpiixgcko2W5GLsqBEvSfluyEs.jpg"
  },
  {
    "credit_id" : "534feadf0e0a267eab000f7c",
    "department" : "Costume & Make-Up",
    "gender" : 0,
    "id" : 39670,
    "job" : "Costume Design",
    "name" : "Stephanie Collie",
    "profile_path" : null
  }
]

Fila 2:
  Total de crew: 5
  Crew limpia:
[
  {
    "credit_id" : "52fe49c7c3a36847f81a5a21",
    "department" : "Directing",
    "gender" : 2,
    "id" : 14855,
    "job" : "Director",
    "name" : "Frank Borzage",
    "profile_path" : "/ywsES5vf5uH4GelIDmaJWJAhdOG.jpg"
  },
  {
    "credit_id" : "52fe49c7c3a36847f81a5a3f",
    "department" : "Writing",
    "gender" : 2,
    "id" : 96253,
    "job" : "Screenplay",
    "name" : "Wells Root",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe49c7c3a36847f81a5a2d",
    "department" : "Writing",
    "gender" : 0,
    "id" : 171837,
    "job" : "Story",
    "name" : "John Sutherland",
    "profile_path" : "/vMzn5BoqA09WoKyj0lyGk8eTiH.jpg"
  },
  {
    "credit_id" : "52fe49c7c3a36847f81a5a45",
    "department" : "Writing",
    "gender" : 0,
    "id" : 1022764,
    "job" : "Screenplay",
    "name" : "Harvey S. Haislip",
    "profile_path" : null
  },
  {
    "credit_id" : "52fe49c7c3a36847f81a5a27",
    "department" : "Writing",
    "gender" : 0,
    "id" : 1022764,
    "job" : "Story",
    "name" : "Harvey S. Haislip",
    "profile_path" : null
  }
]

Fila 3:
  Total de crew: 4
  Crew limpia:
[
  {
    "credit_id" : "53f5e242c3a36833f7003a15",
    "department" : "Directing",
    "gender" : 0,
    "id" : 40016,
    "job" : "Director",
    "name" : "Angelina Maccarone",
    "profile_path" : null
  },
  {
    "credit_id" : "53f5e255c3a36833f7003a18",
    "department" : "Writing",
    "gender" : 0,
    "id" : 1355322,
    "job" : "Screenplay",
    "name" : "Susanne Billig",
    "profile_path" : null
  },
  {
    "credit_id" : "58612dadc3a3681a620306a2",
    "department" : "Crew",
    "gender" : 0,
    "id" : 1398486,
    "job" : "Compositors",
    "name" : "Jakob Hansonis",
    "profile_path" : null
  },
  {
    "credit_id" : "58612dbdc3a3681a620306af",
    "department" : "Crew",
    "gender" : 0,
    "id" : 1469094,
    "job" : "Compositors",
    "name" : "Hartmut Ewert",
    "profile_path" : null
  }
]

============================================================
PROCESAMIENTO COMPLETADO ✓
Archivo guardado en: src/main/resources/data/pi-movies-complete-2025-12-04-limpio.csv
============================================================


```
