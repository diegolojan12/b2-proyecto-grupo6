package models

object Estadisticos:
  
  def promedio(datos: List[Double]): Double =
    if datos.isEmpty then 0.0 else datos.sum / datos.length

  def sumaTotal(datos: List[Double]): Double = datos.sum

  def desviacionEstandar(datos: List[Double]): Double =
    if datos.isEmpty then 0.0
    else
      val media = promedio(datos)
      val varianza = datos.map(x => Math.pow(x - media, 2)).sum / datos.length
      Math.sqrt(varianza)

  // Cálculo de cuartiles con interpolación (movido aquí para evitar dependencia circular)
  def calcularCuartil(ordenados: List[Double], percentil: Double): Double =
    if (ordenados.isEmpty) return 0.0
    val pos = percentil * (ordenados.size - 1)
    val lower = ordenados(pos.toInt)
    val upper = if (pos.toInt + 1 < ordenados.size) ordenados(pos.toInt + 1) else lower
    val fraction = pos - pos.toInt
    lower + fraction * (upper - lower)

  // Métodos para análisis de limpieza (limpieza.scala)
  def calcularEstadisticas(datos: List[Double]): Map[String, Double] =
    if (datos.isEmpty) return Map.empty

    val ordenados = datos.sorted
    val n = ordenados.size
    val media = datos.sum / n
    val varianza = datos.map(x => math.pow(x - media, 2)).sum / n
    val mediana = if (n % 2 == 1) ordenados(n / 2)
    else (ordenados(n / 2 - 1) + ordenados(n / 2)) / 2.0

    Map(
      "min" -> ordenados.head,
      "max" -> ordenados.last,
      "media" -> media,
      "mediana" -> mediana,
      "desv_std" -> math.sqrt(varianza),
      "q1" -> calcularCuartil(ordenados, 0.25),
      "q3" -> calcularCuartil(ordenados, 0.75)
    )

object EstadisticosTexto:
  // Calcula la distribución de frecuencia y devuelve el Top N
  def distribucionFrecuencia(datos: List[String], top: Int = 5): List[(String, Int)] =
    datos
      .filterNot(_.trim.isEmpty) // Ignorar celdas vacías
      .groupBy(identity)         // Agrupar por el contenido del texto
      .map((txt, lista) => (txt, lista.length))
      .toList
      .sortBy(-_._2)             // Ordenar de mayor a menor frecuencia
      .take(top)