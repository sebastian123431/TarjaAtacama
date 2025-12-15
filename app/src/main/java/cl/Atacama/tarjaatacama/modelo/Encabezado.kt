package cl.Atacama.tarjaatacama.modelo

/**
 * Clase de datos (data class) que representa la estructura de un encabezado de tarja.
 * Funciona como un "molde" para los datos leídos de la tabla TARJAS_ENCABEZADO.
 * para administrar y manipular los datos de las tarjas.
 * Intermediario entre la capa de datos y la capa de presentación.
 */
data class Encabezado(
    val numTarja: Int,
    val numPallet: Int,
    val fechaEmbalaje: String,
    val embalaje: String,
    val etiqueta: String,
    val variedad: String, // Nuevo campo para la variedad
    val recibidor: String,
    val logo: String,
    val procProd: Int,
    val procCom: Int,
    val plu: Int,
    val totalCajas: Int,
    val status: String
)
