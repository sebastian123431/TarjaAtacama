package cl.Atacama.tarjaatacama.modelo

/**
 * Clase de datos (data class) que representa la estructura de un detalle de tarja.
 * Funciona como un "molde" para los datos leídos de la tabla TARJAS_DETALLE.
 * para administrar y manipular los datos de las tarjas.
 * Intermediario entre la capa de datos y la capa de presentación.
 */
data class Tarja(
    var folio: Int,
    var csg: String,
    var lote: String,
    var sdp: String,
    var linea: String,
    var categoria: String,
    var cajas: Int,
    var id: Int = 0
)
