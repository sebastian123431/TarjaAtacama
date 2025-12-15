package cl.Atacama.tarjaatacama.util

import android.content.Context
import cl.Atacama.tarjaatacama.modelo.Encabezado
import java.io.File
import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Objeto de utilidad para generar archivos CSV a partir de datos de la aplicación.
 */
object CsvGenerator {

    /**
     * Crea un archivo CSV a partir de una lista de encabezados de tarja y lo guarda en el directorio caché de la aplicación.
     */
    fun createCsv(context: Context, encabezados: List<Encabezado>, dateRange: String): File? {
        // Formato para la fecha que se usará en el nombre del archivo.
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
        // Construye el nombre del archivo, ej: "Resumen_Tarjas_2023-10-27.csv"
        val fileName = "Resumen_Tarjas_${dateFormat.format(Date())}.csv"
        // Crea el objeto File en el directorio de caché de la app. Es un buen lugar para archivos temporales que no necesitan ser permanentes.
        val file = File(context.cacheDir, fileName)

        try {
            // FileWriter es la clase que nos permite escribir texto en el archivo.
            val writer = FileWriter(file)

            // --- Inicio de la escritura del contenido del CSV ---

            // 1. Encabezado del archivo (título y rango de fechas)
            writer.append("Resumen Tarjas Packing\n")
            writer.append("$dateRange\n\n") // `\n` crea un salto de línea

            // 2. Cabecera de la tabla (los nombres de las columnas)
            writer.append("Tarja,Pallet,Fecha,Prod,Com,PLU,Total Cajas,Estado\n")

            // 3. Datos: Itera sobre cada `encabezado` en la lista para escribir sus datos en una fila.
            for (encabezado in encabezados) {
                writer.append("${encabezado.numTarja},")
                writer.append("${encabezado.numPallet},")
                writer.append("${encabezado.fechaEmbalaje},")
                writer.append("${encabezado.procProd},")
                writer.append("${encabezado.procCom},")
                writer.append("${encabezado.plu},")
                writer.append("${encabezado.totalCajas},")
                writer.append("${encabezado.status}\n") // Termina la fila con un salto de línea
            }

            // Asegura que todos los datos se escriban en el archivo antes de cerrarlo.
            writer.flush()
            // Cierra el escritor para liberar los recursos del sistema.
            writer.close()

            // Devuelve el archivo creado exitosamente.
            return file
        } catch (e: Exception) {
            // Si algo sale mal (ej: no hay permisos, no hay espacio), imprime el error y devuelve null.
            e.printStackTrace()
            return null
        }
    }
}
