package cl.Atacama.tarjaatacama.util

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.pdf.PdfDocument
import cl.Atacama.tarjaatacama.modelo.Encabezado
import java.io.File
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Objeto singleton que contiene la lógica para crear un archivo PDF a partir de una lista de tarjas.
 * Utiliza las clases nativas de Android para la creación de documentos PDF.
 */
object PdfGenerator {

    /**
     * Crea un documento PDF con un resumen de las tarjas y lo guarda en el directorio caché de la aplicación.
     *
     * @param context El contexto de la aplicación, necesario para acceder al sistema de archivos.
     * @param encabezados La lista de objetos [Encabezado] que se dibujarán como filas en la tabla del PDF.
     * @param title El título principal que se mostrará en la parte superior del documento.
     * @param dateRange Un texto que describe el rango de fechas de los datos, que se mostrará debajo del título.
     * @return Un objeto [File] que representa el archivo PDF creado, o `null` si ocurrió un error durante el proceso.
     */
    fun createPdf(context: Context, encabezados: List<Encabezado>, title: String, dateRange: String): File? {
        // Crea un nuevo documento PDF en memoria.
        val document = PdfDocument()

        // Configura el tamaño de la página a A4 (595x842 puntos) y el número de página.
        val pageInfo = PdfDocument.PageInfo.Builder(595, 842, 1).create()
        // Inicia una nueva página y obtiene su 'Canvas', que es la superficie sobre la que se dibuja.
        var page = document.startPage(pageInfo)
        var canvas: Canvas = page.canvas

        // 'Paint' es el "pincel" que se usa para dibujar. Define el color, tamaño del texto, etc.
        val paint = Paint()
        paint.color = Color.BLACK

        // Posición vertical inicial para empezar a dibujar, desde la parte superior de la página.
        var yPosition = 60f

        // --- Dibuja el contenido del PDF ---

        // Dibuja el Título principal
        paint.textSize = 18f
        paint.isFakeBoldText = true // Pone el texto en negrita.
        canvas.drawText(title, 40f, yPosition, paint)
        yPosition += 20f // Mueve la posición vertical hacia abajo.

        // Dibuja el Rango de fechas
        paint.textSize = 10f
        paint.isFakeBoldText = false // Quita la negrita.
        canvas.drawText(dateRange, 40f, yPosition, paint)
        yPosition += 30f // Añade más espacio después del subtítulo.

        // Dibuja la Cabecera de la tabla
        paint.isFakeBoldText = true
        paint.textSize = 10f
        val headerY = yPosition
        // Dibuja cada título de columna en su posición horizontal (eje X) correspondiente.
        canvas.drawText("Tarja", 40f, headerY, paint)
        canvas.drawText("Pallet", 100f, headerY, paint)
        canvas.drawText("Fecha", 160f, headerY, paint)
        canvas.drawText("Prod", 240f, headerY, paint)
        canvas.drawText("Com", 300f, headerY, paint)
        canvas.drawText("PLU", 360f, headerY, paint)
        canvas.drawText("Total Cajas", 420f, headerY, paint)
        canvas.drawText("Estado", 500f, headerY, paint)
        yPosition += 5f

        // Dibuja una línea horizontal debajo de la cabecera.
        canvas.drawLine(40f, yPosition, 555f, yPosition, paint)
        yPosition += 15f // Mueve la posición hacia abajo para los datos.

        // Dibuja los Datos de la tabla
        paint.isFakeBoldText = false
        paint.textSize = 9f
        // Itera sobre cada 'encabezado' de la lista para dibujarlo como una fila.
        for (encabezado in encabezados) {
            // Si la siguiente línea de texto se va a salir de la página, crea una nueva.
            if (yPosition > 800) {
                document.finishPage(page) // Finaliza la página actual.
                page = document.startPage(pageInfo) // Inicia una nueva página.
                canvas = page.canvas // Obtiene el nuevo canvas.
                yPosition = 60f // Reinicia la posición vertical al principio.
                // Opcional: Se podría redibujar la cabecera en la nueva página aquí si se deseara.
            }

            // Dibuja los datos de cada encabezado en sus columnas correspondientes.
            canvas.drawText(encabezado.numTarja.toString(), 40f, yPosition, paint)
            canvas.drawText(encabezado.numPallet.toString(), 100f, yPosition, paint)
            canvas.drawText(encabezado.fechaEmbalaje, 160f, yPosition, paint)
            canvas.drawText(encabezado.procProd.toString(), 240f, yPosition, paint)
            canvas.drawText(encabezado.procCom.toString(), 300f, yPosition, paint)
            canvas.drawText(encabezado.plu.toString(), 360f, yPosition, paint)
            canvas.drawText(encabezado.totalCajas.toString(), 420f, yPosition, paint)
            canvas.drawText(encabezado.status, 500f, yPosition, paint)
            yPosition += 15f // Mueve la posición a la siguiente línea.
        }

        document.finishPage(page) // Finaliza la última página.

        // --- Guarda el archivo en el almacenamiento caché de la app ---
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
        val fileName = "Resumen_Tarjas_${dateFormat.format(Date())}.pdf"
        val file = File(context.cacheDir, fileName)
        try {
            // Escribe el contenido del documento PDF en el archivo físico.
            document.writeTo(FileOutputStream(file))
        } catch (e: Exception) {
            e.printStackTrace()
            document.close() // Asegúrate de cerrar el documento incluso si hay un error.
            return null // Devuelve null si hay un error al escribir.
        }
        document.close() // Cierra el documento para liberar recursos.
        return file
    }
}
