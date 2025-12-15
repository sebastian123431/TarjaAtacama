package cl.Atacama.tarjaatacama.util

import android.content.Context
import cl.Atacama.tarjaatacama.modelo.Encabezado
import org.apache.poi.ss.usermodel.FillPatternType
import org.apache.poi.ss.usermodel.HorizontalAlignment
import org.apache.poi.ss.usermodel.IndexedColors
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * Objeto singleton que contiene la lógica para crear un archivo Excel (.xlsx) a partir de una lista de tarjas.
 * Utiliza la librería Apache POI para la manipulación de documentos Excel.
 */
object ExcelGenerator {

    /**
     * Crea un documento Excel (.xlsx) con un resumen de las tarjas y lo guarda en el directorio caché de la aplicación.
     *
     * @param context El contexto de la aplicación, necesario para acceder al sistema de archivos.
     * @param encabezados La lista de objetos [Encabezado] que se escribirán como filas en el reporte.
     * @param dateRange Un texto que describe el rango de fechas de los datos, para ser incluido en el título del archivo.
     * @return Un objeto [File] que representa el archivo Excel creado, o `null` si ocurrió un error durante el proceso.
     */
    fun createExcel(context: Context, encabezados: List<Encabezado>, dateRange: String): File? {
        // Configura una propiedad del sistema para indicarle a POI que ignore las librerías de fuentes de Java de escritorio (AWT),
        // que no existen en Android y pueden causar un crash. Esto es crucial para que POI funcione en Android.
        System.setProperty("org.apache.poi.ss.usermodel.WorkbookFactory.ignoreMissingFontSystem", "true")

        try {
            // 1. Crear el libro y la hoja de trabajo
            // XSSFWorkbook es para el formato .xlsx (moderno). HSSFWorkbook es para .xls (antiguo).
            val workbook = XSSFWorkbook()
            val sheet = workbook.createSheet("Resumen Tarjas")

            // --- 2. Creación de Estilos para las Celdas ---

            // Estilo para la cabecera de la tabla: fondo azul, texto blanco y centrado.
            val headerStyle = workbook.createCellStyle().apply {
                fillForegroundColor = IndexedColors.ROYAL_BLUE.index
                fillPattern = FillPatternType.SOLID_FOREGROUND
                alignment = HorizontalAlignment.CENTER
                val font = workbook.createFont().apply {
                    color = IndexedColors.WHITE.index
                    bold = true
                }
                setFont(font)
            }

            // Estilo para el título principal: texto grande y en negrita.
            val titleStyle = workbook.createCellStyle().apply {
                val font = workbook.createFont().apply {
                    bold = true
                    fontHeightInPoints = 14.toShort() // La altura se especifica como Short
                }
                setFont(font)
            }

            // --- 3. Escribir contenido en la hoja ---

            // Fila 0: Título principal del reporte.
            val titleRow = sheet.createRow(0)
            titleRow.createCell(0).apply {
                setCellValue("Resumen Tarjas Packing")
                cellStyle = titleStyle
            }

            // Fila 1: Rango de fechas de los datos.
            sheet.createRow(1).createCell(0).setCellValue(dateRange)

            // Fila 3: Cabecera de la tabla (dejando la fila 2 en blanco como separador).
            val headerRow = sheet.createRow(3)
            val headers = listOf("Tarja", "Pallet", "Fecha", "Prod", "Com", "PLU", "Total Cajas", "Estado")
            headers.forEachIndexed { index, header ->
                val cell = headerRow.createCell(index)
                cell.setCellValue(header)
                cell.cellStyle = headerStyle
                // Se establece un ancho de columna fijo. autoSizeColumn() puede ser lento o causar errores en Android.
                sheet.setColumnWidth(index, 15 * 256) // El ancho se mide en unidades de 1/256 de un caracter.
            }

            // 4. Llenar los datos de las tarjas
            var rowIndex = 4 // Empezamos a escribir los datos desde la fila 4.
            for (encabezado in encabezados) {
                val row = sheet.createRow(rowIndex++) // Crea una nueva fila y avanza el índice.
                row.createCell(0).setCellValue(encabezado.numTarja.toDouble())
                row.createCell(1).setCellValue(encabezado.numPallet.toDouble())
                row.createCell(2).setCellValue(encabezado.fechaEmbalaje)
                row.createCell(3).setCellValue(encabezado.procProd.toDouble())
                row.createCell(4).setCellValue(encabezado.procCom.toDouble())
                row.createCell(5).setCellValue(encabezado.plu.toDouble())
                row.createCell(6).setCellValue(encabezado.totalCajas.toDouble())
                row.createCell(7).setCellValue(encabezado.status)
            }

            // --- 5. Guardar el libro de trabajo en un archivo ---
            val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
            val fileName = "Resumen_Tarjas_${dateFormat.format(Date())}.xlsx"
            val file = File(context.cacheDir, fileName) // Guardar en el directorio caché de la app.

            // Se usa un FileOutputStream para escribir el contenido del libro en el archivo físico.
            val fileOut = FileOutputStream(file)
            workbook.write(fileOut)
            // Es muy importante cerrar los recursos para liberar memoria y evitar fugas.
            fileOut.close()
            workbook.close()

            // Devuelve el archivo creado si todo salió bien.
            return file

        } catch (e: Exception) {
            // Si ocurre cualquier error durante el proceso, se imprime en la consola y se devuelve null.
            e.printStackTrace()
            return null
        }
    }
}
