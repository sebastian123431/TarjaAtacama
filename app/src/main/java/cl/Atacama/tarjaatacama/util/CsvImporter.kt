package cl.Atacama.tarjaatacama.util

import android.content.ContentValues
import android.content.Context
import android.content.SharedPreferences
import android.database.sqlite.SQLiteDatabase
import android.util.Log
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.FileInputStream
import java.text.Normalizer
import java.security.MessageDigest
import cl.Atacama.tarjaatacama.db.DB

private const val TAG = "CsvImporter"

/**
 * Utilidad para importar archivos CSV a la base de datos local.
 *
 * Uso: CsvImporter.importFromFolder(context, folderPath)
 *
 * - Lee archivos CSV con nombres esperados (ver `expectedFiles`)
 * - Inserta en el orden de tablas de referencia primero y tablas relacionales / trazabilidad al final
 * - Evita duplicados comprobando valores únicos definidos en las tablas
 * - Devuelve un resumen con counts y errores por archivo
 */
object CsvImporter {
    private const val PREFS = "csv_importer_prefs"
    private const val KEY_PROCESSED_PREFIX = "processed_"

    // Mapa para convertir ids provenientes del CSV (csvId) a los ids reales en la BD (dbId)
    // Estructura: tableName -> (csvId -> dbId)
    private val idMappings: MutableMap<String, MutableMap<Long, Long>> = mutableMapOf()

    // Orden por defecto adaptado a la petición del usuario:
    // codigo_sag, cuartel, embalaje, etiqueta, logo, plu, productor, variedad, variedad_plu, codigos_trazabilidad
    private val expectedFiles = listOf(
        "CODIGO_SAG.csv",
        "Cuartel.csv",
        "Embalaje.csv",
        "Etiqueta.csv",
        "Logo.csv",
        "PLU.csv",
        "PRODUCTOR.csv",
        "Variedad.csv",
        "VARIEDAD_PLU.csv",
        "CODIGOS_TRAZABILIDAD.csv"
    )

    data class FileResult(val fileName: String, val inserted: Int, val skipped: Int, val errors: List<String>)

    /**
     * Importa todos los CSV desde la carpeta especificada.
     * @param forceReimport si true se fuerza la importación aunque el archivo ya tenga checksum procesado (útil para pruebas).
     */
    fun importFromFolder(context: Context, folderPath: String, forceReimport: Boolean = false): String {
        val dbHelper = DB(context)
        val db: SQLiteDatabase = dbHelper.writableDatabase
        // Intentar desactivar comprobaciones de FK para permitir inserts masivos; se hace antes de iniciar transacciones
        try {
            db.execSQL("PRAGMA foreign_keys = OFF")
            android.util.Log.d(TAG, "PRAGMA foreign_keys = OFF for CSV import")
        } catch (e: Exception) {
            android.util.Log.w(TAG, "No se pudo desactivar FK antes de import: ${e.message}")
        }

        val folder = File(folderPath)
        val results = mutableListOf<FileResult>()

        if (!folder.exists() || !folder.isDirectory) {
            return "Carpeta no encontrada: $folderPath"
        }

        // Si existe un archivo import_order.txt en la carpeta, úsalo para definir el orden
        val orderFile = File(folder, "import_order.txt")
        val processingList: List<String> = if (orderFile.exists() && orderFile.isFile) {
            try {
                orderFile.readLines()
                    .map { it.trim() }
                    .filter { it.isNotEmpty() && !it.startsWith("#") }
            } catch (_: Exception) {
                // En caso de error leyendo el archivo de orden, fallback a expectedFiles
                expectedFiles
            }
        } else {
            expectedFiles
        }

        // Construir mapa de archivos en carpeta por nombre en mayúsculas para comparación case-insensitive
        val filesOnDisk: Map<String, File> = folder.listFiles()?.associateBy { it.name.uppercase() } ?: emptyMap()

        for (fileName in processingList) {
            val key = fileName.uppercase()
            val file = filesOnDisk[key]
            if (file == null || !file.isFile) {
                // No encontrar el archivo no es fatal; reportarlo en el resumen
                results.add(FileResult(fileName, 0, 0, listOf("Archivo no encontrado en carpeta: $fileName")))
                continue
            }

            // calcular checksum MD5 y verificar si ya fue procesado
            val checksum = try { md5(file) } catch (_: Exception) { null }
            if (checksum == null) {
                results.add(FileResult(file.name, 0, 0, listOf("No se pudo calcular checksum para: ${file.name}")))
                continue
            }
            val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            if (!forceReimport && isProcessed(prefs, checksum)) {
                Log.i(TAG, "Archivo ya procesado, se omite por checksum: ${file.name}")
                results.add(FileResult(file.name, 0, 0, listOf("Archivo ya procesado (checksum)")))
                continue
            }

            val res = try {
                when (fileName.uppercase()) {
                    "PRODUCTOR.CSV" -> importProductores(db, file)
                    "CODIGO_SAG.CSV" -> importCodigosSag(db, file)
                    "VARIEDAD.CSV" -> importVariedades(db, file)
                    "CUARTEL.CSV" -> importCuarteles(db, file)
                    "EMBALAJE.CSV" -> importEmbalajes(db, file)
                    "ETIQUETA.CSV" -> importEtiquetas(db, file)
                    "LOGO.CSV" -> importLogos(db, file)
                    "PLU.CSV" -> importPlu(db, file)
                    "VARIEDAD_PLU.CSV" -> importVariedadPlu(db, file)
                    "CODIGOS_TRAZABILIDAD.CSV" -> importTrazabilidad(db, file)
                    else -> FileResult(file.name, 0, 0, listOf("Archivo no manejado"))
                }
            } catch (e: Exception) {
                FileResult(file.name, 0, 0, listOf("Excepción: ${e.message}"))
            }
            // Marcar checksum como procesado solo si no hubo errores críticos (errors.isEmpty())
            if (res.errors.isEmpty()) {
                try { markProcessed(prefs, checksum) } catch (_: Exception) { /* no fatal */ }
            } else {
                Log.w(TAG, "Import terminado con errores para ${file.name}; no se marca checksum para permitir reintentos")
            }
            results.add(res)
        }

        // Después de procesar archivos, actualizar sqlite_sequence para tablas con AUTOINCREMENT
        try {
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_EMBALAJE, DB.COL_EMB_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_ETIQUETA, DB.COL_ETI_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_LOGO, DB.COL_LOGO_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_PLU, DB.COL_PLU_ID)
            updateSqliteSequenceIfNeededInternal(db, DB.TABLE_TRAZABILIDAD, DB.COL_TRAZ_ID)
        } catch (_: Exception) {
            // no fatal
        }

        // Reactivar comprobación FK al final
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            android.util.Log.d(TAG, "PRAGMA foreign_keys = ON after CSV import")
        } catch (e: Exception) {
            android.util.Log.w(TAG, "No se pudo reactivar FK despues de import: ${e.message}")
        }

        // Construir resumen
        val sb = StringBuilder()
        sb.append("Resultado de importación desde: ${folder.absolutePath}\n")
        for (r in results) {
            sb.append("- ${r.fileName}: insertados=${r.inserted}, omitidos=${r.skipped}")
            if (r.errors.isNotEmpty()) {
                sb.append(", errores=${r.errors.size}")
                for (err in r.errors) sb.append("\n    * $err")
            }
            sb.append("\n")
        }
        return sb.toString()
    }

    // ----------------- Parsers / importadores por archivo -----------------

    /**
     * Sincroniza los encabezados del CSV con las columnas de la base de datos.
     * Registra encabezados no reconocidos y valida antes de procesar las filas.
     */
    private fun synchronizeCsvHeadersWithDb(headers: List<String>, dbColumns: List<String>): Map<String, String> {
        val headerMap = mutableMapOf<String, String>()
        headers.forEach { header ->
            val matchingColumn = dbColumns.find { it.equals(header, ignoreCase = true) }
            if (matchingColumn != null) {
                headerMap[header] = matchingColumn
            } else {
                Log.w(TAG, "Header '$header' not found in database columns")
            }
        }
        return headerMap
    }

    // Corrección en validateAndSynchronizeHeaders para manejar encabezados adicionales o faltantes
    private fun validateAndSynchronizeHeaders(headerCols: List<String>, dbColumns: List<String>): Pair<Map<String, String>, List<String>> {
        val columnMapping = synchronizeCsvHeadersWithDb(headerCols, dbColumns)
        val unmappedHeaders = headerCols.filterNot { columnMapping.containsKey(it) }
        if (unmappedHeaders.isNotEmpty()) Log.e(TAG, "Encabezados no válidos: ${unmappedHeaders.joinToString(", ")}")
        return Pair(columnMapping, unmappedHeaders)
    }

    /**
     * Importa productores desde el CSV (función interna de importación por archivo).
     */
    private fun importProductores(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0

        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = safeReadLine(reader, file.name) ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                val dbColumns = listOf(DB.COL_PROD_ID, DB.COL_PROD_CODIGO, DB.COL_PROD_NOMBRE)

                val (columnMapping, unmappedHeaders) = validateAndSynchronizeHeaders(headerCols, dbColumns)
                if (unmappedHeaders.isNotEmpty()) {
                    errors.add("Encabezados no válidos en el archivo CSV")
                    return FileResult(file.name, 0, 0, errors)
                }

                // diagnóstico rápido: volcar header y primeras líneas para inspección
                dumpHeaderAndSample(file, reader, headerCols, 5)

                reader.lineSequence().forEachIndexed { lineNo, line ->
                    val values = parseCsvLine(line)
                    if (values.size < headerCols.size) {
                        errors.add("Línea $lineNo: número de columnas insuficiente")
                        return@forEachIndexed
                    }

                    val contentValues = ContentValues().apply {
                        columnMapping.forEach { (csvHeader, dbColumn) ->
                            val idx = headerCols.indexOf(csvHeader)
                            if (idx >= 0) put(dbColumn, values.getOrNull(idx))
                        }
                    }

                    val id = insertRowAllowId(db, DB.TABLE_PRODUCTOR, contentValues)
                    if (id == -1L) skipped++ else inserted++
                }
            }
            db.setTransactionSuccessful()
        } catch (e: Exception) {
            errors.add("Error al procesar el archivo: ${e.message}")
        } finally {
            db.endTransaction()
        }

        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importCodigosSag(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0

        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                // Usar findHeaderIndex para aceptar variantes como "cod_sdp" o "cod_sdp_sag"
                val idIdx = findHeaderIndex(headerCols, DB.COL_SAG_ID, "Id")
                val codigoIdx = findHeaderIndex(headerCols, DB.COL_SAG_CODIGO_SAG, "codigo_sag", "codigo")
                val sdpIdx = findHeaderIndex(headerCols, DB.COL_SAG_COD_SDP_SAG, "cod_sdp_sag", "cod_sdp", "cod_sdp_sag")

                if (codigoIdx == -1) {
                    errors.add("Encabezado 'codigo_sag' no encontrado en el archivo CSV")
                    return FileResult(file.name, 0, 0, errors)
                }

                reader.lineSequence().forEachIndexed { lineNo, line ->
                    val cols = parseCsvLine(line)
                    val codigo = cols.getOrNull(codigoIdx)?.trim() ?: ""
                    val sdp = if (sdpIdx >= 0) cols.getOrNull(sdpIdx)?.trim() ?: "" else ""
                    // requerir al menos codigo_sag; cod_sdp es opcional si no existe en header
                    if (codigo.isEmpty()) {
                        errors.add("Línea $lineNo: codigo_sag vacío o inválido")
                        return@forEachIndexed
                    }

                    val contentValues = ContentValues().apply {
                        if (idIdx >= 0) put(DB.COL_SAG_ID, cols.getOrNull(idIdx)?.toLongOrNull())
                        put(DB.COL_SAG_CODIGO_SAG, codigo)
                        if (sdp.isNotEmpty()) put(DB.COL_SAG_COD_SDP_SAG, sdp)
                    }

                    val rowId = db.insertWithOnConflict(DB.TABLE_CODIGO_SAG, null, contentValues, SQLiteDatabase.CONFLICT_IGNORE)
                    if (rowId == -1L) {
                        skipped++
                    } else {
                        inserted++
                    }
                }
            }
            db.setTransactionSuccessful()
        } catch (e: Exception) {
            errors.add("Error procesando archivo: ${e.message}")
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importVariedades(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0

        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_VAR_ID, ignoreCase = true) }
                val nombreIdx = headerCols.indexOfFirst { it.equals("nom_variedad", ignoreCase = true) }.takeIf { it >= 0 } ?: 0
                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    if (nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: nombre variedad vacío")
                        skipped++
                        continue
                    }
                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null

                    val contentValues = ContentValues().apply {
                        put(DB.COL_VAR_ID, idFromCsv)
                        put("nom_variedad", nombre)
                    }

                    val rowId = db.insertWithOnConflict(DB.TABLE_VARIEDAD, null, contentValues, SQLiteDatabase.CONFLICT_IGNORE)
                    if (rowId == -1L) {
                        skipped++
                    } else {
                        inserted++
                    }
                }
            }
            db.setTransactionSuccessful()
        } catch (e: Exception) {
            errors.add("Error procesando archivo: ${e.message}")
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importCuarteles(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        val failedLines = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_CUA_ID, true) || it.equals("Id", true) }
                val numIdx = headerCols.indexOfFirst { it.equals("num_cuartel", true) }.takeIf { it >= 0 } ?: 0
                val nombreIdx = headerCols.indexOfFirst { it.equals("nom_cuartel", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val num = cols.getOrNull(numIdx)?.trim() ?: ""
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null

                    // Si no viene Id, requerimos al menos el nombre para poder identificar/crear el registro.
                    if (idFromCsv == null && nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: falta Id y nombre - no se puede importar")
                        failedLines.add(line)
                        skipped++
                        continue
                    }

                    try {
                        if (idFromCsv != null) {
                            // Si viene Id, insertar/reemplazar respetando ese Id; no descartar por num vacío
                            val cv = ContentValues().apply {
                                put(DB.COL_CUA_ID, idFromCsv)
                                if (num.isNotBlank()) put(DB.COL_CUA_NUM, num)
                                if (nombre.isNotBlank()) put(DB.COL_CUA_NOMBRE, nombre)
                            }
                            val id = insertRowAllowId(db, DB.TABLE_CUARTEL, cv)
                            if (id == -1L) {
                                // Fallback: intentar encontrar por num o por nombre y registrar mapping si csv trae Id
                                var foundId: Long = -1L
                                if (num.isNotBlank()) {
                                    val cnum = db.query(DB.TABLE_CUARTEL, arrayOf(DB.COL_CUA_ID), "${DB.COL_CUA_NUM} = ?", arrayOf(num), null, null, null)
                                    if (cnum.moveToFirst()) {
                                        foundId = cnum.getLong(cnum.getColumnIndexOrThrow(DB.COL_CUA_ID))
                                    }
                                    cnum.close()
                                }
                                if (foundId == -1L && nombre.isNotBlank()) {
                                    val cnom = db.query(DB.TABLE_CUARTEL, arrayOf(DB.COL_CUA_ID), "${DB.COL_CUA_NOMBRE} = ?", arrayOf(nombre), null, null, null)
                                    if (cnom.moveToFirst()) {
                                        foundId = cnom.getLong(cnom.getColumnIndexOrThrow(DB.COL_CUA_ID))
                                    }
                                    cnom.close()
                                }
                                if (foundId != -1L) {
                                    registerIdMapping(DB.TABLE_CUARTEL, idFromCsv, foundId)
                                    inserted++
                                } else {
                                    errors.add("$file: línea $lineNo: fallo insertar cuartel $nombre")
                                    failedLines.add(line)
                                    skipped++
                                }
                            } else {
                                inserted++
                                registerIdMapping(DB.TABLE_CUARTEL, idFromCsv, id)
                            }
                        } else {
                            // No hay Id: buscar por nombre (clave natural). NO descartar por num duplicado.
                            val cur = db.query(DB.TABLE_CUARTEL, arrayOf(DB.COL_CUA_ID), "${DB.COL_CUA_NOMBRE} = ?", arrayOf(nombre), null, null, null)
                            val existsByName = cur.moveToFirst()
                            var existingId: Long = -1L
                            if (existsByName) existingId = cur.getLong(cur.getColumnIndexOrThrow(DB.COL_CUA_ID))
                            cur.close()

                            if (existsByName) {
                                // Actualizar el num si viene y es distinto
                                if (num.isNotBlank()) {
                                    try {
                                        val cvUpd = ContentValues().apply { put(DB.COL_CUA_NUM, num) }
                                        db.update(DB.TABLE_CUARTEL, cvUpd, "${DB.COL_CUA_ID} = ?", arrayOf(existingId.toString()))
                                    } catch (e: Exception) {
                                        errors.add("$file: línea $lineNo: fallo actualizar num de cuartel id=$existingId: ${e.message}")
                                    }
                                }
                                // No omitir: permitimos insertar también un nuevo cuartel duplicado
                            } else {
                                // Insertar nuevo cuartel (sin Id explícito)
                                val cv = ContentValues().apply {
                                    if (num.isNotBlank()) put(DB.COL_CUA_NUM, num)
                                    put(DB.COL_CUA_NOMBRE, nombre)
                                }
                                val id = insertRowAllowId(db, DB.TABLE_CUARTEL, cv)
                                if (id == -1L) {
                                    errors.add("$file: línea $lineNo: fallo insertar cuartel $nombre")
                                    failedLines.add(line)
                                    skipped++
                                } else {
                                    inserted++
                                }
                            }
                        }
                    } catch (e: Exception) {
                        errors.add("$file: línea $lineNo: excepción insertar/actualizar cuartel: ${e.message}")
                        failedLines.add(line)
                        skipped++
                    }
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }

        // escribir diagnóstico y failedLines para inspección
        try {
            val diagSb = StringBuilder()
            diagSb.append("Import CUARTEL: insertados=$inserted, omitidos=$skipped, errores=${errors.size}\n")
            for (e in errors) diagSb.append(e).append("\n")
            val diagFile = File(file.parentFile, "DIAG_${file.name}.txt")
            diagFile.bufferedWriter().use { it.write(diagSb.toString()) }
        } catch (_: Exception) { /* no fatal */ }

        try {
            if (failedLines.isNotEmpty()) {
                val outFile = File(file.parentFile, "FAILED_${file.name}")
                outFile.bufferedWriter().use { w ->
                    w.write((file.bufferedReader().use { it.readLine() }) ?: "")
                    w.newLine()
                    for (ln in failedLines) {
                        w.write(ln)
                        w.newLine()
                    }
                }
            }
        } catch (_: Exception) { /* no fatal */ }

        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importEmbalajes(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_EMB_ID, ignoreCase = true) || it.equals("Id", ignoreCase = true) }
                val codigoIdx = headerCols.indexOfFirst { it.equals("codigo", ignoreCase = true) || it.equals(DB.COL_EMB_CODIGO, ignoreCase = true) }.takeIf { it >= 0 } ?: 0

                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val codigo = cols.getOrNull(codigoIdx)?.trim() ?: ""
                    if (codigo.isBlank()) {
                        errors.add("$file: línea $lineNo: codigo vacío")
                        skipped++
                        continue
                    }
                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null

                    val cur = db.query(DB.TABLE_EMBALAJE, arrayOf(DB.COL_EMB_ID), "${DB.COL_EMB_CODIGO} = ?", arrayOf(codigo), null, null, null)
                    val exists = cur.moveToFirst()
                    var existingId: Long = -1L
                    if (exists) existingId = cur.getLong(cur.getColumnIndexOrThrow(DB.COL_EMB_ID))
                    cur.close()

                    if (exists) {
                        // Existe un embalaje con mismo codigo: permitimos duplicados (no omitir)
                        // Si el CSV trae Id diferente, registrar mapping y actualizar ese registro
                        if (idFromCsv != null && idFromCsv != existingId) {
                            registerIdMapping(DB.TABLE_EMBALAJE, idFromCsv, existingId)
                            try {
                                val cvUpd = ContentValues().apply { put(DB.COL_EMB_CODIGO, codigo) }
                                db.update(DB.TABLE_EMBALAJE, cvUpd, "${DB.COL_EMB_ID} = ?", arrayOf(existingId.toString()))
                                inserted++
                                continue
                            } catch (e: Exception) {
                                errors.add("$file: línea $lineNo: fallo actualizar embalaje id=$existingId: ${e.message}")
                                // fallthrough: intentar insertar nuevo registro
                            }
                        }
                        // No registrar skip: permitimos crear un nuevo registro aunque exista otro con mismo codigo
                    }

                    val cv = ContentValues().apply {
                        if (idFromCsv != null) put(DB.COL_EMB_ID, idFromCsv)
                        put(DB.COL_EMB_CODIGO, codigo)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_EMBALAJE, cv)
                    if (id == -1L) {
                        val existingCur = db.query(DB.TABLE_EMBALAJE, arrayOf(DB.COL_EMB_ID), "${DB.COL_EMB_CODIGO} = ?", arrayOf(codigo), null, null, null)
                        if (existingCur.moveToFirst()) {
                            val existing = existingCur.getLong(existingCur.getColumnIndexOrThrow(DB.COL_EMB_ID))
                            existingCur.close()
                            if (idFromCsv != null) registerIdMapping(DB.TABLE_EMBALAJE, idFromCsv, existing)
                            inserted++
                        } else {
                            existingCur.close()
                            errors.add("$file: línea $lineNo: no se pudo insertar embalaje $codigo")
                        }
                    } else {
                        inserted++
                        if (idFromCsv != null) registerIdMapping(DB.TABLE_EMBALAJE, idFromCsv, id)
                    }
                }
            }
            db.setTransactionSuccessful()
        } catch (e: Exception) {
            errors.add("Error procesando archivo: ${e.message}")
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importEtiquetas(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_ETI_ID, ignoreCase = true) || it.equals("Id", ignoreCase = true) }
                val nombreIdx = headerCols.indexOfFirst { it.equals("nombre", ignoreCase = true) || it.equals("nom_etiqueta", ignoreCase = true) }.takeIf { it >= 0 } ?: 0
                val imagenIdx = headerCols.indexOfFirst { it.startsWith("nombre_imagen", true) || it.startsWith("imagen", true) }.takeIf { it >= 0 } ?: 1

                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    val imagen = cols.getOrNull(imagenIdx)?.trim() ?: ""
                    if (nombre.isBlank() || imagen.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos")
                        skipped++
                        continue
                    }
                    val cur = db.query(DB.TABLE_ETIQUETA, arrayOf(DB.COL_ETI_ID), "${DB.COL_ETI_NOMBRE} = ?", arrayOf(nombre), null, null, null)
                    val exists = cur.moveToFirst()
                    var existingId: Long = -1L
                    if (exists) existingId = cur.getLong(cur.getColumnIndexOrThrow(DB.COL_ETI_ID))
                    cur.close()
                    if (exists) {
                        // Si CSV trae Id distinto, registrar mapping y actualizar el registro existente
                        val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null
                        if (idFromCsv != null && idFromCsv != existingId) {
                            registerIdMapping(DB.TABLE_ETIQUETA, idFromCsv, existingId)
                            try {
                                val cvUpd = ContentValues().apply { put(DB.COL_ETI_NOMBRE_IMAGEN, imagen); put(DB.COL_ETI_NOMBRE, nombre) }
                                db.update(DB.TABLE_ETIQUETA, cvUpd, "${DB.COL_ETI_ID} = ?", arrayOf(existingId.toString()))
                                inserted++
                                continue
                            } catch (e: Exception) {
                                errors.add("$file: línea $lineNo: fallo actualizar etiqueta id=$existingId: ${e.message}")
                            }
                        }
                        // Si no trae Id, permitimos insertar un nuevo registro duplicado (no omitir)
                    }

                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_ETI_ID, idNum)
                        }
                        put(DB.COL_ETI_NOMBRE, nombre)
                        put(DB.COL_ETI_NOMBRE_IMAGEN, imagen)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_ETIQUETA, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar etiqueta $nombre") else {
                        inserted++
                        if (idFromCsv != null) registerIdMapping(DB.TABLE_ETIQUETA, idFromCsv, id)
                    }
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importLogos(db: SQLiteDatabase, file: File): FileResult {
        // corregir nomCodIdx comparaciones (evitar 'nom_cod'[0])
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = findHeaderIndex(headerCols, DB.COL_LOGO_ID, "Id")
                val nomCodIdx = findHeaderIndex(headerCols, DB.COL_LOGO_NOM_COD, "nom_cod", "nom_cod_logo", "nomcod")
                val nombreIdx = findHeaderIndex(headerCols, DB.COL_LOGO_NOMBRE, "nombre", "imagen_uri", "imagen", "nombre_imagen", "imagen_uri")

                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    // declarar idFromCsv aquí para que esté disponible en todo el bloque
                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null
                    val nomCod = if (nomCodIdx >= 0) cols.getOrNull(nomCodIdx)?.trim() ?: "" else ""
                    val nombre = if (nombreIdx >= 0) cols.getOrNull(nombreIdx)?.trim() ?: "" else ""
                    // Si no existe nom_cod en headers y tampoco imagen/nombre, no podemos importar
                    if (nomCod.isBlank() && nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos (nom_cod o nombre/imagen necesarios)")
                        skipped++
                        continue
                    }
                    val cur = db.query(DB.TABLE_LOGO, arrayOf(DB.COL_LOGO_ID), "${DB.COL_LOGO_NOM_COD} = ?", arrayOf(nomCod), null, null, null)
                    val exists = cur.moveToFirst()
                    var existingIdLogo: Long = -1L
                    if (exists) existingIdLogo = cur.getLong(cur.getColumnIndexOrThrow(DB.COL_LOGO_ID))
                    cur.close()
                    if (exists) {
                        if (idFromCsv != null && idFromCsv != existingIdLogo) {
                            registerIdMapping(DB.TABLE_LOGO, idFromCsv, existingIdLogo)
                            try {
                                val cvUpd = ContentValues().apply { put(DB.COL_LOGO_NOMBRE, nombre); if (nomCod.isNotBlank()) put(DB.COL_LOGO_NOM_COD, nomCod) }
                                db.update(DB.TABLE_LOGO, cvUpd, "${DB.COL_LOGO_ID} = ?", arrayOf(existingIdLogo.toString()))
                                inserted++
                                continue
                            } catch (e: Exception) {
                                errors.add("$file: línea $lineNo: fallo actualizar logo id=$existingIdLogo: ${e.message}")
                            }
                        }
                        // permitir insertar duplicado si no hay Id o Id coincide
                    }

                    val cv = ContentValues().apply {
                        if (idFromCsv != null) put(DB.COL_LOGO_ID, idFromCsv)
                        if (nomCod.isNotBlank()) put(DB.COL_LOGO_NOM_COD, nomCod)
                        if (nombre.isNotBlank()) put(DB.COL_LOGO_NOMBRE, nombre)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_LOGO, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar logo $nomCod") else {
                        inserted++
                        if (idFromCsv != null) registerIdMapping(DB.TABLE_LOGO, idFromCsv, id)
                    }
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importPlu(db: SQLiteDatabase, file: File): FileResult {
        // corregir codeIdx/descIdx declaración ya hecha arriba
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_PLU_ID, ignoreCase = true) || it.equals("Id", ignoreCase = true) }
                val codeIdx = headerCols.indexOfFirst { it.equals("plu_code", ignoreCase = true) || it.equals("plu", ignoreCase = true) }
                val descIdx = headerCols.indexOfFirst { it.equals("description", ignoreCase = true) || it.equals("desc", ignoreCase = true) }

                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val codeStr = cols.getOrNull(codeIdx)?.trim() ?: ""
                    val desc = cols.getOrNull(descIdx)?.trim() ?: ""
                    val code = codeStr.toIntOrNull()
                    if (code == null) {
                        errors.add("$file: línea $lineNo: plu_code inválido: '$codeStr'")
                        skipped++
                        continue
                    }
                    // comprobar existencia por plu_code
                    val cur = db.query(DB.TABLE_PLU, arrayOf(DB.COL_PLU_ID), "${DB.COL_PLU_CODE} = ?", arrayOf(code.toString()), null, null, null)
                    val exists = cur.moveToFirst()
                    var existingPluId: Long = -1L
                    if (exists) existingPluId = cur.getLong(cur.getColumnIndexOrThrow(DB.COL_PLU_ID))
                    cur.close()
                    if (exists) {
                        val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null
                        if (idFromCsv != null && idFromCsv != existingPluId) {
                            registerIdMapping(DB.TABLE_PLU, idFromCsv, existingPluId)
                            try {
                                val cvUpd = ContentValues().apply { put(DB.COL_PLU_CODE, code); put(DB.COL_PLU_DESCRIPTION, desc) }
                                db.update(DB.TABLE_PLU, cvUpd, "${DB.COL_PLU_ID} = ?", arrayOf(existingPluId.toString()))
                                inserted++
                                continue
                            } catch (e: Exception) {
                                errors.add("$file: línea $lineNo: fallo actualizar PLU id=$existingPluId: ${e.message}")
                            }
                        }
                        // Si no hay Id, permitir insertar duplicado
                    }

                    val idFromCsv: Long? = if (idIdx >= 0) cols.getOrNull(idIdx)?.trim()?.toLongOrNull() else null
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_PLU_ID, idNum)
                        }
                        put(DB.COL_PLU_CODE, code)
                        put(DB.COL_PLU_DESCRIPTION, desc)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_PLU, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar PLU $code") else {
                        inserted++
                        if (idFromCsv != null) registerIdMapping(DB.TABLE_PLU, idFromCsv, id)
                    }
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importVariedadPlu(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val headerLine = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(headerLine)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 5)

                val variedadIdx = headerCols.indexOfFirst { it.equals("variedad", ignoreCase = true) || it.equals("nom_variedad", ignoreCase = true) }.takeIf { it >= 0 } ?: 0
                val pluIdx = headerCols.indexOfFirst { it.equals("plu_code", ignoreCase = true) || it.equals("plu", ignoreCase = true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val variedadName = cols.getOrNull(variedadIdx)?.trim() ?: ""
                    val pluCodeStr = cols.getOrNull(pluIdx)?.trim() ?: ""
                    val pluCode = pluCodeStr.toIntOrNull()
                    if (variedadName.isBlank() || pluCode == null) {
                        errors.add("$file: línea $lineNo: datos inválidos")
                        skipped++
                        continue
                    }
                    // obtener ids: la columna 'variedad' puede contener nombre o id numérico
                    val (tempVarId, tempVarDiag) = if (variedadName.toLongOrNull() != null) {
                        resolveIdConsideringMapping(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_ID, variedadName)
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_NOMBRE, variedadName)
                    }
                    val varId = tempVarId
                    val varDiag = tempVarDiag

                    // PLU: CSV puede contener PLU primary key (Id) o PLU code (plu_code). Probar ambos y usar mapping
                    val pluStr = pluCodeStr
                    val pluPair = if (pluStr.toLongOrNull() != null) {
                        val byId = resolveIdConsideringMapping(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_ID, pluStr)
                        if (byId.first != -1L) byId else resolveIdConsideringMapping(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_CODE, pluStr)
                    } else {
                        resolveIdConsideringMapping(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_CODE, pluStr)
                    }
                    val pluId = pluPair.first
                    val pluDiag = pluPair.second

                    if (varId == -1L || pluId == -1L) {
                        errors.add("$file: línea $lineNo: no se encontró variedad o PLU (variedad='$variedadName', plu=$pluCode); detalles: $varDiag, $pluDiag")
                        skipped++
                        continue
                    }
                    // comprobar si ya existe mapeo
                    val cur = db.query(DB.TABLE_VARIEDAD_PLU, arrayOf(DB.COL_VP_VARIEDAD_ID), "${DB.COL_VP_VARIEDAD_ID} = ? AND ${DB.COL_VP_PLU_ID} = ?", arrayOf(varId.toString(), pluId.toString()), null, null, null)
                    val exists = cur.moveToFirst()
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        put(DB.COL_VP_VARIEDAD_ID, varId)
                        put(DB.COL_VP_PLU_ID, pluId)
                    }
                    // usar insertWithOnConflict para evitar duplicados si existe constraint UNIQUE
                    val id = db.insertWithOnConflict(DB.TABLE_VARIEDAD_PLU, null, cv, SQLiteDatabase.CONFLICT_IGNORE)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar variedad_plu") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importTrazabilidad(db: SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        val failedLines = mutableListOf<String>()
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                // diagnóstico rápido
                dumpHeaderAndSample(file, reader, headerCols, 8)

                Log.i(TAG, "importTrazabilidad: headerCols=${headerCols}")
                // Dump quick diagnostics: counts and first rows of referenced tables to help debugging
                try {
                    val diagSb = StringBuilder()
                    fun dumpSample(queryCount: String, querySample: String, label: String) {
                        val cCount = db.rawQuery(queryCount, null)
                        var cnt = 0
                        try { if (cCount.moveToFirst()) cnt = cCount.getInt(0) } finally { cCount.close() }
                        diagSb.append("$label: count=$cnt\n")
                        val cs = db.rawQuery(querySample, null)
                        try {
                            var i = 0
                            while (cs.moveToNext() && i < 20) {
                                val row = StringBuilder()
                                for (j in 0 until cs.columnCount) {
                                    row.append(cs.getColumnName(j)).append("=").append(cs.getString(j)).append(";")
                                }
                                diagSb.append(row.toString()).append("\n")
                                i++
                            }
                        } finally { cs.close() }
                        diagSb.append("\n")
                    }
                    dumpSample("SELECT COUNT(*) FROM ${DB.TABLE_PRODUCTOR}", "SELECT ${DB.COL_PROD_ID}, ${DB.COL_PROD_CODIGO}, ${DB.COL_PROD_NOMBRE} FROM ${DB.TABLE_PRODUCTOR} ORDER BY ${DB.COL_PROD_ID} LIMIT 20", "PRODUCTOR")
                    dumpSample("SELECT COUNT(*) FROM ${DB.TABLE_CODIGO_SAG}", "SELECT ${DB.COL_SAG_ID}, ${DB.COL_SAG_CODIGO_SAG} FROM ${DB.TABLE_CODIGO_SAG} ORDER BY ${DB.COL_SAG_ID} LIMIT 20", "CODIGO_SAG")
                    dumpSample("SELECT COUNT(*) FROM ${DB.TABLE_VARIEDAD}", "SELECT ${DB.COL_VAR_ID}, ${DB.COL_VAR_NOMBRE} FROM ${DB.TABLE_VARIEDAD} ORDER BY ${DB.COL_VAR_ID} LIMIT 20", "VARIEDAD")
                    dumpSample("SELECT COUNT(*) FROM ${DB.TABLE_CUARTEL}", "SELECT ${DB.COL_CUA_ID}, ${DB.COL_CUA_NUM}, ${DB.COL_CUA_NOMBRE} FROM ${DB.TABLE_CUARTEL} ORDER BY ${DB.COL_CUA_ID} LIMIT 20", "CUARTEL")
                    Log.i(TAG, "importTrazabilidad DIAG:\n" + diagSb.toString())
                    // escribir archivo de diagnóstico junto al CSV
                    try {
                        val diagFile = File(file.parentFile, "DIAG_${file.name}.txt")
                        diagFile.bufferedWriter().use { w -> w.write(diagSb.toString()) }
                    } catch (_: Exception) { /* no fatal */ }
                } catch (e: Exception) {
                    Log.w(TAG, "importTrazabilidad: fallo al generar DIAG: ${e.message}")
                }
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_TRAZ_ID, true) || it.equals("Id", true) }
                // buscar variantes comunes de nombres de columna (incluye sufijo _id)
                val productorIdx = headerCols.indexOfFirst { h ->
                    listOf("productor","productor_id","cod_productor","cod_productor_id").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: headerCols.indexOfFirst { it.equals("productor", true) }.takeIf { it >= 0 } ?: 1
                val codigoSagIdx = headerCols.indexOfFirst { h ->
                    listOf("codigo_sag","codigo_sag_id","cod_sag","cod_sag_id","codigo","codigo_id").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: headerCols.indexOfFirst { it.equals("codigo", true) }.takeIf { it >= 0 } ?: 2
                val variedadIdx = headerCols.indexOfFirst { h ->
                    listOf("variedad","variedad_id","nom_variedad","id_variedad").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: headerCols.indexOfFirst { it.equals("variedad", true) }.takeIf { it >= 0 } ?: 3
                val cuartelIdx = headerCols.indexOfFirst { h ->
                    listOf("cuartel","cuartel_id","nom_cuartel","num_cuartel").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: headerCols.indexOfFirst { it.equals("cuartel", true) }.takeIf { it >= 0 } ?: 4

                var lineNo = 1
                var line: String? = null
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    if (lineNo <= 3) Log.i(TAG, "importTrazabilidad: línea $lineNo cols=$cols")
                    val productorCodigo = cols.getOrNull(productorIdx)?.trim() ?: ""
                    val codigoSag = cols.getOrNull(codigoSagIdx)?.trim() ?: ""
                    val variedadName = cols.getOrNull(variedadIdx)?.trim() ?: ""
                    val cuartelName = cols.getOrNull(cuartelIdx)?.trim() ?: ""
                    if (productorCodigo.isBlank() || codigoSag.isBlank() || variedadName.isBlank() || cuartelName.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos")
                        Log.w(TAG, "importTrazabilidad: línea $lineNo datos incompletos producto='$productorCodigo' sag='$codigoSag' variedad='$variedadName' cuartel='$cuartelName'")
                        skipped++
                        continue
                    }
                    // Resolver productor: el CSV puede traer el código del productor o su Id numérico
                    val (prodId, prodDiag) = if (productorCodigo.toLongOrNull() != null) {
                        // probar por Id primero, luego por codigo, usando mapping
                        val byId = resolveIdConsideringMapping(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_ID, productorCodigo)
                        if (byId.first != -1L) byId else resolveIdConsideringMapping(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_CODIGO, productorCodigo)
                    } else {
                        // probar por codigo, luego por nombre
                        val byCode = resolveIdConsideringMapping(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_CODIGO, productorCodigo)
                        if (byCode.first != -1L) byCode else resolveIdConsideringMapping(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_NOMBRE, productorCodigo)
                    }

                    // Resolver codigo_sag: puede venir como id numérico o como codigo_sag
                    val (sagId, sagDiag) = if (codigoSag.toLongOrNull() != null) {
                        val byId = resolveIdConsideringMapping(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_ID, codigoSag)
                        if (byId.first != -1L) byId else resolveIdConsideringMapping(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_CODIGO_SAG, codigoSag)
                    } else {
                        resolveIdConsideringMapping(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_CODIGO_SAG, codigoSag)
                    }

                    // Variedad: ya manejábamos nombre o id; mantenemos esa lógica pero usando mapping si es id
                    val (varId, varDiag) = if (variedadName.toLongOrNull() != null) {
                        resolveIdConsideringMapping(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_ID, variedadName)
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_NOMBRE, variedadName)
                    }

                    // Cuartel: puede venir como Id, como num_cuartel (num) o como nombre; probar en ese orden usando mapping
                    val (cuaId, cuaDiag) = if (cuartelName.toLongOrNull() != null) {
                        val byId = resolveIdConsideringMapping(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_ID, cuartelName)
                        if (byId.first != -1L) byId else {
                            val byNum = resolveIdConsideringMapping(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NUM, cuartelName)
                            if (byNum.first != -1L) byNum else resolveIdConsideringMapping(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NOMBRE, cuartelName)
                        }
                    } else {
                        val byName = getIdWithDiagnostics(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NOMBRE, cuartelName)
                        if (byName.first != -1L) byName else getIdWithDiagnostics(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NUM, cuartelName)
                    }

                    if (prodId == -1L || sagId == -1L || varId == -1L || cuaId == -1L) {
                        val msg = "$file: línea $lineNo: referencia no encontrada (productor/sag/variedad/cuartel); detalles: $prodDiag, $sagDiag, $varDiag, $cuaDiag"
                        errors.add(msg)
                        Log.w(TAG, "importTrazabilidad: FALLO linea $lineNo -> prod='$productorCodigo'($prodDiag) sag='$codigoSag'($sagDiag) variedad='$variedadName'($varDiag) cuartel='$cuartelName'($cuaDiag)")
                        // guardar la línea original para análisis posterior
                        failedLines.add(line)
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_TRAZ_ID, idNum)
                        }
                        put(DB.COL_TRAZ_PRODUCTOR_ID, prodId)
                        put(DB.COL_TRAZ_CODIGO_SAG_ID, sagId)
                        put(DB.COL_TRAZ_VARIEDAD_ID, varId)
                        put(DB.COL_TRAZ_CUARTEL_ID, cuaId)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_TRAZABILIDAD, cv)
                    if (id == -1L) {
                        errors.add("$file: línea $lineNo: fallo insertar trazabilidad")
                        Log.e(TAG, "importTrazabilidad: fallo INSERT linea $lineNo cols=$cols cv=$cv")
                        failedLines.add(line)
                    } else {
                        inserted++
                        // si CSV tenía Id y se insertó/replace con ese Id, registrar mapping
                        val csvId = cols.getOrNull(idIdx)?.trim()?.toLongOrNull()
                        if (csvId != null) registerIdMapping(DB.TABLE_TRAZABILIDAD, csvId, id)
                    }
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        // Si hubo líneas fallidas, volcar a un archivo para revisión junto al csv original
        try {
            if (failedLines.isNotEmpty()) {
                val outFile = File(file.parentFile, "FAILED_${file.name}")
                outFile.bufferedWriter().use { w ->
                    // escribir header para referencia
                    w.write("# Archivo original: ${file.name}")
                    w.newLine()
                    w.write("# Líneas que no se pudieron importar (posición: original). Revisa causas en el resumen y logs.")
                    w.newLine()
                    w.write( (file.bufferedReader().use { it.readLine() } ) ?: "" )
                    w.newLine()
                    for (ln in failedLines) {
                        w.write(ln)
                        w.newLine()
                    }
                }
            }
        } catch (_: Exception) {
            // no fatal
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    // ----------------- Helpers -----------------

    // Nuevo: volcar encabezado y primeras N líneas (no consume el stream permanentemente, usa mark/reset)
    private fun dumpHeaderAndSample(file: File, reader: BufferedReader, headerCols: List<String>, sampleLines: Int = 5) {
        try {
            // intentar mark/reset (si no soporta, simplemente no leer sample)
            try {
                reader.mark(2_000_000) // permitir lectura de hasta ~2MB antes de reset
            } catch (_: Exception) { /* algunos readers no soportan mark */ }
            val samples = mutableListOf<String>()
            var i = 0
            var line: String? = null
            while (i < sampleLines && reader.readLine().also { line = it } != null) {
                samples.add(line ?: "")
                i++
            }
            try { reader.reset() } catch (_: Exception) { /* no fatal si no soporta */ }

            val diagSb = StringBuilder()
            diagSb.append("Archivo: ${file.name}\n")
            diagSb.append("Header detectado (${headerCols.size} columnas):\n")
            diagSb.append(headerCols.joinToString(" | ")).append("\n\n")
            diagSb.append("Primeras $i líneas (raw):\n")
            for (s in samples) diagSb.append(s).append("\n")

            val diagFile = File(file.parentFile, "DIAG_HEADER_${file.name}.txt")
            diagFile.bufferedWriter().use { it.write(diagSb.toString()) }
        } catch (e: Exception) {
            Log.w(TAG, "dumpHeaderAndSample falló para ${file.name}: ${e.message}")
        }
    }

    // Nuevo helper: busca el índice de la primera variante de nombre de columna que aparezca en headerCols.
    // Acepta varias variantes y también intenta comparación normalizada (sin acentos, case-insensitive).
    private fun findHeaderIndex(headerCols: List<String>, vararg variants: String): Int {
        // búsqueda directa (ignoreCase)
        for (v in variants) {
            val idx = headerCols.indexOfFirst { it.equals(v, ignoreCase = true) }
            if (idx >= 0) return idx
        }
        // búsqueda normalizada
        val normalizedHeaders = headerCols.mapIndexed { i, h -> i to normalizeStringForCompare(h) }
        for (v in variants) {
            val nv = normalizeStringForCompare(v)
            val found = normalizedHeaders.find { it.second == nv }?.first
            if (found != null) return found
        }
        return -1
    }

    // Inserta una fila en la tabla. Si el ContentValues contiene la columna 'Id' la inserta explícitamente
    // usando INSERT OR REPLACE para asegurar que el Id personalizado se respete (y para sobrescribir si ya existe).
    private fun insertRowAllowId(db: android.database.sqlite.SQLiteDatabase, table: String, cv: ContentValues): Long {
        // La columna Id está definida en las constantes de DB como "Id".
        if (cv.containsKey(DB.COL_PROD_ID) || cv.containsKey(DB.COL_SAG_ID) || cv.containsKey(DB.COL_VAR_ID) || cv.containsKey(DB.COL_CUA_ID) || cv.containsKey(DB.COL_EMB_ID) || cv.containsKey(DB.COL_ETI_ID) || cv.containsKey(DB.COL_LOGO_ID) || cv.containsKey(DB.COL_PLU_ID) || cv.containsKey(DB.COL_TRAZ_ID) || cv.containsKey(DB.COL_DET_ID) || cv.containsKey(DB.COL_ENC_NUM_TARJA)) {
            // Construir SQL dinámico con los campos del ContentValues
            val cols = ArrayList<String>()
            val placeholders = ArrayList<String>()
            val args = ArrayList<Any?>()
            for (entry in cv.valueSet()) {
                cols.add(entry.key)
                placeholders.add("?")
                args.add(entry.value)
            }
            val sql = "INSERT OR REPLACE INTO $table (${cols.joinToString(",")}) VALUES (${placeholders.joinToString(",")})"
            db.execSQL(sql, args.toTypedArray())
            // Si viene Id, devolverlo; en caso contrario devolver -1
            val id = cv.getAsLong(DB.COL_PROD_ID) ?: cv.getAsLong(DB.COL_SAG_ID) ?: cv.getAsLong(DB.COL_VAR_ID) ?: cv.getAsLong(DB.COL_CUA_ID) ?: cv.getAsLong(DB.COL_EMB_ID) ?: cv.getAsLong(DB.COL_ETI_ID) ?: cv.getAsLong(DB.COL_LOGO_ID) ?: cv.getAsLong(DB.COL_PLU_ID) ?: cv.getAsLong(DB.COL_TRAZ_ID) ?: cv.getAsLong(DB.COL_DET_ID) ?: cv.getAsLong(DB.COL_ENC_NUM_TARJA)
            return id ?: -1L
        }
        // Si no contiene Id, usar insertWithOnConflict(..., CONFLICT_IGNORE) para evitar duplicados si existe índice UNIQUE
        return db.insertWithOnConflict(table, null, cv, android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE)
    }

    /**
     * Calcula el checksum MD5 de un archivo.
     */
    private fun md5(file: File): String {
        val digest = MessageDigest.getInstance("MD5")
        val inputStream = FileInputStream(file)
        val buffer = ByteArray(1024)
        var bytesRead: Int
        while (inputStream.read(buffer).also { bytesRead = it } != -1) {
            digest.update(buffer, 0, bytesRead)
        }
        inputStream.close()
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    /**
     * Verifica si un archivo ya fue procesado usando su checksum.
     */
    private fun isProcessed(prefs: SharedPreferences, checksum: String): Boolean {
        return prefs.getBoolean("$KEY_PROCESSED_PREFIX$checksum", false)
    }

    /**
     * Marca un archivo como procesado usando su checksum.
     */
    private fun markProcessed(prefs: SharedPreferences, checksum: String) {
        prefs.edit().putBoolean("$KEY_PROCESSED_PREFIX$checksum", true).apply()
    }

    /**
     * Actualiza la secuencia SQLite para tablas con AUTOINCREMENT.
     */
    private fun updateSqliteSequenceIfNeeded(db: SQLiteDatabase, tableName: String, columnName: String) {
        try {
            val cursor = db.rawQuery("SELECT MAX($columnName) FROM $tableName", null)
            if (cursor.moveToFirst()) {
                val maxId = cursor.getLong(0)
                db.execSQL("UPDATE sqlite_sequence SET seq = $maxId WHERE name = '$tableName'")
            }
            cursor.close()
        } catch (_: Exception) {
            // no fatal
        }
    }

    // Reemplazar parseCsvLine por versión robusta (manejo de comillas y BOM)
    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        var l = line
        if (l.startsWith('\uFEFF')) l = l.substring(1)
        val cur = StringBuilder()
        var inQuotes = false
        var i = 0
        while (i < l.length) {
            val c = l[i]
            when (c) {
                '"' -> {
                    if (inQuotes && i + 1 < l.length && l[i + 1] == '"') {
                        cur.append('"')
                        i++
                    } else {
                        inQuotes = !inQuotes
                    }
                }
                ',' -> {
                    if (inQuotes) cur.append(c) else {
                        result.add(cur.toString())
                        cur.clear()
                    }
                }
                else -> cur.append(c)
            }
            i++
        }
        result.add(cur.toString())
        return result
    }

    private fun normalizeStringForCompare(s: String?): String {
        if (s == null) return ""
        var t = s
        if (t.startsWith('\uFEFF')) t = t.substring(1)
        t = t.trim()
        t = t.replace(Regex("\\s+"), " ")
        val normalized = Normalizer.normalize(t, Normalizer.Form.NFD)
        val withoutDiacritics = normalized.replace(Regex("\\p{InCombiningDiacriticalMarks}+"), "")
        return withoutDiacritics.lowercase()
    }

    private fun getIdWithDiagnostics(db: SQLiteDatabase, table: String, idCol: String, whereCol: String, whereVal: String): Pair<Long, String> {
        var cursor = db.query(table, arrayOf(idCol), "$whereCol = ?", arrayOf(whereVal), null, null, null)
        try {
            if (cursor.moveToFirst()) return Pair(cursor.getLong(cursor.getColumnIndexOrThrow(idCol)), "match exacto")
        } finally { cursor.close() }

        val isNumeric = whereVal.toLongOrNull() != null
        if (!isNumeric) {
            val lowerVal = whereVal.lowercase()
            cursor = db.query(table, arrayOf(idCol), "LOWER($whereCol) = ?", arrayOf(lowerVal), null, null, null)
            try {
                if (cursor.moveToFirst()) return Pair(cursor.getLong(cursor.getColumnIndexOrThrow(idCol)), "match LOWER()")
            } finally { cursor.close() }

            cursor = db.query(table, arrayOf(idCol, whereCol), null, null, null, null, null)
            try {
                val target = normalizeStringForCompare(whereVal)
                while (cursor.moveToNext()) {
                    val candidate = cursor.getString(cursor.getColumnIndexOrThrow(whereCol)) ?: ""
                    if (normalizeStringForCompare(candidate) == target) return Pair(cursor.getLong(cursor.getColumnIndexOrThrow(idCol)), "match normalizado: '$candidate'")
                }
            } finally { cursor.close() }
        }
        return Pair(-1L, "no encontrado")
    }

    // keep other helpers (registerIdMapping, getMappedId, resolveIdConsideringMapping, insertRowAllowId)
    private fun safeReadLine(reader: BufferedReader, fileName: String): String? {
        return try {
            reader.readLine()
        } catch (e: Exception) {
            Log.e(TAG, "Error leyendo línea en $fileName: ${e.message}")
            null
        }
    }

    private fun registerIdMapping(table: String, csvId: Long, dbId: Long) {
        val m = idMappings.getOrPut(table) { mutableMapOf() }
        m[csvId] = dbId
    }

    private fun getMappedId(table: String, csvId: Long): Long? {
        return idMappings[table]?.get(csvId)
    }

    private fun resolveIdConsideringMapping(db: SQLiteDatabase, table: String, idCol: String, whereCol: String, whereVal: String): Pair<Long, String> {
        val asNum = whereVal.toLongOrNull()
        if (asNum != null) {
            val mapped = getMappedId(table, asNum)
            if (mapped != null) return Pair(mapped, "mapped CSV id->$mapped")
        }
        return getIdWithDiagnostics(db, table, idCol, whereCol, whereVal)
    }

    // Implementación final de updateSqliteSequenceIfNeeded (única copia) renombrada
    private fun updateSqliteSequenceIfNeededInternal(db: SQLiteDatabase, tableName: String, columnName: String) {
        try {
            val cursor = db.rawQuery("SELECT MAX($columnName) FROM $tableName", null)
            if (cursor.moveToFirst()) {
                val maxId = cursor.getLong(0)
                db.execSQL("UPDATE sqlite_sequence SET seq = $maxId WHERE name = '$tableName'")
            }
            cursor.close()
        } catch (_: Exception) {
            // no fatal
        }
    }

}
