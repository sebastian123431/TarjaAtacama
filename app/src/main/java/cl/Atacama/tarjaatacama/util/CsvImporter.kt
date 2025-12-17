package cl.Atacama.tarjaatacama.util

import android.content.ContentValues
import android.content.Context
import android.util.Log
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.text.Normalizer
import cl.Atacama.tarjaatacama.db.DB

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
    private const val TAG = "CsvImporter"

    // Orden por defecto adaptado a la petición del usuario:
    // codigo_sag, cuartel, embalaje, etiqueta, logo, plu, productor, variedad, variedad_plu, codigos_trazabilidad
    private val expectedFiles = listOf(
        "CODIGO_SAG.csv",
        "CUARTEL.csv",
        "EMBALAJE.csv",
        "ETIQUETA.csv",
        "LOGO.csv",
        "PLU.csv",
        "PRODUCTOR.csv",
        "VARIEDAD.csv",
        "VARIEDAD_PLU.csv",
        "CODIGOS_TRAZABILIDAD.csv"
    )

    data class FileResult(val fileName: String, val inserted: Int, val skipped: Int, val errors: List<String>)

    fun importFromFolder(context: Context, folderPath: String): String {
        val dbHelper = DB(context)
        val db = dbHelper.writableDatabase
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
            results.add(res)
        }

        // Después de procesar archivos, actualizar sqlite_sequence para tablas con AUTOINCREMENT
        try {
            updateSqliteSequenceIfNeeded(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_EMBALAJE, DB.COL_EMB_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_ETIQUETA, DB.COL_ETI_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_LOGO, DB.COL_LOGO_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_PLU, DB.COL_PLU_ID)
            updateSqliteSequenceIfNeeded(db, DB.TABLE_TRAZABILIDAD, DB.COL_TRAZ_ID)
        } catch (_: Exception) {
            // no fatal
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

    // Ajusta sqlite_sequence para la tabla si existe y si el max(Id) es mayor que el value actual
    private fun updateSqliteSequenceIfNeeded(db: android.database.sqlite.SQLiteDatabase, tableName: String, idCol: String) {
        try {
            val cursor = db.rawQuery("SELECT seq FROM sqlite_sequence WHERE name = ?", arrayOf(tableName))
            var currentSeq: Long = -1
            if (cursor.moveToFirst()) {
                currentSeq = cursor.getLong(0)
            }
            cursor.close()

            val cursorMax = db.rawQuery("SELECT MAX($idCol) FROM $tableName", null)
            var maxId: Long = -1
            if (cursorMax.moveToFirst()) {
                maxId = cursorMax.getLong(0)
            }
            cursorMax.close()

            if (maxId > currentSeq) {
                // actualizar o insertar registro en sqlite_sequence
                db.execSQL("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES(?, ?)", arrayOf<Any>(tableName, maxId))
            }
        } catch (_: Exception) {
            // sqlite_sequence puede no existir o no aplicar, no es fatal
        }
    }

    // ----------------- Parsers / importadores por archivo -----------------

    private fun importProductores(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_PROD_ID, true) || it.equals("Id", true) }
                val codigoIdx = headerCols.indexOfFirst { it.equals("cod_productor", true) }.takeIf { it >= 0 }
                    ?: headerCols.indexOfFirst { it.equals("codigo", true) }.takeIf { it >= 0 }
                    ?: 0
                val nombreIdx = headerCols.indexOfFirst { it.equals("nom_productor", true) }.takeIf { it >= 0 }
                    ?: headerCols.indexOfFirst { it.equals("nombre", true) }.takeIf { it >= 0 }
                    ?: 1

                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val codigo = cols.getOrNull(codigoIdx)?.trim() ?: ""
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    if (codigo.isBlank() || nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos")
                        skipped++
                        continue
                    }
                    // Verificar existencia por codigo único
                    val cur = db.query(DB.TABLE_PRODUCTOR, arrayOf(DB.COL_PROD_ID), "${DB.COL_PROD_CODIGO} = ?", arrayOf(codigo), null, null, null)
                    val exists = cur.moveToFirst()
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        // Si el CSV trae un Id personalizado, preservarlo
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_PROD_ID, idNum)
                        }
                        put(DB.COL_PROD_CODIGO, codigo)
                        put(DB.COL_PROD_NOMBRE, nombre)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_PRODUCTOR, cv)
                    if (id == -1L) {
                        errors.add("$file: línea $lineNo: no se pudo insertar productor $codigo")
                    } else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importCodigosSag(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_SAG_ID, true) || it.equals("Id", true) }
                val codigoIdx = headerCols.indexOfFirst { it.equals("codigo_sag", true) }.takeIf { it >= 0 } ?: 0
                val sdpIdx = headerCols.indexOfFirst { it.equals("cod_sdp_sag", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val codigo = cols.getOrNull(codigoIdx)?.trim() ?: ""
                    val sdp = cols.getOrNull(sdpIdx)?.trim() ?: ""
                    if (codigo.isBlank()) {
                        errors.add("$file: línea $lineNo: codigo_sag vacío")
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_SAG_ID, idNum)
                        }
                        put(DB.COL_SAG_CODIGO_SAG, codigo)
                        put(DB.COL_SAG_COD_SDP_SAG, sdp)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_CODIGO_SAG, cv)
                    if (id == -1L) {
                        errors.add("$file: línea $lineNo: no se pudo insertar codigo_sag $codigo")
                        skipped++
                    } else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importVariedades(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_VAR_ID, true) || it.equals("Id", true) }
                val nombreIdx = headerCols.indexOfFirst { it.equals("nom_variedad", true) }.takeIf { it >= 0 } ?: 0
                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    if (nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: nombre variedad vacío")
                        skipped++
                        continue
                    }
                    val cur = db.query(DB.TABLE_VARIEDAD, arrayOf(DB.COL_VAR_ID), "${DB.COL_VAR_NOMBRE} = ?", arrayOf(nombre), null, null, null)
                    val exists = cur.moveToFirst()
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_VAR_ID, idNum)
                        }
                        put(DB.COL_VAR_NOMBRE, nombre)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_VARIEDAD, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar variedad $nombre") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importCuarteles(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_CUA_ID, true) || it.equals("Id", true) }
                val numIdx = headerCols.indexOfFirst { it.equals("num_cuartel", true) }.takeIf { it >= 0 } ?: 0
                val nombreIdx = headerCols.indexOfFirst { it.equals("nom_cuartel", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val num = cols.getOrNull(numIdx)?.trim() ?: ""
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    if (num.isBlank() || nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos")
                        skipped++
                        continue
                    }
                    val cur = db.query(DB.TABLE_CUARTEL, arrayOf(DB.COL_CUA_ID), "${DB.COL_CUA_NOMBRE} = ?", arrayOf(nombre), null, null, null)
                    val exists = cur.moveToFirst()
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_CUA_ID, idNum)
                        }
                        put(DB.COL_CUA_NUM, num)
                        put(DB.COL_CUA_NOMBRE, nombre)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_CUARTEL, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar cuartel $nombre") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importEmbalajes(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_EMB_ID, true) || it.equals("Id", true) }
                val codigoIdx = headerCols.indexOfFirst { it.equals("codigo", true) }.takeIf { it >= 0 } ?: 0
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
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_EMB_ID, idNum)
                        }
                        put(DB.COL_EMB_CODIGO, codigo)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_EMBALAJE, cv)
                    if (id == -1L) {
                        errors.add("$file: línea $lineNo: fallo insertar embalaje $codigo")
                        skipped++
                    } else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importEtiquetas(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_ETI_ID, true) || it.equals("Id", true) }
                val nombreIdx = headerCols.indexOfFirst { it.equals("nombre", true) || it.equals("nom_etiqueta", true) }.takeIf { it >= 0 } ?: 0
                val imagenIdx = headerCols.indexOfFirst { it.equals("nombre_imagen", true) || it.equals("imagen", true) }.takeIf { it >= 0 } ?: 1
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
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
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
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar etiqueta $nombre") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importLogos(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_LOGO_ID, true) || it.equals("Id", true) }
                val nomCodIdx = headerCols.indexOfFirst { it.equals("nom_cod", true) || it.equals("nom_cod_logo", true) }.takeIf { it >= 0 } ?: 0
                val nombreIdx = headerCols.indexOfFirst { it.equals("nombre", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String?
                while (reader.readLine().also { line = it } != null) {
                    lineNo++
                    val cols = parseCsvLine(line!!)
                    val nomCod = cols.getOrNull(nomCodIdx)?.trim() ?: ""
                    val nombre = cols.getOrNull(nombreIdx)?.trim() ?: ""
                    if (nomCod.isBlank() || nombre.isBlank()) {
                        errors.add("$file: línea $lineNo: datos incompletos")
                        skipped++
                        continue
                    }
                    val cur = db.query(DB.TABLE_LOGO, arrayOf(DB.COL_LOGO_ID), "${DB.COL_LOGO_NOM_COD} = ?", arrayOf(nomCod), null, null, null)
                    val exists = cur.moveToFirst()
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
                    val cv = ContentValues().apply {
                        if (idIdx >= 0) {
                            val idStr = cols.getOrNull(idIdx)?.trim()
                            val idNum = idStr?.toLongOrNull()
                            if (idNum != null) put(DB.COL_LOGO_ID, idNum)
                        }
                        put(DB.COL_LOGO_NOM_COD, nomCod)
                        put(DB.COL_LOGO_NOMBRE, nombre)
                    }
                    val id = insertRowAllowId(db, DB.TABLE_LOGO, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar logo $nomCod") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importPlu(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val idIdx = headerCols.indexOfFirst { it.equals(DB.COL_PLU_ID, true) || it.equals("Id", true) }
                val codeIdx = headerCols.indexOfFirst { it.equals("plu_code", true) || it.equals("code", true) }.takeIf { it >= 0 } ?: 0
                val descIdx = headerCols.indexOfFirst { it.equals("description", true) || it.equals("desc", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String?
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
                    cur.close()
                    if (exists) {
                        skipped++
                        continue
                    }
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
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar PLU $code") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importVariedadPlu(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
                val variedadIdx = headerCols.indexOfFirst { it.equals("variedad", true) || it.equals("nom_variedad", true) }.takeIf { it >= 0 } ?: 0
                val pluIdx = headerCols.indexOfFirst { it.equals("plu_code", true) || it.equals("plu", true) }.takeIf { it >= 0 } ?: 1
                var lineNo = 1
                var line: String?
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
                    val varId: Long
                    val varDiag: String
                    val (tempVarId, tempVarDiag) = if (variedadName.toLongOrNull() != null) {
                        // buscar por id directamente
                        val id = getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_ID, variedadName)
                        id
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_NOMBRE, variedadName)
                    }
                    varId = tempVarId
                    varDiag = tempVarDiag

                    // PLU: CSV puede contener PLU primary key (Id) o PLU code (plu_code). Probar ambos.
                    val (pluId, pluDiag) = if (pluCode.toString().toLongOrNull() != null) {
                        // intentar PLU.Id primero
                        val byId = getIdWithDiagnostics(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_ID, pluCode.toString())
                        if (byId.first != -1L) byId else getIdWithDiagnostics(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_CODE, pluCode.toString())
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_PLU, DB.COL_PLU_ID, DB.COL_PLU_CODE, pluCode.toString())
                    }

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
                    val id = db.insert(DB.TABLE_VARIEDAD_PLU, null, cv)
                    if (id == -1L) errors.add("$file: línea $lineNo: fallo insertar variedad_plu") else inserted++
                }
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
        return FileResult(file.name, inserted, skipped, errors)
    }

    private fun importTrazabilidad(db: android.database.sqlite.SQLiteDatabase, file: File): FileResult {
        val errors = mutableListOf<String>()
        var inserted = 0
        var skipped = 0
        val failedLines = mutableListOf<String>()
        db.beginTransaction()
        try {
            BufferedReader(FileReader(file)).use { reader ->
                val header = reader.readLine() ?: return FileResult(file.name, 0, 0, listOf("Archivo vacío"))
                val headerCols = parseCsvLine(header)
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
                }.takeIf { it >= 0 } ?: 2
                val variedadIdx = headerCols.indexOfFirst { h ->
                    listOf("variedad","variedad_id","nom_variedad","id_variedad").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: 3
                val cuartelIdx = headerCols.indexOfFirst { h ->
                    listOf("cuartel","cuartel_id","nom_cuartel","num_cuartel").any { v -> h.equals(v, true) }
                }.takeIf { it >= 0 } ?: 4

                var lineNo = 1
                var line: String?
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
                        // probar por Id primero, luego por codigo
                        val byId = getIdWithDiagnostics(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_ID, productorCodigo)
                        if (byId.first != -1L) byId else getIdWithDiagnostics(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_CODIGO, productorCodigo)
                    } else {
                        // probar por codigo, luego por nombre
                        val byCode = getIdWithDiagnostics(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_CODIGO, productorCodigo)
                        if (byCode.first != -1L) byCode else getIdWithDiagnostics(db, DB.TABLE_PRODUCTOR, DB.COL_PROD_ID, DB.COL_PROD_NOMBRE, productorCodigo)
                    }

                    // Resolver codigo_sag: puede venir como id numérico o como codigo_sag
                    val (sagId, sagDiag) = if (codigoSag.toLongOrNull() != null) {
                        val byId = getIdWithDiagnostics(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_ID, codigoSag)
                        if (byId.first != -1L) byId else getIdWithDiagnostics(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_CODIGO_SAG, codigoSag)
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_CODIGO_SAG, DB.COL_SAG_ID, DB.COL_SAG_CODIGO_SAG, codigoSag)
                    }

                    // Variedad: ya manejábamos nombre o id; mantenemos esa lógica
                    val (varId, varDiag) = if (variedadName.toLongOrNull() != null) {
                        getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_ID, variedadName)
                    } else {
                        getIdWithDiagnostics(db, DB.TABLE_VARIEDAD, DB.COL_VAR_ID, DB.COL_VAR_NOMBRE, variedadName)
                    }

                    // Cuartel: puede venir como Id, como num_cuartel (num) o como nombre; probar en ese orden
                    val (cuaId, cuaDiag) = if (cuartelName.toLongOrNull() != null) {
                        val byId = getIdWithDiagnostics(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_ID, cuartelName)
                        if (byId.first != -1L) byId else {
                            val byNum = getIdWithDiagnostics(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NUM, cuartelName)
                            if (byNum.first != -1L) byNum else getIdWithDiagnostics(db, DB.TABLE_CUARTEL, DB.COL_CUA_ID, DB.COL_CUA_NOMBRE, cuartelName)
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
                    } else inserted++
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
        // Si no contiene Id, usar insert normal
        return db.insert(table, null, cv)
    }

    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        val cur = StringBuilder()
        var inQuotes = false
        var i = 0
        while (i < line.length) {
            val c = line[i]
            when (c) {
                '"' -> {
                    // toggle quotes, handle double quotes inside quoted field
                    if (inQuotes && i + 1 < line.length && line[i + 1] == '"') {
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
        // eliminar BOM y caracteres invisibles
        if (t.startsWith('\uFEFF')) t = t.substring(1)
        t = t.trim()
        // colapsar espacios múltiples
        t = t.replace(Regex("\\s+"), " ")
        // normalizar y quitar diacríticos
        val normalized = Normalizer.normalize(t, Normalizer.Form.NFD)
        val withoutDiacritics = normalized.replace(Regex("\\p{InCombiningDiacriticalMarks}+"), "")
        return withoutDiacritics.lowercase()
    }

    /** Versión de getId que devuelve diagnóstico (id, detalle) */
    private fun getIdWithDiagnostics(db: android.database.sqlite.SQLiteDatabase, table: String, idCol: String, whereCol: String, whereVal: String): Pair<Long, String> {
        // Intentar exacto
        var cursor = db.query(table, arrayOf(idCol), "$whereCol = ?", arrayOf(whereVal), null, null, null)
        try {
            if (cursor.moveToFirst()) {
                val id = cursor.getLong(cursor.getColumnIndexOrThrow(idCol))
                return Pair(id, "match exacto")
            }
        } finally { cursor.close() }

        // SQL case-insensitive
        val isNumeric = whereVal.toLongOrNull() != null
        if (!isNumeric) {
            val lowerVal = whereVal.lowercase()
            cursor = db.query(table, arrayOf(idCol), "LOWER($whereCol) = ?", arrayOf(lowerVal), null, null, null)
            try {
                if (cursor.moveToFirst()) {
                    val id = cursor.getLong(cursor.getColumnIndexOrThrow(idCol))
                    return Pair(id, "match LOWER()")
                }
            } finally { cursor.close() }

            // búsqueda en memoria comparando normalizados
            cursor = db.query(table, arrayOf(idCol, whereCol), null, null, null, null, null)
            try {
                val target = normalizeStringForCompare(whereVal)
                while (cursor.moveToNext()) {
                    val candidate = cursor.getString(cursor.getColumnIndexOrThrow(whereCol)) ?: ""
                    val candNorm = normalizeStringForCompare(candidate)
                    if (candNorm == target) {
                        val id = cursor.getLong(cursor.getColumnIndexOrThrow(idCol))
                        return Pair(id, "match normalizado: '$candidate'")
                    }
                }
            } finally { cursor.close() }
        }

        return Pair(-1L, "no encontrado")
    }
}
