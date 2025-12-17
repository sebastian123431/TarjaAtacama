package cl.Atacama.tarjaatacama.util

import android.content.Context
import android.util.Log
import cl.Atacama.tarjaatacama.db.DB
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.security.MessageDigest

/**
 * Helper para ejecutar la importación de CSV desde la carpeta por defecto:
 *  context.getExternalFilesDir("import")
 *
 * También copia automáticamente archivos desde `assets/import` (si existen) al folder externo
 * para que no tengas que hacer pushes manuales con adb.
 */
object CsvImportRunner {
    private const val TAG = "CsvImportRunner"
    private const val ASSET_FOLDER = "import"
    private const val META_KEY_HASH = "csv_import_hash"

    fun runImportFromDefaultFolder(context: Context): String {
        val dbHelper = DB(context)
        val db = dbHelper.writableDatabase

        // Primero, intentar copiar desde assets/import si hay archivos ahí
        try {
            copyAssetsImportFolder(context)
        } catch (e: Exception) {
            Log.w(TAG, "Error copiando assets/import: ${e.message}")
        }

        val folder = context.getExternalFilesDir(ASSET_FOLDER) ?: context.filesDir
        val path = folder.absolutePath

        // Calcular hash de la carpeta externa (la fuente definitiva de importación)
        val externalHash = try {
            computeFolderHash(folder)
        } catch (e: Exception) {
            Log.w(TAG, "Error calculando hash de carpeta externa: ${e.message}")
            null
        }

        // Comparar con metadata en BD; si coincide, no hay cambios -> saltar import
        if (externalHash != null) {
            val existing = dbHelper.getMetadata(db, META_KEY_HASH)
            if (existing != null && existing == externalHash) {
                // Si la tabla CODIGOS_TRAZABILIDAD está vacía, forzar la importación
                try {
                    val cursor = db.rawQuery("SELECT COUNT(*) FROM ${DB.TABLE_TRAZABILIDAD}", null)
                    var cnt = -1
                    try {
                        if (cursor.moveToFirst()) cnt = cursor.getInt(0)
                    } finally { cursor.close() }
                    if (cnt <= 0) {
                        Log.i(TAG, "Hash igual pero ${DB.TABLE_TRAZABILIDAD} vacía (count=$cnt) — forzando importación.")
                        // no return; continuar con la importación
                    } else {
                        Log.i(TAG, "No hay cambios en CSV desde última importación; se omite el proceso.")
                        return "Importación omitida: sin cambios detectados"
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "Error comprobando estado de ${DB.TABLE_TRAZABILIDAD}: ${e.message}; procediendo con import.")
                }
            }
        }

        Log.i(TAG, "Iniciando importación desde folder: $path")
        val result = CsvImporter.importFromFolder(context, path)
        Log.i(TAG, "Resultado importación:\n$result")

        // Verificar integridad (buscar filas huérfanas) — si hay errores, no actualizar hash
        val integrityIssues = try {
            checkIntegrity(db)
        } catch (e: Exception) {
            listOf("Error verificando integridad: ${e.message}")
        }

        if (integrityIssues.isNotEmpty()) {
            val sb = StringBuilder()
            sb.append(result)
            sb.append("\nIntegridad: se encontraron problemas (no se actualizará el hash):\n")
            integrityIssues.forEach { sb.append(" - $it\n") }
            Log.e(TAG, "Problemas de integridad detectados: ${integrityIssues.size}")
            return sb.toString()
        }

        // Guardar hash en metadata si todo OK (usar externalHash si está disponible, si no usar hash de assets)
        try {
            if (externalHash != null) {
                dbHelper.setMetadata(db, META_KEY_HASH, externalHash)
                Log.i(TAG, "Hash de import actualizado en metadata (external)")
            } else {
                // fallback: intentar hash de assets
                val assetHash = try {
                    computeAssetsFolderHash(context, ASSET_FOLDER)
                } catch (_: Exception) { null }
                if (assetHash != null) {
                    dbHelper.setMetadata(db, META_KEY_HASH, assetHash)
                    Log.i(TAG, "Hash de import actualizado en metadata (assets)")
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "No se pudo guardar metadata de hash: ${e.message}")
        }

        return result
    }

    /** Copia todo lo que esté en assets/import al folder externo app/files/import */
    private fun copyAssetsImportFolder(context: Context) {
        val assetManager = context.assets
        val assetFolder = ASSET_FOLDER
        val files = assetManager.list(assetFolder) ?: return
        if (files.isEmpty()) return

        val outDir = File(context.getExternalFilesDir(null), assetFolder)
        if (!outDir.exists()) outDir.mkdirs()

        for (name in files) {
            val outFile = File(outDir, name)
            // Si ya existe, sobrescribir
            assetManager.open("$assetFolder/$name").use { input ->
                FileOutputStream(outFile).use { output ->
                    input.copyTo(output)
                }
            }
            Log.i(TAG, "Copiado asset: $name -> ${outFile.absolutePath}")
        }
    }

    private fun computeAssetsFolderHash(context: Context, folder: String): String? {
        val assetManager = context.assets
        val files = assetManager.list(folder) ?: return null
        if (files.isEmpty()) return null

        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(8 * 1024)

        // Incluir nombre de archivo en el hash y su contenido
        for (name in files.sorted()) {
            digest.update(name.toByteArray(Charsets.UTF_8))
            assetManager.open("$folder/$name").use { input: InputStream ->
                var read = input.read(buffer)
                while (read > 0) {
                    digest.update(buffer, 0, read)
                    read = input.read(buffer)
                }
            }
        }

        return digest.digest().toHexString()
    }

    private fun computeFolderHash(folder: File): String? {
        if (!folder.exists() || !folder.isDirectory) return null
        val files = folder.listFiles()?.filter { it.isFile } ?: return null
        if (files.isEmpty()) return null

        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(8 * 1024)
        for (file in files.sortedBy { it.name }) {
            digest.update(file.name.toByteArray(Charsets.UTF_8))
            FileInputStream(file).use { fis ->
                var read = fis.read(buffer)
                while (read > 0) {
                    digest.update(buffer, 0, read)
                    read = fis.read(buffer)
                }
            }
        }
        return digest.digest().toHexString()
    }

    private fun ByteArray.toHexString(): String {
        val sb = StringBuilder()
        for (b in this) {
            sb.append(String.format("%02x", b.toInt() and 0xff))
        }
        return sb.toString()
    }

    /** Verificaciones simples de integridad después de la importación. Devuelve lista de issues (vacía si OK). */
    private fun checkIntegrity(db: android.database.sqlite.SQLiteDatabase): List<String> {
        val issues = mutableListOf<String>()

        // Variables locales para construir consultas usando constantes de DB
        val tableVariedadPlu = DB.TABLE_VARIEDAD_PLU
        val colVpVar = DB.COL_VP_VARIEDAD_ID
        val colVpPlu = DB.COL_VP_PLU_ID
        val tableVariedad = DB.TABLE_VARIEDAD
        val colVarId = DB.COL_VAR_ID
        val tablePlu = DB.TABLE_PLU
        val colPluId = DB.COL_PLU_ID

        // 1) VARIEDAD_PLU: verificar que ambas FK existan
        var varPluQuery = "SELECT vp.$colVpVar, vp.$colVpPlu FROM $tableVariedadPlu vp LEFT JOIN $tableVariedad v ON vp.$colVpVar = v.$colVarId WHERE v.$colVarId IS NULL"
        var cursor = db.rawQuery(varPluQuery, null)
        try {
            if (cursor.moveToFirst()) {
                issues.add("VARIEDAD_PLU: existen filas con variedad_id huérfano")
            }
        } finally { cursor.close() }

        varPluQuery = "SELECT vp.$colVpVar, vp.$colVpPlu FROM $tableVariedadPlu vp LEFT JOIN $tablePlu p ON vp.$colVpPlu = p.$colPluId WHERE p.$colPluId IS NULL"
        cursor = db.rawQuery(varPluQuery, null)
        try {
            if (cursor.moveToFirst()) {
                issues.add("VARIEDAD_PLU: existen filas con plu_id huérfano")
            }
        } finally { cursor.close() }

        // 2) CODIGOS_TRAZABILIDAD: verificar FK referenciales
        val tableTraz = DB.TABLE_TRAZABILIDAD
        val colTrazId = DB.COL_TRAZ_ID
        val colTrazProd = DB.COL_TRAZ_PRODUCTOR_ID
        val colTrazSag = DB.COL_TRAZ_CODIGO_SAG_ID
        val colTrazVar = DB.COL_TRAZ_VARIEDAD_ID
        val colTrazCua = DB.COL_TRAZ_CUARTEL_ID

        var trazQuery = "SELECT t.$colTrazId FROM $tableTraz t LEFT JOIN ${DB.TABLE_PRODUCTOR} pr ON t.$colTrazProd = pr.${DB.COL_PROD_ID} WHERE pr.${DB.COL_PROD_ID} IS NULL"
        cursor = db.rawQuery(trazQuery, null)
        try {
            if (cursor.moveToFirst()) issues.add("CODIGOS_TRAZABILIDAD: existe trazabilidad con productor huérfano")
        } finally { cursor.close() }

        trazQuery = "SELECT t.$colTrazId FROM $tableTraz t LEFT JOIN ${DB.TABLE_CODIGO_SAG} s ON t.$colTrazSag = s.${DB.COL_SAG_ID} WHERE s.${DB.COL_SAG_ID} IS NULL"
        cursor = db.rawQuery(trazQuery, null)
        try {
            if (cursor.moveToFirst()) issues.add("CODIGOS_TRAZABILIDAD: existe trazabilidad con codigo_sag huérfano")
        } finally { cursor.close() }

        trazQuery = "SELECT t.$colTrazId FROM $tableTraz t LEFT JOIN ${DB.TABLE_VARIEDAD} v ON t.$colTrazVar = v.${DB.COL_VAR_ID} WHERE v.${DB.COL_VAR_ID} IS NULL"
        cursor = db.rawQuery(trazQuery, null)
        try {
            if (cursor.moveToFirst()) issues.add("CODIGOS_TRAZABILIDAD: existe trazabilidad con variedad huérfana")
        } finally { cursor.close() }

        trazQuery = "SELECT t.$colTrazId FROM $tableTraz t LEFT JOIN ${DB.TABLE_CUARTEL} c ON t.$colTrazCua = c.${DB.COL_CUA_ID} WHERE c.${DB.COL_CUA_ID} IS NULL"
        cursor = db.rawQuery(trazQuery, null)
        try {
            if (cursor.moveToFirst()) issues.add("CODIGOS_TRAZABILIDAD: existe trazabilidad con cuartel huérfano")
        } finally { cursor.close() }

        return issues
    }
}
