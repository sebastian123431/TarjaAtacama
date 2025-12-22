package cl.Atacama.tarjaatacama.controller

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteException
import android.util.Log
import cl.Atacama.tarjaatacama.db.DB
import cl.Atacama.tarjaatacama.modelo.Encabezado
import cl.Atacama.tarjaatacama.modelo.Etiqueta
import cl.Atacama.tarjaatacama.modelo.Tarja

class TarjaController(private val context: Context) {

    private val dbHelper = DB(context)
    private val TAG = "TarjaController"

    // Wrappers seguros para abrir la DB: si la migración falla en onUpgrade, capturamos la excepción
    private fun safeReadableDatabase(): SQLiteDatabase? {
        return try {
            dbHelper.readableDatabase
        } catch (e: SQLiteException) {
            Log.e(TAG, "No se pudo abrir la base de datos (readable): ${e.message}")
            null
        } catch (e: Exception) {
            Log.e(TAG, "Error inesperado abriendo DB (readable): ${e.message}")
            null
        }
    }

    private fun safeWritableDatabase(): SQLiteDatabase? {
        return try {
            dbHelper.writableDatabase
        } catch (e: SQLiteException) {
            Log.e(TAG, "No se pudo abrir la base de datos (writable): ${e.message}")
            null
        } catch (e: Exception) {
            Log.e(TAG, "Error inesperado abriendo DB (writable): ${e.message}")
            null
        }
    }

    fun getEncabezado(numTarja: Int): Encabezado? {
        val db = safeReadableDatabase() ?: return null
         var encabezado: Encabezado? = null
         val cursor = db.query(DB.TABLE_ENCABEZADO, null, "${DB.COL_ENC_NUM_TARJA} = ?", arrayOf(numTarja.toString()), null, null, null)

         if (cursor.moveToFirst()) {
             val totalCajas = getTotalCajas(numTarja)
             encabezado = Encabezado(
                 numTarja = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_NUM_TARJA)),
                 numPallet = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_NUM_PALLET)),
                 fechaEmbalaje = cursor.getString(cursor.getColumnIndexOrThrow(DB.COL_ENC_FECHA)),
                 embalaje = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_EMBALAJE_ID)).toString(),
                 etiqueta = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_ETIQUETA_ID)).toString(),
                 variedad = cursor.getString(cursor.getColumnIndexOrThrow(DB.COL_ENC_VARIEDAD)),
                 recibidor = cursor.getString(cursor.getColumnIndexOrThrow(DB.COL_ENC_RECIBIDOR)),
                 logo = cursor.getString(cursor.getColumnIndexOrThrow(DB.COL_ENC_LOGO_NOM)),
                 procProd = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_PROC_PROD)),
                 procCom = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_PROC_COM)),
                 plu = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_ENC_PLU)),
                 totalCajas = totalCajas,
                 status = cursor.getString(cursor.getColumnIndexOrThrow(DB.COL_ENC_STATUS))
             )
         }
         cursor.close()
         return encabezado
     }

     fun updateEncabezado(
        numTarja: Int,
        numPallet: Int,
        fechaEmbalaje: String,
        embalaje: String,
        etiquetaId: String,
        variedad: String,
        recibidor: String,
        logo: String,
        procProd: Int,
        procCom: Int,
        plu: Int
    ): Int {
        val db = safeWritableDatabase() ?: return -1
        val values = ContentValues().apply {
            put(DB.COL_ENC_NUM_PALLET, numPallet)
            put(DB.COL_ENC_FECHA, fechaEmbalaje)
            put(DB.COL_ENC_EMBALAJE_ID, embalaje)
            put(DB.COL_ENC_ETIQUETA_ID, etiquetaId.toIntOrNull())
            put(DB.COL_ENC_VARIEDAD, variedad)
            put(DB.COL_ENC_RECIBIDOR, recibidor)
            put(DB.COL_ENC_LOGO_NOM, logo)
            put(DB.COL_ENC_PROC_PROD, procProd)
            put(DB.COL_ENC_PROC_COM, procCom)
            put(DB.COL_ENC_PLU, plu)
        }
        return db.update(DB.TABLE_ENCABEZADO, values, "${DB.COL_ENC_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
    }

    fun addEncabezado(
        numTarja: Int,
        numPallet: Int,
        fechaEmbalaje: String,
        embalaje: String,
        etiquetaId: String,
        variedad: String,
        recibidor: String,
        logo: String,
        procProd: Int,
        procCom: Int,
        plu: Int
    ): Long {
        val db = safeWritableDatabase() ?: return -1
        val values = ContentValues().apply {
            put(DB.COL_ENC_NUM_TARJA, numTarja)
            put(DB.COL_ENC_NUM_PALLET, numPallet)
            put(DB.COL_ENC_FECHA, fechaEmbalaje)
            put(DB.COL_ENC_EMBALAJE_ID, embalaje)
            put(DB.COL_ENC_ETIQUETA_ID, etiquetaId.toIntOrNull())
            put(DB.COL_ENC_VARIEDAD, variedad)
            put(DB.COL_ENC_RECIBIDOR, recibidor)
            put(DB.COL_ENC_LOGO_NOM, logo)
            put(DB.COL_ENC_PROC_PROD, procProd)
            put(DB.COL_ENC_PROC_COM, procCom)
            put(DB.COL_ENC_PLU, plu)
        }
        return db.insert(DB.TABLE_ENCABEZADO, null, values)
    }

    fun getAllEmbalajes(): List<Pair<Int, String>> {
        val embalajes = mutableListOf<Pair<Int, String>>()
        val db = safeReadableDatabase() ?: return embalajes
        val cursor = db.query(DB.TABLE_EMBALAJE, arrayOf(DB.COL_EMB_ID, DB.COL_EMB_CODIGO), null, null, null, null, "${DB.COL_EMB_CODIGO} ASC")

        with(cursor) {
            while (moveToNext()) {
                val id = getInt(getColumnIndexOrThrow(DB.COL_EMB_ID))
                val codigo = getString(getColumnIndexOrThrow(DB.COL_EMB_CODIGO))
                embalajes.add(Pair(id, codigo))
            }
        }
        cursor.close()
        return embalajes
    }

    fun getAllEtiquetas(): List<Etiqueta> {
        val etiquetas = mutableListOf<Etiqueta>()
        val db = safeReadableDatabase() ?: return etiquetas
        val cursor = db.query(DB.TABLE_ETIQUETA, null, null, null, null, null, "${DB.COL_ETI_ID} ASC")

        with(cursor) {
            while (moveToNext()) {
                val etiqueta = Etiqueta(
                    id = getLong(getColumnIndexOrThrow(DB.COL_ETI_ID)),
                    nombre = getString(getColumnIndexOrThrow(DB.COL_ETI_NOMBRE)),
                    imagenUri = getString(getColumnIndexOrThrow(DB.COL_ETI_NOMBRE_IMAGEN))
                )
                etiquetas.add(etiqueta)
            }
        }
        cursor.close()
        return etiquetas
    }

    fun getAllVariedades(): List<String> {
        val variedades = mutableListOf<String>()
        val db = safeReadableDatabase() ?: return variedades
        val cursor = db.query(DB.TABLE_VARIEDAD, arrayOf(DB.COL_VAR_NOMBRE), null, null, null, null, "${DB.COL_VAR_NOMBRE} ASC")

        with(cursor) {
            while (moveToNext()) {
                val variedad = getString(getColumnIndexOrThrow(DB.COL_VAR_NOMBRE))
                variedades.add(variedad)
            }
        }
        cursor.close()
        return variedades
    }

    fun getPluForVariedad(nombreVariedad: String): Int? {
        val db = safeReadableDatabase() ?: return null
         var pluCode: Int? = null

        val query = """
            SELECT p.${DB.COL_PLU_CODE}
            FROM ${DB.TABLE_VARIEDAD} v
            JOIN ${DB.TABLE_VARIEDAD_PLU} vp ON v.${DB.COL_VAR_ID} = vp.${DB.COL_VP_VARIEDAD_ID}
            JOIN ${DB.TABLE_PLU} p ON vp.${DB.COL_VP_PLU_ID} = p.${DB.COL_PLU_ID}
            WHERE v.${DB.COL_VAR_NOMBRE} = ?
        """

        val cursor = db.rawQuery(query, arrayOf(nombreVariedad))
        if (cursor.moveToFirst()) {
            pluCode = cursor.getInt(cursor.getColumnIndexOrThrow(DB.COL_PLU_CODE))
        }
        cursor.close()
        return pluCode
    }

    fun updateStatusEnviado(numTarja: Int): Int {
        val db = safeWritableDatabase() ?: return 0
        val values = ContentValues().apply {
            put(DB.COL_ENC_STATUS, "enviado")
        }
        return db.update(DB.TABLE_ENCABEZADO, values, "${DB.COL_ENC_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
    }

    fun updateStatusPendiente(numTarja: Int): Int {
        val db = safeWritableDatabase() ?: return 0
        val values = ContentValues().apply {
            put(DB.COL_ENC_STATUS, "pendiente")
        }
        return db.update(DB.TABLE_ENCABEZADO, values, "${DB.COL_ENC_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
    }

    fun getFilteredEncabezados(status: String?, fechaDesde: String?, fechaHasta: String?): List<Encabezado> {
         val encabezados = mutableListOf<Encabezado>()
        val db = safeReadableDatabase() ?: return encabezados

        var selection = ""
        val selectionArgs = mutableListOf<String>()

        if (status != null) {
            selection += "${DB.COL_ENC_STATUS} = ?"
            selectionArgs.add(status)
        }

        if (fechaDesde != null && fechaHasta != null) {
            if (selection.isNotEmpty()) selection += " AND "
            selection += "${DB.COL_ENC_FECHA} BETWEEN ? AND ?"
            selectionArgs.add(fechaDesde)
            selectionArgs.add(fechaHasta)
        }

        val cursor = db.query(DB.TABLE_ENCABEZADO, null, selection.ifEmpty { null }, selectionArgs.toTypedArray(), null, null, "${DB.COL_ENC_NUM_TARJA} DESC")

        with(cursor) {
            while (moveToNext()) {
                val numTarja = getInt(getColumnIndexOrThrow(DB.COL_ENC_NUM_TARJA))
                val totalCajas = getTotalCajas(numTarja)

                val encabezado = Encabezado(
                    numTarja = numTarja,
                    numPallet = getInt(getColumnIndexOrThrow(DB.COL_ENC_NUM_PALLET)),
                    fechaEmbalaje = getString(getColumnIndexOrThrow(DB.COL_ENC_FECHA)),
                    embalaje = getInt(getColumnIndexOrThrow(DB.COL_ENC_EMBALAJE_ID)).toString(),
                    etiqueta = getInt(getColumnIndexOrThrow(DB.COL_ENC_ETIQUETA_ID)).toString(),
                    variedad = getString(getColumnIndexOrThrow(DB.COL_ENC_VARIEDAD)),
                    recibidor = getString(getColumnIndexOrThrow(DB.COL_ENC_RECIBIDOR)),
                    logo = getString(getColumnIndexOrThrow(DB.COL_ENC_LOGO_NOM)),
                    procProd = getInt(getColumnIndexOrThrow(DB.COL_ENC_PROC_PROD)),
                    procCom = getInt(getColumnIndexOrThrow(DB.COL_ENC_PROC_COM)),
                    plu = getInt(getColumnIndexOrThrow(DB.COL_ENC_PLU)),
                    totalCajas = totalCajas,
                    status = getString(getColumnIndexOrThrow(DB.COL_ENC_STATUS))
                )
                encabezados.add(encabezado)
            }
        }
        cursor.close()
        return encabezados
    }

    fun addDetalle(
        numTarja: Int,
        folio: Int,
        csg: String,
        lote: String,
        sdp: String,
        linea: String,
        categoria: String,
        cantidadCajas: Int
    ): Long {
        val db = safeWritableDatabase() ?: return -1
        val values = ContentValues().apply {
            put(DB.COL_DET_NUM_TARJA, numTarja)
            put(DB.COL_DET_FOLIO, folio)
            put(DB.COL_DET_CSG, csg)
            put(DB.COL_DET_LOTE, lote)
            put(DB.COL_DET_SDP, sdp)
            put(DB.COL_DET_LINEA, linea)
            put(DB.COL_DET_CATEGORIA, categoria)
            put(DB.COL_DET_CANTIDAD, cantidadCajas)
        }
        return db.insert(DB.TABLE_DETALLE, null, values)
    }

    fun getDetallesPorTarja(numTarja: Int): List<Tarja> {
         val detalles = mutableListOf<Tarja>()
        val db = safeReadableDatabase() ?: return detalles
         val cursor = db.query(
             DB.TABLE_DETALLE,
             null,
             "${DB.COL_DET_NUM_TARJA} = ?",
             arrayOf(numTarja.toString()),
             null, null, "${DB.COL_DET_ID} DESC"
         )

        with(cursor) {
            while (moveToNext()) {
                val tarja = Tarja(
                    folio = getInt(getColumnIndexOrThrow(DB.COL_DET_FOLIO)),
                    csg = getString(getColumnIndexOrThrow(DB.COL_DET_CSG)),
                    lote = getString(getColumnIndexOrThrow(DB.COL_DET_LOTE)),
                    sdp = getString(getColumnIndexOrThrow(DB.COL_DET_SDP)),
                    linea = getString(getColumnIndexOrThrow(DB.COL_DET_LINEA)),
                    categoria = getString(getColumnIndexOrThrow(DB.COL_DET_CATEGORIA)),
                    cajas = getInt(getColumnIndexOrThrow(DB.COL_DET_CANTIDAD)),
                    id = getInt(getColumnIndexOrThrow(DB.COL_DET_ID))
                )
                detalles.add(tarja)
            }
        }
        cursor.close()
        return detalles
    }

    fun updateDetalle(
        idDetalle: Int,
        folio: Int,
        csg: String,
        lote: String,
        sdp: String,
        linea: String,
        categoria: String,
        cantidadCajas: Int
    ): Int {
        val db = safeWritableDatabase() ?: return 0
        val values = ContentValues().apply {
            put(DB.COL_DET_FOLIO, folio)
            put(DB.COL_DET_CSG, csg)
            put(DB.COL_DET_LOTE, lote)
            put(DB.COL_DET_SDP, sdp)
            put(DB.COL_DET_LINEA, linea)
            put(DB.COL_DET_CATEGORIA, categoria)
            put(DB.COL_DET_CANTIDAD, cantidadCajas)
        }
        return db.update(DB.TABLE_DETALLE, values, "${DB.COL_DET_ID} = ?", arrayOf(idDetalle.toString()))
    }

    fun deleteDetalle(idDetalle: Int): Int {
        val db = safeWritableDatabase() ?: return 0
        return db.delete(DB.TABLE_DETALLE, "${DB.COL_DET_ID} = ?", arrayOf(idDetalle.toString()))
    }

    private fun getTotalCajas(numTarja: Int): Int {
        val db = safeReadableDatabase() ?: return 0
         var total = 0
         val cursor = db.rawQuery("SELECT SUM(${DB.COL_DET_CANTIDAD}) as total FROM ${DB.TABLE_DETALLE} WHERE ${DB.COL_DET_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
         if (cursor.moveToFirst()) {
             total = cursor.getInt(cursor.getColumnIndexOrThrow("total"))
         }
         cursor.close()
         return total
     }

     fun getAllEncabezados(): List<Encabezado> {
         val encabezados = mutableListOf<Encabezado>()
        val db = safeReadableDatabase() ?: return encabezados
         val cursor = db.query(DB.TABLE_ENCABEZADO, null, null, null, null, null, "${DB.COL_ENC_NUM_TARJA} DESC")

        with(cursor) {
            while (moveToNext()) {
                val numTarja = getInt(getColumnIndexOrThrow(DB.COL_ENC_NUM_TARJA))
                val totalCajas = getTotalCajas(numTarja)

                val encabezado = Encabezado(
                    numTarja = numTarja,
                    numPallet = getInt(getColumnIndexOrThrow(DB.COL_ENC_NUM_PALLET)),
                    fechaEmbalaje = getString(getColumnIndexOrThrow(DB.COL_ENC_FECHA)),
                    embalaje = getInt(getColumnIndexOrThrow(DB.COL_ENC_EMBALAJE_ID)).toString(),
                    etiqueta = getInt(getColumnIndexOrThrow(DB.COL_ENC_ETIQUETA_ID)).toString(),
                    variedad = getString(getColumnIndexOrThrow(DB.COL_ENC_VARIEDAD)),
                    recibidor = getString(getColumnIndexOrThrow(DB.COL_ENC_RECIBIDOR)),
                    logo = getString(getColumnIndexOrThrow(DB.COL_ENC_LOGO_NOM)),
                    procProd = getInt(getColumnIndexOrThrow(DB.COL_ENC_PROC_PROD)),
                    procCom = getInt(getColumnIndexOrThrow(DB.COL_ENC_PROC_COM)),
                    plu = getInt(getColumnIndexOrThrow(DB.COL_ENC_PLU)),
                    totalCajas = totalCajas,
                    status = getString(getColumnIndexOrThrow(DB.COL_ENC_STATUS))
                )
                encabezados.add(encabezado)
            }
        }
        cursor.close()
        return encabezados
    }

    fun deleteTarjaCompleta(numTarja: Int): Boolean {
        val db = safeWritableDatabase() ?: return false
         var result = false
         db.beginTransaction()
         try {
             db.delete(DB.TABLE_DETALLE, "${DB.COL_DET_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
             val deletedRows = db.delete(DB.TABLE_ENCABEZADO, "${DB.COL_ENC_NUM_TARJA} = ?", arrayOf(numTarja.toString()))
             db.setTransactionSuccessful()
             result = deletedRows > 0
         } finally {
             db.endTransaction()
         }
         return result
     }
 }
