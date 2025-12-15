package cl.Atacama.tarjaatacama.db

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper

class DB(context: Context) : SQLiteOpenHelper(context, DATABASE_NAME, null, DATABASE_VERSION) {

    companion object {
        private const val DATABASE_NAME = "TarjaAtacama.db"
        private const val DATABASE_VERSION = 6

        // --- Tablas Catálogo ---
        const val TABLE_PRODUCTOR = "PRODUCTOR"
        const val COL_PROD_ID = "Id"
        const val COL_PROD_CODIGO = "cod_productor"
        const val COL_PROD_NOMBRE = "nom_productor"

        const val TABLE_CODIGO_SAG = "CODIGO_SAG"
        const val COL_SAG_ID = "Id"
        const val COL_SAG_CODIGO_SAG = "codigo_sag"
        const val COL_SAG_COD_SDP_SAG = "cod_sdp_sag"

        const val TABLE_VARIEDAD = "Variedad"
        const val COL_VAR_ID = "Id"
        const val COL_VAR_NOMBRE = "nom_variedad"

        const val TABLE_CUARTEL = "Cuartel"
        const val COL_CUA_ID = "Id"
        const val COL_CUA_NUM = "num_cuartel"
        const val COL_CUA_NOMBRE = "nom_cuartel"

        const val TABLE_EMBALAJE = "Embalaje"
        const val COL_EMB_ID = "Id"
        const val COL_EMB_CODIGO = "codigo"

        const val TABLE_ETIQUETA = "Etiqueta"
        const val COL_ETI_ID = "Id"
        const val COL_ETI_NOMBRE = "nombre"
        const val COL_ETI_NOMBRE_IMAGEN = "nombre_imagen"

        const val TABLE_LOGO = "Logo"
        const val COL_LOGO_ID = "Id"
        const val COL_LOGO_NOM_COD = "nom_cod"
        const val COL_LOGO_NOMBRE = "nombre"

        const val TABLE_PLU = "PLU"
        const val COL_PLU_ID = "Id"
        const val COL_PLU_CODE = "plu_code"
        const val COL_PLU_DESCRIPTION = "description"

        const val TABLE_VARIEDAD_PLU = "VARIEDAD_PLU"
        const val COL_VP_VARIEDAD_ID = "variedad_id"
        const val COL_VP_PLU_ID = "plu_id"

        const val TABLE_TRAZABILIDAD = "CODIGOS_TRAZABILIDAD"
        const val COL_TRAZ_ID = "Id"
        const val COL_TRAZ_PRODUCTOR_ID = "productor_id"
        const val COL_TRAZ_CODIGO_SAG_ID = "codigo_sag_id"
        const val COL_TRAZ_VARIEDAD_ID = "variedad_id"
        const val COL_TRAZ_CUARTEL_ID = "cuartel_id"

        // --- Tablas Transaccionales ---
        const val TABLE_ENCABEZADO = "TARJA_ENCABEZADO"
        const val COL_ENC_NUM_TARJA = "num_tarja"
        const val COL_ENC_NUM_PALLET = "num_pallet"
        const val COL_ENC_FECHA = "fecha_embalaje"
        const val COL_ENC_EMBALAJE_ID = "Embalaje_id"
        const val COL_ENC_ETIQUETA_ID = "Etiqueta_id"
        const val COL_ENC_VARIEDAD = "variedad" // Nueva columna
        const val COL_ENC_RECIBIDOR = "Recibidor"
        const val COL_ENC_LOGO_NOM = "Logo_nom_cod"
        const val COL_ENC_PROC_PROD = "ProcProd"
        const val COL_ENC_PROC_COM = "ProcCom"
        const val COL_ENC_PLU = "PLU"
        const val COL_ENC_STATUS = "status"

        const val TABLE_DETALLE = "TARJA_DETALLE"
        const val COL_DET_ID = "id_detalle"
        const val COL_DET_NUM_TARJA = "num_tarja"
        const val COL_DET_FOLIO = "folio"
        const val COL_DET_CSG = "csg"
        const val COL_DET_LOTE = "lote"
        const val COL_DET_SDP = "sdp"
        const val COL_DET_LINEA = "linea"
        const val COL_DET_CATEGORIA = "categoria"
        const val COL_DET_CANTIDAD = "cantidad_cajas"
    }

    override fun onConfigure(db: SQLiteDatabase) {
        super.onConfigure(db)
        db.setForeignKeyConstraintsEnabled(true)
    }

    override fun onCreate(db: SQLiteDatabase) {
        createAllTables(db)
        prepopulateData(db)
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        db.execSQL("DROP TABLE IF EXISTS $TABLE_DETALLE")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_ENCABEZADO")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_TRAZABILIDAD")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_VARIEDAD_PLU")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_PLU")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_LOGO")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_ETIQUETA")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_EMBALAJE")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_CUARTEL")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_VARIEDAD")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_CODIGO_SAG")
        db.execSQL("DROP TABLE IF EXISTS $TABLE_PRODUCTOR")
        onCreate(db)
    }

    private fun createAllTables(db: SQLiteDatabase) {
        db.execSQL("CREATE TABLE $TABLE_PRODUCTOR ($COL_PROD_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_PROD_CODIGO TEXT NOT NULL UNIQUE, $COL_PROD_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_CODIGO_SAG ($COL_SAG_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_SAG_CODIGO_SAG TEXT NOT NULL, $COL_SAG_COD_SDP_SAG TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_VARIEDAD ($COL_VAR_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_VAR_NOMBRE TEXT NOT NULL UNIQUE)")
        db.execSQL("CREATE TABLE $TABLE_CUARTEL ($COL_CUA_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_CUA_NUM TEXT NOT NULL, $COL_CUA_NOMBRE TEXT NOT NULL UNIQUE)")
        db.execSQL("CREATE TABLE $TABLE_EMBALAJE ($COL_EMB_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_EMB_CODIGO TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_ETIQUETA ($COL_ETI_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_ETI_NOMBRE TEXT NOT NULL UNIQUE, $COL_ETI_NOMBRE_IMAGEN TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_LOGO ($COL_LOGO_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_LOGO_NOM_COD TEXT NOT NULL UNIQUE, $COL_LOGO_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_PLU ($COL_PLU_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_PLU_CODE INTEGER NOT NULL UNIQUE, $COL_PLU_DESCRIPTION TEXT)")
        
        db.execSQL("CREATE TABLE $TABLE_VARIEDAD_PLU ($COL_VP_VARIEDAD_ID INTEGER NOT NULL, $COL_VP_PLU_ID INTEGER NOT NULL, PRIMARY KEY ($COL_VP_VARIEDAD_ID, $COL_VP_PLU_ID), FOREIGN KEY($COL_VP_VARIEDAD_ID) REFERENCES $TABLE_VARIEDAD($COL_VAR_ID), FOREIGN KEY($COL_VP_PLU_ID) REFERENCES $TABLE_PLU($COL_PLU_ID))")
        
        db.execSQL("CREATE TABLE $TABLE_TRAZABILIDAD ($COL_TRAZ_ID INTEGER PRIMARY KEY AUTOINCREMENT, $COL_TRAZ_PRODUCTOR_ID INTEGER NOT NULL, $COL_TRAZ_CODIGO_SAG_ID INTEGER NOT NULL, $COL_TRAZ_VARIEDAD_ID INTEGER NOT NULL, $COL_TRAZ_CUARTEL_ID INTEGER NOT NULL, FOREIGN KEY($COL_TRAZ_PRODUCTOR_ID) REFERENCES $TABLE_PRODUCTOR($COL_PROD_ID), FOREIGN KEY($COL_TRAZ_CODIGO_SAG_ID) REFERENCES $TABLE_CODIGO_SAG($COL_SAG_ID), FOREIGN KEY($COL_TRAZ_VARIEDAD_ID) REFERENCES $TABLE_VARIEDAD($COL_VAR_ID), FOREIGN KEY($COL_TRAZ_CUARTEL_ID) REFERENCES $TABLE_CUARTEL($COL_CUA_ID))")

        val createEncabezado = """CREATE TABLE $TABLE_ENCABEZADO (
                $COL_ENC_NUM_TARJA INTEGER PRIMARY KEY, 
                $COL_ENC_NUM_PALLET INTEGER, 
                $COL_ENC_FECHA TEXT NOT NULL, 
                $COL_ENC_EMBALAJE_ID INTEGER NOT NULL, 
                $COL_ENC_ETIQUETA_ID INTEGER NOT NULL, 
                $COL_ENC_VARIEDAD TEXT NOT NULL, 
                $COL_ENC_RECIBIDOR TEXT, 
                $COL_ENC_LOGO_NOM TEXT NOT NULL, 
                $COL_ENC_PROC_PROD INTEGER, 
                $COL_ENC_PROC_COM INTEGER, 
                $COL_ENC_PLU INTEGER, 
                $COL_ENC_STATUS TEXT NOT NULL DEFAULT 'pendiente', 
                FOREIGN KEY($COL_ENC_EMBALAJE_ID) REFERENCES $TABLE_EMBALAJE($COL_EMB_ID), 
                FOREIGN KEY($COL_ENC_ETIQUETA_ID) REFERENCES $TABLE_ETIQUETA($COL_ETI_ID), 
                FOREIGN KEY($COL_ENC_LOGO_NOM) REFERENCES $TABLE_LOGO($COL_LOGO_NOM_COD), 
                FOREIGN KEY($COL_ENC_PLU) REFERENCES $TABLE_PLU($COL_PLU_CODE)
            ) """
        db.execSQL(createEncabezado)

        val createDetalle = """CREATE TABLE $TABLE_DETALLE (
                $COL_DET_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                $COL_DET_NUM_TARJA INTEGER NOT NULL,
                $COL_DET_FOLIO INTEGER,
                $COL_DET_CSG TEXT,
                $COL_DET_LOTE TEXT,
                $COL_DET_SDP TEXT,
                $COL_DET_LINEA TEXT,
                $COL_DET_CATEGORIA TEXT,
                $COL_DET_CANTIDAD INTEGER NOT NULL,
                FOREIGN KEY($COL_DET_NUM_TARJA) REFERENCES $TABLE_ENCABEZADO($COL_ENC_NUM_TARJA) ON DELETE CASCADE
            )"""
        db.execSQL(createDetalle)
    }

    private fun prepopulateData(db: SQLiteDatabase) {
        // Prepopulate minimal data only. Bulk data has been removed to keep DB limpia
        prepopulateEmbalajes(db)
        prepopulateProductores(db)
        prepopulateCodigoSag(db)
        prepopulateVariedades(db)
        prepopulateCuarteles(db)
        prepopulatePlu(db)
        prepopulateVariedadPlu(db)
        //prepopulateTrazabilidad(db) // Needs to be updated when mappings are ready
    }

    private fun prepopulateProductores(db: SQLiteDatabase) {
        // Lista vacía por defecto — el usuario podrá importar datos desde Excel o añadir manualmente
        val productores = emptyList<Pair<String, String>>()
        insertInTransaction(db, TABLE_PRODUCTOR, { cv, (codigo, nombre) ->
            cv.put(COL_PROD_CODIGO, codigo)
            cv.put(COL_PROD_NOMBRE, nombre)
        }, productores)
    }

    private fun prepopulateCodigoSag(db: SQLiteDatabase) {
        // Lista vacía para códigos SAG. Rellenar mediante importación si es necesario.
        val codigos = emptyList<Pair<String, String>>()
        insertInTransaction(db, TABLE_CODIGO_SAG, { cv, (codigoSag, codSdp) ->
            cv.put(COL_SAG_CODIGO_SAG, codigoSag)
            cv.put(COL_SAG_COD_SDP_SAG, codSdp)
        }, codigos)
    }

    private fun prepopulateVariedades(db: SQLiteDatabase) {
        // Variedades iniciales reducidas (ejemplo) — ajustar según necesidad
        val variedades = emptyList<String>()
        insertInTransaction(db, TABLE_VARIEDAD, { cv, nombre ->
            cv.put(COL_VAR_NOMBRE, nombre)
        }, variedades)
    }

    private fun prepopulateCuarteles(db: SQLiteDatabase) {
        // Cuarteles vacíos por defecto (lista de pares num -> nombre). Importación desde Excel puede llenar esta lista.
        val cuarteles = emptyList<Pair<Int, String>>()
        insertInTransaction(db, TABLE_CUARTEL, { cv, (num, nombre) ->
            cv.put(COL_CUA_NUM, num)
            cv.put(COL_CUA_NOMBRE, nombre)
        }, cuarteles)
    }
    
    private fun prepopulateTrazabilidad(db: SQLiteDatabase) {
        // Dejar vacío; se deberá poblar cuando existan IDs de variedad y cuartel consolidados
    }

    private fun prepopulateEmbalajes(db: SQLiteDatabase) {
        // Tabla para embalajes: vacía por defecto para que el usuario suba sus propios datos
    }

    private fun prepopulatePlu(db: SQLiteDatabase) {
        // PLU vacíos por defecto; se pueden importar desde Excel
        val plu = emptyList<Pair<Int, String>>()
        insertInTransaction(db, TABLE_PLU, { cv, (code, desc) ->
            cv.put(COL_PLU_CODE, code)
            cv.put(COL_PLU_DESCRIPTION, desc)
        }, plu)
    }

    private fun prepopulateVariedadPlu(db: SQLiteDatabase) {
        // Mapeos variedad<->PLU: no se insertan por ahora. Dejar la tabla lista para importación.
    }

    private fun <T> insertInTransaction(db: SQLiteDatabase, tableName: String, populate: (ContentValues, T) -> Unit, items: List<T>) {
        db.beginTransaction()
        try {
            items.forEach { item ->
                val cv = ContentValues()
                populate(cv, item)
                db.insert(tableName, null, cv)
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }
    
    private fun getColumnId(db: SQLiteDatabase, tableName: String, columnId: String, whereColumn: String, whereValue: String): Long {
        var id: Long = -1
        val cursor = db.query(tableName, arrayOf(columnId), "$whereColumn = ?", arrayOf(whereValue), null, null, null)
        if (cursor.moveToFirst()) {
            val columnIndex = cursor.getColumnIndex(columnId)
            if (columnIndex != -1) {
                id = cursor.getLong(columnIndex)
            }
        }
        cursor.close()
        return id
    }
}
