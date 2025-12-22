package cl.Atacama.tarjaatacama.db

import android.content.ContentValues
import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper

class DB(context: Context) : SQLiteOpenHelper(context, DATABASE_NAME, null, DATABASE_VERSION) {

    // Guardar applicationContext para uso interno (copias de seguridad, paths, etc.)
    private val appContext: Context = context.applicationContext

    companion object {
        // Cambiado el nombre de la BD y la versión
        private const val DATABASE_NAME = "bd_tarja"
        private const val DATABASE_VERSION = 13

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

        const val TABLE_METADATA = "APP_METADATA"
        const val COL_META_KEY = "meta_key"
        const val COL_META_VALUE = "meta_value"
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
        // Si venimos de una versión anterior, migramos preservando datos
        if (oldVersion < 12) {
            try {
                migratePreserveIds(db)
            } catch (e: Exception) {
                // Si la migración falla, registramos el error y hacemos fallback recreando tablas.
                android.util.Log.e("DB", "migratePreserveIds fallo durante onUpgrade, aplicando fallback recrear tablas: ${e.message}", e)
                try {
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
                } catch (_: Exception) {
                    // ignore
                }
                onCreate(db)
            }
        } else {
            // fallback: recrear tablas
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
    }

    // Migra las tablas para permitir insertar Ids explícitos preservando los datos.
    private fun migratePreserveIds(db: SQLiteDatabase) {
        // Crear copia de seguridad local de la BD antes de migrar (en filesDir)
        try {
            val dbFile = appContext.getDatabasePath(DATABASE_NAME)
            val backupFile = java.io.File(appContext.filesDir, "${DATABASE_NAME}_backup_${System.currentTimeMillis()}.db")
            dbFile.copyTo(backupFile, overwrite = true)
            android.util.Log.i("DB", "Backup de la BD creado en: ${backupFile.path}")
        } catch (e: Exception) {
            android.util.Log.w("DB", "No se pudo crear backup de la BD antes de migrar: ${e.message}")
        }

        // Helper local para ejecutar y loggear SQL con contexto
        fun runSql(sql: String) {
            android.util.Log.d("DB", "Ejecutando SQL de migración: $sql")
            try {
                db.execSQL(sql)
            } catch (ex: Exception) {
                android.util.Log.e("DB", "Fallo al ejecutar SQL en migración: $sql", ex)
                throw ex
            }
        }

        // Desactivar temporalmente la comprobación de claves foráneas para permitir copiar filas en cualquier orden
        try {
            // Usar API de Android para deshabilitar (más fiable que ejecutar PRAGMA manualmente)
            db.setForeignKeyConstraintsEnabled(false)
            android.util.Log.d("DB", "Foreign key constraints DISABLED for migration")
        } catch (e: Exception) {
            android.util.Log.w("DB", "No se pudo deshabilitar FK via setForeignKeyConstraintsEnabled: ${e.message}")
            try {
                db.execSQL("PRAGMA foreign_keys = OFF")
            } catch (_: Exception) {
                // Ignorar si tampoco funciona
            }
        }

        db.beginTransaction()
        try {
            // Para cada tabla con INTEGER PRIMARY KEY AUTOINCREMENT antigua, creamos una tabla temporal nueva (sin AUTOINCREMENT), copiamos datos y renombramos.

            // PRODUCTOR
            android.util.Log.d("DB", "Migrando tabla PRODUCTOR")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_PRODUCTOR}_new ($COL_PROD_ID INTEGER PRIMARY KEY, $COL_PROD_CODIGO TEXT NOT NULL, $COL_PROD_NOMBRE TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_PRODUCTOR}_new($COL_PROD_ID, $COL_PROD_CODIGO, $COL_PROD_NOMBRE) SELECT $COL_PROD_ID, $COL_PROD_CODIGO, $COL_PROD_NOMBRE FROM $TABLE_PRODUCTOR")
            runSql("DROP TABLE IF EXISTS $TABLE_PRODUCTOR")
            runSql("ALTER TABLE ${TABLE_PRODUCTOR}_new RENAME TO $TABLE_PRODUCTOR")

            // CODIGO_SAG
            android.util.Log.d("DB", "Migrando tabla CODIGO_SAG")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_CODIGO_SAG}_new ($COL_SAG_ID INTEGER PRIMARY KEY, $COL_SAG_CODIGO_SAG TEXT NOT NULL, $COL_SAG_COD_SDP_SAG TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_CODIGO_SAG}_new($COL_SAG_ID,$COL_SAG_CODIGO_SAG,$COL_SAG_COD_SDP_SAG) SELECT $COL_SAG_ID,$COL_SAG_CODIGO_SAG,$COL_SAG_COD_SDP_SAG FROM $TABLE_CODIGO_SAG")
            runSql("DROP TABLE IF EXISTS $TABLE_CODIGO_SAG")
            runSql("ALTER TABLE ${TABLE_CODIGO_SAG}_new RENAME TO $TABLE_CODIGO_SAG")

            // VARIEDAD
            android.util.Log.d("DB", "Migrando tabla VARIEDAD")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_VARIEDAD}_new ($COL_VAR_ID INTEGER PRIMARY KEY, $COL_VAR_NOMBRE TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_VARIEDAD}_new($COL_VAR_ID,$COL_VAR_NOMBRE) SELECT $COL_VAR_ID,$COL_VAR_NOMBRE FROM $TABLE_VARIEDAD")
            runSql("DROP TABLE IF EXISTS $TABLE_VARIEDAD")
            runSql("ALTER TABLE ${TABLE_VARIEDAD}_new RENAME TO $TABLE_VARIEDAD")

            // CUARTEL
            android.util.Log.d("DB", "Migrando tabla CUARTEL")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_CUARTEL}_new ($COL_CUA_ID INTEGER PRIMARY KEY, $COL_CUA_NUM TEXT NOT NULL, $COL_CUA_NOMBRE TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_CUARTEL}_new($COL_CUA_ID,$COL_CUA_NUM,$COL_CUA_NOMBRE) SELECT $COL_CUA_ID,$COL_CUA_NUM,$COL_CUA_NOMBRE FROM $TABLE_CUARTEL")
            runSql("DROP TABLE IF EXISTS $TABLE_CUARTEL")
            runSql("ALTER TABLE ${TABLE_CUARTEL}_new RENAME TO $TABLE_CUARTEL")

            // EMBALAJE
            android.util.Log.d("DB", "Migrando tabla EMBALAJE")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_EMBALAJE}_new ($COL_EMB_ID INTEGER PRIMARY KEY, $COL_EMB_CODIGO TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_EMBALAJE}_new($COL_EMB_ID,$COL_EMB_CODIGO) SELECT $COL_EMB_ID,$COL_EMB_CODIGO FROM $TABLE_EMBALAJE")
            runSql("DROP TABLE IF EXISTS $TABLE_EMBALAJE")
            runSql("ALTER TABLE ${TABLE_EMBALAJE}_new RENAME TO $TABLE_EMBALAJE")

            // ETIQUETA
            android.util.Log.d("DB", "Migrando tabla ETIQUETA")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_ETIQUETA}_new ($COL_ETI_ID INTEGER PRIMARY KEY, $COL_ETI_NOMBRE TEXT NOT NULL, $COL_ETI_NOMBRE_IMAGEN TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_ETIQUETA}_new($COL_ETI_ID,$COL_ETI_NOMBRE,$COL_ETI_NOMBRE_IMAGEN) SELECT $COL_ETI_ID,$COL_ETI_NOMBRE,$COL_ETI_NOMBRE_IMAGEN FROM $TABLE_ETIQUETA")
            runSql("DROP TABLE IF EXISTS $TABLE_ETIQUETA")
            runSql("ALTER TABLE ${TABLE_ETIQUETA}_new RENAME TO $TABLE_ETIQUETA")

            // LOGO
            android.util.Log.d("DB", "Migrando tabla LOGO")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_LOGO}_new ($COL_LOGO_ID INTEGER PRIMARY KEY, $COL_LOGO_NOM_COD TEXT NOT NULL, $COL_LOGO_NOMBRE TEXT NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_LOGO}_new($COL_LOGO_ID,$COL_LOGO_NOM_COD,$COL_LOGO_NOMBRE) SELECT $COL_LOGO_ID,$COL_LOGO_NOM_COD,$COL_LOGO_NOMBRE FROM $TABLE_LOGO")
            runSql("DROP TABLE IF EXISTS $TABLE_LOGO")
            runSql("ALTER TABLE ${TABLE_LOGO}_new RENAME TO $TABLE_LOGO")

            // PLU
            android.util.Log.d("DB", "Migrando tabla PLU")
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_PLU}_new ($COL_PLU_ID INTEGER PRIMARY KEY, $COL_PLU_CODE INTEGER NOT NULL, $COL_PLU_DESCRIPTION TEXT)")
            runSql("INSERT OR IGNORE INTO ${TABLE_PLU}_new($COL_PLU_ID,$COL_PLU_CODE,$COL_PLU_DESCRIPTION) SELECT $COL_PLU_ID,$COL_PLU_CODE,$COL_PLU_DESCRIPTION FROM $TABLE_PLU")
            runSql("DROP TABLE IF EXISTS $TABLE_PLU")
            runSql("ALTER TABLE ${TABLE_PLU}_new RENAME TO $TABLE_PLU")

            // VARIEDAD_PLU (clave compuesta) — recrear y copiar
            android.util.Log.d("DB", "Migrando tabla VARIEDAD_PLU")
            // Crear sin FOREIGN KEY para evitar chequear constraints durante la migración
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_VARIEDAD_PLU}_new ($COL_VP_VARIEDAD_ID INTEGER NOT NULL, $COL_VP_PLU_ID INTEGER NOT NULL, PRIMARY KEY ($COL_VP_VARIEDAD_ID, $COL_VP_PLU_ID))")
            runSql("INSERT OR IGNORE INTO ${TABLE_VARIEDAD_PLU}_new($COL_VP_VARIEDAD_ID,$COL_VP_PLU_ID) SELECT $COL_VP_VARIEDAD_ID,$COL_VP_PLU_ID FROM $TABLE_VARIEDAD_PLU")
            runSql("DROP TABLE IF EXISTS $TABLE_VARIEDAD_PLU")
            runSql("ALTER TABLE ${TABLE_VARIEDAD_PLU}_new RENAME TO $TABLE_VARIEDAD_PLU")

            // TRAZABILIDAD
            android.util.Log.d("DB", "Migrando tabla TRAZABILIDAD")
            // Crear sin FOREIGN KEY para evitar chequear constraints durante la migración
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_TRAZABILIDAD}_new ($COL_TRAZ_ID INTEGER PRIMARY KEY, $COL_TRAZ_PRODUCTOR_ID INTEGER NOT NULL, $COL_TRAZ_CODIGO_SAG_ID INTEGER NOT NULL, $COL_TRAZ_VARIEDAD_ID INTEGER NOT NULL, $COL_TRAZ_CUARTEL_ID INTEGER NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_TRAZABILIDAD}_new($COL_TRAZ_ID,$COL_TRAZ_PRODUCTOR_ID,$COL_TRAZ_CODIGO_SAG_ID,$COL_TRAZ_VARIEDAD_ID,$COL_TRAZ_CUARTEL_ID) SELECT $COL_TRAZ_ID,$COL_TRAZ_PRODUCTOR_ID,$COL_TRAZ_CODIGO_SAG_ID,$COL_TRAZ_VARIEDAD_ID,$COL_TRAZ_CUARTEL_ID FROM $TABLE_TRAZABILIDAD")
            runSql("DROP TABLE IF EXISTS $TABLE_TRAZABILIDAD")
            runSql("ALTER TABLE ${TABLE_TRAZABILIDAD}_new RENAME TO $TABLE_TRAZABILIDAD")

            // ENCABEZADO y DETALLE
            android.util.Log.d("DB", "Migrando tablas ENCABEZADO y DETALLE")
            // Crear sin FOREIGN KEY para evitar chequear constraints durante la migración
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_ENCABEZADO}_new ($COL_ENC_NUM_TARJA INTEGER PRIMARY KEY, $COL_ENC_NUM_PALLET INTEGER, $COL_ENC_FECHA TEXT NOT NULL, $COL_ENC_EMBALAJE_ID INTEGER NOT NULL, $COL_ENC_ETIQUETA_ID INTEGER NOT NULL, $COL_ENC_VARIEDAD TEXT NOT NULL, $COL_ENC_RECIBIDOR TEXT, $COL_ENC_LOGO_NOM TEXT NOT NULL, $COL_ENC_PROC_PROD INTEGER, $COL_ENC_PROC_COM INTEGER, $COL_ENC_PLU INTEGER, $COL_ENC_STATUS TEXT NOT NULL DEFAULT 'pendiente')")
            runSql("INSERT OR IGNORE INTO ${TABLE_ENCABEZADO}_new SELECT * FROM $TABLE_ENCABEZADO")
            runSql("DROP TABLE IF EXISTS $TABLE_ENCABEZADO")
            runSql("ALTER TABLE ${TABLE_ENCABEZADO}_new RENAME TO $TABLE_ENCABEZADO")

            // Crear DETALLE sin FK para migración; la integridad referencial será responsabilidad de la app tras migrar
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_DETALLE}_new ($COL_DET_ID INTEGER PRIMARY KEY, $COL_DET_NUM_TARJA INTEGER NOT NULL, $COL_DET_FOLIO INTEGER, $COL_DET_CSG TEXT, $COL_DET_LOTE TEXT, $COL_DET_SDP TEXT, $COL_DET_LINEA TEXT, $COL_DET_CATEGORIA TEXT, $COL_DET_CANTIDAD INTEGER NOT NULL)")
            runSql("INSERT OR IGNORE INTO ${TABLE_DETALLE}_new SELECT * FROM $TABLE_DETALLE")
            runSql("DROP TABLE IF EXISTS $TABLE_DETALLE")
            runSql("ALTER TABLE ${TABLE_DETALLE}_new RENAME TO $TABLE_DETALLE")

            // Actualizar sqlite_sequence con los máximos actuales para evitar colisiones en futuras inserciones automáticas
            updateSqliteSequenceIfNeeded(db, TABLE_PRODUCTOR, COL_PROD_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_CODIGO_SAG, COL_SAG_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_VARIEDAD, COL_VAR_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_CUARTEL, COL_CUA_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_EMBALAJE, COL_EMB_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_ETIQUETA, COL_ETI_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_LOGO, COL_LOGO_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_PLU, COL_PLU_ID)
            updateSqliteSequenceIfNeeded(db, TABLE_TRAZABILIDAD, COL_TRAZ_ID)

            db.setTransactionSuccessful()
        } catch (e: Exception) {
            // Log detallado y relanzar para que la app lo capture (evita fallos silenciosos)
            android.util.Log.e("DB", "migratePreserveIds fallo: ${e.message}", e)
            throw e
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
            // Volver a activar la comprobación de claves foráneas
            try {
                db.setForeignKeyConstraintsEnabled(true)
                android.util.Log.d("DB", "Foreign key constraints ENABLED after migration")
            } catch (e: Exception) {
                android.util.Log.w("DB", "No se pudo reactivar FK via setForeignKeyConstraintsEnabled: ${e.message}")
                try {
                    db.execSQL("PRAGMA foreign_keys = ON")
                } catch (_: Exception) {
                }
            }
        }
    }

    private fun createAllTables(db: SQLiteDatabase) {
        db.execSQL("CREATE TABLE $TABLE_PRODUCTOR ($COL_PROD_ID INTEGER PRIMARY KEY, $COL_PROD_CODIGO TEXT NOT NULL, $COL_PROD_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_CODIGO_SAG ($COL_SAG_ID INTEGER PRIMARY KEY, $COL_SAG_CODIGO_SAG TEXT NOT NULL, $COL_SAG_COD_SDP_SAG TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_VARIEDAD ($COL_VAR_ID INTEGER PRIMARY KEY, $COL_VAR_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_CUARTEL ($COL_CUA_ID INTEGER PRIMARY KEY, $COL_CUA_NUM TEXT NOT NULL, $COL_CUA_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_EMBALAJE ($COL_EMB_ID INTEGER PRIMARY KEY, $COL_EMB_CODIGO TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_ETIQUETA ($COL_ETI_ID INTEGER PRIMARY KEY, $COL_ETI_NOMBRE TEXT NOT NULL, $COL_ETI_NOMBRE_IMAGEN TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_LOGO ($COL_LOGO_ID INTEGER PRIMARY KEY, $COL_LOGO_NOM_COD TEXT NOT NULL, $COL_LOGO_NOMBRE TEXT NOT NULL)")
        db.execSQL("CREATE TABLE $TABLE_PLU ($COL_PLU_ID INTEGER PRIMARY KEY, $COL_PLU_CODE INTEGER NOT NULL, $COL_PLU_DESCRIPTION TEXT)")

        db.execSQL("CREATE TABLE $TABLE_VARIEDAD_PLU ($COL_VP_VARIEDAD_ID INTEGER NOT NULL, $COL_VP_PLU_ID INTEGER NOT NULL, PRIMARY KEY ($COL_VP_VARIEDAD_ID, $COL_VP_PLU_ID))")

        db.execSQL("CREATE TABLE $TABLE_TRAZABILIDAD ($COL_TRAZ_ID INTEGER PRIMARY KEY, $COL_TRAZ_PRODUCTOR_ID INTEGER NOT NULL, $COL_TRAZ_CODIGO_SAG_ID INTEGER NOT NULL, $COL_TRAZ_VARIEDAD_ID INTEGER NOT NULL, $COL_TRAZ_CUARTEL_ID INTEGER NOT NULL)")

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
                 $COL_ENC_STATUS TEXT NOT NULL DEFAULT 'pendiente'
             ) """
         db.execSQL(createEncabezado)

         val createDetalle = """CREATE TABLE $TABLE_DETALLE (
                $COL_DET_ID INTEGER PRIMARY KEY,
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

         db.execSQL("CREATE TABLE $TABLE_METADATA ($COL_META_KEY TEXT PRIMARY KEY, $COL_META_VALUE TEXT)")
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

    // Ajusta sqlite_sequence para la tabla si existe y si el max(Id) es mayor que el value actual
    private fun updateSqliteSequenceIfNeeded(db: SQLiteDatabase, tableName: String, idCol: String) {
        try {
            val cursor = db.rawQuery("SELECT seq FROM sqlite_sequence WHERE name = ?", arrayOf(tableName))
            var currentSeq: Long = -1
            if (cursor.moveToFirst()) currentSeq = cursor.getLong(0)
            cursor.close()

            val cursorMax = db.rawQuery("SELECT MAX($idCol) FROM $tableName", null)
            var maxId: Long = -1
            if (cursorMax.moveToFirst()) maxId = cursorMax.getLong(0)
            cursorMax.close()

            if (maxId > currentSeq) {
                db.execSQL("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES(?, ?)", arrayOf<Any>(tableName, maxId))
            }
        } catch (_: Exception) {
            // no fatal
        }
    }

    // --- Metadata helpers ---
    fun getMetadata(db: SQLiteDatabase, key: String): String? {
        // Ensure metadata table exists (should be created in onCreate)
        return try {
            val cursor = db.query(TABLE_METADATA, arrayOf(COL_META_VALUE), "$COL_META_KEY = ?", arrayOf(key), null, null, null)
            val value: String? = if (cursor.moveToFirst()) {
                val idx = cursor.getColumnIndex(COL_META_VALUE)
                if (idx != -1) cursor.getString(idx) else null
            } else null
            cursor.close()
            value
        } catch (e: Exception) {
            null
        }
    }

    fun setMetadata(db: SQLiteDatabase, key: String, value: String) {
        try {
            val cv = ContentValues()
            cv.put(COL_META_KEY, key)
            cv.put(COL_META_VALUE, value)
            db.insertWithOnConflict(TABLE_METADATA, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
        } catch (e: Exception) {
            // ignore
        }
    }

    /** Comprueba si existe un registro en `tableName` donde `whereColumn = whereValue`. */
    fun recordExists(db: SQLiteDatabase, tableName: String, whereColumn: String, whereValue: String): Boolean {
        val cursor = db.query(tableName, arrayOf("1"), "$whereColumn = ?", arrayOf(whereValue), null, null, null)
        val exists = cursor.moveToFirst()
        cursor.close()
        return exists
    }

    fun forceMigratePreserveIds() {
        // Método de depuración para forzar la migración desde código (no se usa en producción salvo pruebas).
        try {
            val db = writableDatabase
            migratePreserveIds(db)
        } catch (e: Exception) {
            // registrar y continuar
            android.util.Log.w("DB", "forceMigratePreserveIds fallo: ${e.message}")
        }
    }

}
