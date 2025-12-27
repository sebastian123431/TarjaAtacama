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
        private const val DATABASE_VERSION = 15

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

        // Nueva tabla PROCEDENCIA_PROD y sus columnas
        const val TABLE_PROCEDENCIA_PROD = "PROCEDENCIA_PROD"
        const val COL_PROC_PROD_ID = "Id"
        const val COL_PROC_PROD_NOMBRE = "nombre"
        const val COL_PROC_PROD_CODIGO = "codigo"

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
                android.util.Log.e(
                    "DB",
                    "migratePreserveIds fallo durante onUpgrade, intentando fallback seguro: ${e.message}",
                    e
                )
                try {
                    // Cerrar la conexión actual y borrar el archivo físico de la BD para recrearlo limpio.
                    try {
                        db.close()
                    } catch (_: Exception) {
                    }
                    val deleted = appContext.deleteDatabase(DATABASE_NAME)
                    android.util.Log.w("DB", "Borrado fichero BD para fallback onUpgrade: $deleted")
                    // Forzar recreación abriendo una nueva conexión (esto ejecutará onCreate)
                    try {
                        val newDb = writableDatabase
                        newDb.close()
                        android.util.Log.i(
                            "DB",
                            "BD recreada correctamente tras fallback de onUpgrade"
                        )
                    } catch (recreateEx: Exception) {
                        android.util.Log.e(
                            "DB",
                            "No se pudo recrear la BD tras borrar fichero: ${recreateEx.message}",
                            recreateEx
                        )
                    }
                } catch (_: Exception) {
                    // ignore
                }
                // No llamar a onCreate aquí porque ya intentamos recrearla arriba; si no se recreó, la app seguirá intentando
            }
        } else {
            // fallback: recrear tablas
            // Intentamos el mismo fallback seguro: cerrar, borrar fichero y recrear
            try {
                try {
                    db.close()
                } catch (_: Exception) {
                }
                val deleted = appContext.deleteDatabase(DATABASE_NAME)
                android.util.Log.w(
                    "DB",
                    "Borrado fichero BD para fallback onUpgrade (else branch): $deleted"
                )
                try {
                    val newDb = writableDatabase
                    newDb.close()
                } catch (recreateEx: Exception) {
                    android.util.Log.e(
                        "DB",
                        "No se pudo recrear la BD tras borrar fichero (else branch): ${recreateEx.message}",
                        recreateEx
                    )
                }
            } catch (_: Exception) {
            }
        }

        // Asegurar que la nueva tabla PROCEDENCIA_PROD existe y está poblada en upgrades sin destruir datos
        try {
            db.execSQL("CREATE TABLE IF NOT EXISTS $TABLE_PROCEDENCIA_PROD ($COL_PROC_PROD_ID INTEGER PRIMARY KEY, $COL_PROC_PROD_NOMBRE TEXT NOT NULL, $COL_PROC_PROD_CODIGO TEXT NOT NULL)")
            // Llamamos a la rutina de poblamiento; usa INSERT OR REPLACE así no elimina registros adicionales existentes
            prepopulateProcedenciaProd(db)
        } catch (e: Exception) {
            android.util.Log.w("DB", "No se pudo asegurar PROCEDENCIA_PROD en onUpgrade: ${e.message}")
        }
    }

    // Migra las tablas para permitir insertar Ids explícitos preservando los datos.
    private fun migratePreserveIds(db: SQLiteDatabase) {
        // Crear copia de seguridad local de la BD antes de migrar (en filesDir)
        try {
            val dbFile = appContext.getDatabasePath(DATABASE_NAME)
            val backupFile = java.io.File(
                appContext.filesDir,
                "${DATABASE_NAME}_backup_${System.currentTimeMillis()}.db"
            )
            dbFile.copyTo(backupFile, overwrite = true)
            android.util.Log.i("DB", "Backup de la BD creado en: ${backupFile.path}")
        } catch (e: Exception) {
            android.util.Log.w(
                "DB",
                "No se pudo crear backup de la BD antes de migrar: ${e.message}"
            )
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
            android.util.Log.w(
                "DB",
                "No se pudo deshabilitar FK via setForeignKeyConstraintsEnabled: ${e.message}"
            )
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
            runSql("CREATE TABLE IF NOT EXISTS ${TABLE_ENCABEZADO}_new ($COL_ENC_NUM_TARJA INTEGER PRIMARY KEY, $COL_ENC_NUM_PALLET INTEGER, $COL_ENC_FECHA TEXT NOT NULL, $COL_ENC_EMBALAJE_ID INTEGER NOT NULL, $COL_ENC_ETIQUETA_ID INTEGER NOT NULL, $COL_ENC_VARIEDAD TEXT NOT NULL, $COL_ENC_RECIBIDOR TEXT, $COL_ENC_LOGO_NOM TEXT NOT NULL, $COL_ENC_PROC_PROD INTEGER, $COL_ENC_PROC_COM INTEGER, $COL_ENC_PLU INTEGER, $COL_ENC_STATUS TEXT NOT NULL DEFAULT 'pendiente', FOREIGN KEY($COL_ENC_PROC_PROD) REFERENCES $TABLE_PROCEDENCIA_PROD($COL_PROC_PROD_ID))")
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
            // Asegurar secuencia para la nueva tabla PROCEDENCIA_PROD
            updateSqliteSequenceIfNeeded(db, TABLE_PROCEDENCIA_PROD, COL_PROC_PROD_ID)

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
                android.util.Log.w(
                    "DB",
                    "No se pudo reactivar FK via setForeignKeyConstraintsEnabled: ${e.message}"
                )
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
        // Crear la tabla PROCEDENCIA_PROD
        db.execSQL("CREATE TABLE $TABLE_PROCEDENCIA_PROD ($COL_PROC_PROD_ID INTEGER PRIMARY KEY, $COL_PROC_PROD_NOMBRE TEXT NOT NULL, $COL_PROC_PROD_CODIGO TEXT NOT NULL)")
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
                 $COL_ENC_STATUS TEXT NOT NULL DEFAULT 'pendiente',
                 FOREIGN KEY($COL_ENC_PROC_PROD) REFERENCES $TABLE_PROCEDENCIA_PROD($COL_PROC_PROD_ID)
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

        // Evitar error si la tabla metadata ya existe
        db.execSQL("CREATE TABLE IF NOT EXISTS $TABLE_METADATA ($COL_META_KEY TEXT PRIMARY KEY, $COL_META_VALUE TEXT)")
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
        prepopulateTrazabilidad(db) // ahora poblamos CODIGOS_TRAZABILIDAD también
        prepopulateEtiquetas(db) // poblar tabla ETIQUETA
        prepopulateLogos(db) // poblar tabla LOGO
        // Poblar PROCEDENCIA_PROD sin tocar ningunos inserts existentes
        prepopulateProcedenciaProd(db)
    }

    // Ajusta sqlite_sequence para la tabla si existe y si el max(Id) es mayor que el value actual

    private fun updateSqliteSequenceIfNeeded(db: SQLiteDatabase, tableName: String, idCol: String) {
        try {
            val cursor =
                db.rawQuery("SELECT seq FROM sqlite_sequence WHERE name = ?", arrayOf(tableName))
            var currentSeq: Long = -1
            if (cursor.moveToFirst()) currentSeq = cursor.getLong(0)
            cursor.close()

            val cursorMax = db.rawQuery("SELECT MAX($idCol) FROM $tableName", null)
            var maxId: Long = -1
            if (cursorMax.moveToFirst()) maxId = cursorMax.getLong(0)
            cursorMax.close()

            if (maxId > currentSeq) {
                db.execSQL(
                    "INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES(?, ?)",
                    arrayOf<Any>(tableName, maxId)
                )
            }
        } catch (_: Exception) {
            // No hacemos nada si falla; es sólo un ajuste de secuencia
        }
    }

    private fun prepopulateProductores(db: SQLiteDatabase) {
        // Insertar lista de productores (Id, cod_productor, nom_productor)
        data class Prod(val id: Int, val codigo: String, val nombre: String)
        val productores = listOf(
            Prod(4011, "401", "HUANCARA"),
            Prod(4022, "402", "MAITENCILLO"),
            Prod(4033, "403", "SANTA BERNARDITA"),
            Prod(4044, "404", "SANTA ADRIANA"),
            Prod(4055, "405", "EL TAMBITO"),
            Prod(4086, "408", "LA COMPAÑÍA"),
            Prod(4107, "410", "LOS PIMIENTOS")
        )

        db.beginTransaction()
        try {
            for (p in productores) {
                val cv = ContentValues()
                cv.put(COL_PROD_ID, p.id)
                cv.put(COL_PROD_CODIGO, p.codigo)
                cv.put(COL_PROD_NOMBRE, p.nombre)
                db.insertWithOnConflict(TABLE_PRODUCTOR, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try { db.endTransaction() } catch (_: Exception) {}
        }
    }

    private fun prepopulateCodigoSag(db: SQLiteDatabase) {
        // Insertar lista de códigos SAG proporcionada (Id, codigo_sag, cod_sdp_sag)
        val codigos = listOf(
            Triple(4011, 106757, 60248),
            Triple(4012, 106757, 62235),
            Triple(4013, 106757, 60249),
            Triple(4014, 106757, 60250),
            Triple(4015, 106757, 62236),
            Triple(4016, 106757, 60251),
            Triple(4017, 106757, 62237),
            Triple(4018, 106757, 60252),
            Triple(4019, 106757, 60253),
            Triple(40110, 106757, 60254),
            Triple(40111, 106757, 60255),
            Triple(40112, 106757, 62238),
            Triple(40213, 106958, 60243),
            Triple(40214, 106958, 60244),
            Triple(40215, 106958, 62239),
            Triple(40216, 106958, 60245),
            Triple(40217, 106958, 62240),
            Triple(40218, 106958, 60246),
            Triple(40219, 106958, 62241),
            Triple(40220, 106958, 63615),
            Triple(40221, 106958, 60247),
            Triple(40322, 106955, 60234),
            Triple(40323, 106955, 60235),
            Triple(40324, 106955, 60236),
            Triple(40325, 106955, 60237),
            Triple(40326, 106955, 60238),
            Triple(40327, 106955, 60239),
            Triple(40328, 106955, 60264),
            Triple(40329, 106955, 60240),
            Triple(40330, 106955, 60241),
            Triple(40331, 106955, 60242),
            Triple(40432, 106956, 60224),
            Triple(40433, 106956, 60225),
            Triple(40434, 106956, 60226),
            Triple(40435, 106956, 62243),
            Triple(40436, 106956, 60227),
            Triple(40437, 106956, 62244),
            Triple(40438, 106956, 60228),
            Triple(40439, 106956, 60229),
            Triple(40440, 106956, 62245),
            Triple(40441, 106956, 60230),
            Triple(40442, 106956, 60231),
            Triple(40443, 106956, 62247),
            Triple(40444, 106956, 62323),
            Triple(40445, 106956, 60232),
            Triple(40446, 106956, 60233),
            Triple(40547, 119072, 49510),
            Triple(40848, 87197, 60256),
            Triple(40849, 87197, 62232),
            Triple(40850, 87197, 62233),
            Triple(40851, 87197, 60257),
            Triple(40852, 87197, 60258),
            Triple(40853, 87197, 62234),
            Triple(40854, 87197, 62321),
            Triple(40855, 87197, 60259),
            Triple(40856, 87197, 62322),
            Triple(41057, 89323, 60260),
            Triple(41058, 89323, 60261),
            Triple(41059, 89323, 60262),
            Triple(41060, 89323, 62231),
            Triple(41061, 89323, 60263)
        )

        db.beginTransaction()
        try {
            for ((id, codigoSag, codSdp) in codigos) {
                val cv = ContentValues()
                cv.put(COL_SAG_ID, id)
                // En la definición la columna de codigo_sag es TEXT, así que insertamos como String
                cv.put(COL_SAG_CODIGO_SAG, codigoSag.toString())
                cv.put(COL_SAG_COD_SDP_SAG, codSdp.toString())
                db.insertWithOnConflict(TABLE_CODIGO_SAG, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }

    private fun prepopulateVariedades(db: SQLiteDatabase) {
        // Insertar lista de variedades (Id, nom_variedad)
        data class Var(val id: Int, val nombre: String)
        val variedades = listOf(
            Var(1, "ARRA 15"),
            Var(2, "SUGRAFIFTYFOUR (APPLAUSE)"),
            Var(3, "ARDTHITYFIVE (FIRE CRUNCH)"),
            Var(4, "GREAT GREEN"),
            Var(5, "KRISSY"),
            Var(6, "SWEET GLOBE"),
            Var(7, "TIMCO"),
            Var(8, "TIMPSON"),
            Var(9, "SWEET CELEBRATION"),
            Var(10, "ALLISON"),
            Var(11, "ARRA 29"),
            Var(12, "INIAGRAPE-ONE (MAYLEN)"),
            Var(13, "SUGRAFIFTYTHREE (RUBY RUSH)"),
            Var(14, "SWEET FAVORS"),
            Var(15, "AUTUMN CRISP"),
            Var(16, "CANDY HEARTS")
        )

        db.beginTransaction()
        try {
            for (v in variedades) {
                val cv = ContentValues()
                cv.put(COL_VAR_ID, v.id)
                cv.put(COL_VAR_NOMBRE, v.nombre)
                db.insertWithOnConflict(TABLE_VARIEDAD, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try { db.endTransaction() } catch (_: Exception) {}
        }
    }

    private fun prepopulateCuarteles(db: SQLiteDatabase) {
        // Insertar lista de cuarteles basada en assets/import/Cuartel.csv
        data class Cuart(val id: Int, val num: Int, val nombre: String)

        val cuarteles = listOf(
            Cuart(4011, 1, "El Bosque"),
            Cuart(4012, 1, "sector 1 A"),
            Cuart(4013, 2, "Sector 2 A"),
            Cuart(4014, 3, "Sector 3 B"),
            Cuart(4015, 4, "Sector 4"),
            Cuart(4016, 5, "Cuartel 5"),
            Cuart(4017, 6, "Añañuca 3"),
            Cuart(4018, 7, "Calixto"),
            Cuart(4019, 8, "Sector 1 B"),
            Cuart(40120, 9, "Los Hornos"),
            Cuart(40121, 9, "Sector 2 B"),
            Cuart(40122, 10, "Añañuca 1"),
            Cuart(40123, 10, "El Calixto"),
            Cuart(40124, 11, "Mapa"),
            Cuart(40125, 11, "Añañuca 3"),
            Cuart(40126, 12, "Burdeo"),
            Cuart(40127, 13, "La Vega"),
            Cuart(40128, 14, "Sector 3A"),
            Cuart(40129, 15, "La Cancha"),
            Cuart(40130, 16, "Calixto A"),
            Cuart(40131, 17, "Calixto B"),
            Cuart(40222, 1, "La Manga"),
            Cuart(40223, 2, "Ladera Grande"),
            Cuart(40224, 3, "Trinagulo"),
            Cuart(40225, 3, "Cuartel 3"),
            Cuart(40226, 4, "Estanque"),
            Cuart(40227, 5, "Bodega"),
            Cuart(40228, 6, "Ladera Chica"),
            Cuart(40229, 7, "Ex Sultanita Vieja"),
            Cuart(40230, 72, "Tina"),
            Cuart(40231, 73, "Tina 1"),
            Cuart(40232, 74, "Tina 2"),
            Cuart(40233, 75, "La Tina"),
            Cuart(40234, 76, "Tina Isla"),
            Cuart(40335, 37, "Rinconada 1"),
            Cuart(40336, 38, "Rinconada 2"),
            Cuart(40337, 39, "Viña San Jose"),
            Cuart(40338, 40, "San Jose 1"),
            Cuart(40339, 41, "San Jose 2"),
            Cuart(40340, 42, "San Jose 3"),
            Cuart(40341, 43, "Lourdes 1"),
            Cuart(40342, 44, "Lourdes 2"),
            Cuart(40343, 45, "Lourdes 3"),
            Cuart(40344, 48, "San Steve"),
            Cuart(40345, 50, "Maria 1"),
            Cuart(40346, 51, "Maria 2"),
            Cuart(40347, 57, "Viña del Cerro"),
            Cuart(40348, 58, "Las Pircas"),
            Cuart(40349, 59, "Grande 1"),
            Cuart(40350, 60, "Grande 2"),
            Cuart(40351, 64, "Provenir 1"),
            Cuart(40352, 65, "Provenir 2"),
            Cuart(40353, 66, "Provenir 3"),
            Cuart(40354, 67, "Provenir 4"),
            Cuart(40355, 69, "Italia"),
            Cuart(40356, 72, "Allison 1"),
            Cuart(40357, 73, "Allison 2"),
            Cuart(40458, 1, "chile 1"),
            Cuart(40459, 2, "chile 2"),
            Cuart(40460, 3, "chile 3"),
            Cuart(40461, 4, "Fernando 1"),
            Cuart(40462, 5, "Fernando 2"),
            Cuart(40463, 6, "America 1"),
            Cuart(40464, 7, "America 2"),
            Cuart(40465, 8, "America 3"),
            Cuart(40466, 9, "America 4"),
            Cuart(40467, 10, "Los Loros"),
            Cuart(40468, 11, "El Redondo"),
            Cuart(40469, 12, "Palestina 1"),
            Cuart(40470, 13, "Palestina 2"),
            Cuart(40471, 14, "Palestina 3"),
            Cuart(40472, 15, "Palestina 4"),
            Cuart(40473, 16, "Palestina 5"),
            Cuart(40474, 17, "Maquina 1"),
            Cuart(40475, 18, "Maquina 2"),
            Cuart(40476, 19, "Ondonada 1"),
            Cuart(40477, 20, "Ondonada 2"),
            Cuart(40478, 22, "San Ralph"),
            Cuart(40479, 23, "Blanco 1"),
            Cuart(40480, 24, "Blanco 2"),
            Cuart(40481, 25, "San Matias "),
            Cuart(40482, 26, "San Jose Domingo"),
            Cuart(40483, 27, "San Nicolas"),
            Cuart(40484, 28, "San Hernan"),
            Cuart(40485, 29, "Santa Ana"),
            Cuart(40486, 30, "San Agustin"),
            Cuart(40487, 31, "San Guillermo 1"),
            Cuart(40488, 32, "San Guillermo 2"),
            Cuart(40489, 33, "San Guillermo 3"),
            Cuart(40490, 34, "El Rojo"),
            Cuart(40491, 35, "Black 1"),
            Cuart(40492, 36, "Black 2"),
            Cuart(40493, 46, "Lourdes 1"),
            Cuart(40494, 47, "Lourdes 2"),
            Cuart(40495, 49, "Santa Florencia"),
            Cuart(40496, 54, "San Benjamin"),
            Cuart(40497, 55, "San Joaquín "),
            Cuart(40498, 56, "San Sebastián "),
            Cuart(40499, 61, "Grande 1"),
            Cuart(404100, 62, "Grande 2"),
            Cuart(404101, 63, "Grande 3"),
            Cuart(404102, 68, "Porvenir"),
            Cuart(405103, 1, "1"),
            Cuart(405104, 2, "2"),
            Cuart(408105, 1, "1"),
            Cuart(408106, 2, "2"),
            Cuart(408107, 3, "3"),
            Cuart(408108, 4, "4"),
            Cuart(408109, 5, "5"),
            Cuart(408110, 6, "6"),
            Cuart(408111, 7, "7"),
            Cuart(408112, 8, "8"),
            Cuart(408113, 9, "9"),
            Cuart(408114, 10, "10"),
            Cuart(408115, 11, "11"),
            Cuart(408116, 12, "12"),
            Cuart(408117, 13, "13"),
            Cuart(408118, 14, "14"),
            Cuart(408119, 15, "15"),
            Cuart(408120, 16, "16"),
            Cuart(408121, 17, "17"),
            Cuart(408122, 18, "18"),
            Cuart(408123, 19, "19"),
            Cuart(408124, 20, "20"),
            Cuart(410125, 1, "1"),
            Cuart(410126, 2, "2"),
            Cuart(410127, 3, "3"),
            Cuart(410128, 4, "4"),
            Cuart(410129, 5, "5"),
            Cuart(410130, 6, "6"),
            Cuart(410131, 7, "7"),
            Cuart(410132, 8, "8"),
            Cuart(410133, 9, "9"),
            Cuart(410134, 10, "10")
        )

        db.beginTransaction()
        try {
            for (c in cuarteles) {
                val cv = ContentValues()
                cv.put(COL_CUA_ID, c.id)
                cv.put(COL_CUA_NUM, c.num.toString())
                cv.put(COL_CUA_NOMBRE, c.nombre)
                db.insertWithOnConflict(TABLE_CUARTEL, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }

    private fun prepopulateTrazabilidad(db: SQLiteDatabase) {
        // Insertar la lista de trazabilidad (Id, productor_id, codigo_sag_id, variedad_id, cuartel_id)
        data class Traz(
            val id: Int,
            val productorId: Int,
            val codigoSagId: Int,
            val variedadId: Int,
            val cuartelId: Int
        )

        val trazs = listOf(
            Traz(1, 4011, 4011, 8, 4011),
            Traz(2, 4011, 4014, 1, 4012),
            Traz(3, 4011, 4016, 7, 4013),
            Traz(4, 4011, 4016, 7, 4014),
            Traz(5, 4011, 4015, 1, 4015),
            Traz(6, 4011, 4013, 3, 4016),
            Traz(7, 4011, 40110, 7, 4017),
            Traz(8, 4011, 40111, 2, 4018),
            Traz(9, 4011, 4014, 1, 4019),
            Traz(10, 4011, 4012, 9, 40110),
            Traz(11, 4011, 4016, 7, 40111),
            Traz(12, 4011, 40110, 7, 40112),
            Traz(13, 4011, 40112, 4, 40113),
            Traz(14, 4011, 4012, 9, 40114),
            Traz(15, 4011, 40110, 7, 40115),
            Traz(16, 4011, 4018, 7, 40116),
            Traz(17, 4011, 4012, 9, 40117),
            Traz(18, 4011, 4017, 5, 40118),
            Traz(19, 4011, 4017, 5, 40119),
            Traz(20, 4011, 4019, 6, 40120),
            Traz(21, 4011, 4019, 6, 40121),
            Traz(22, 4022, 40213, 11, 40222),
            Traz(23, 4022, 40213, 11, 40223),
            Traz(24, 4022, 40218, 5, 40224),
            Traz(25, 4022, 40220, 13, 40225),
            Traz(26, 4022, 40214, 12, 40226),
            Traz(27, 4022, 40214, 12, 40227),
            Traz(28, 4022, 40215, 12, 40228),
            Traz(29, 4022, 40215, 12, 40229),
            Traz(30, 4022, 40214, 5, 40230),
            Traz(31, 4022, 40216, 10, 40231),
            Traz(32, 4022, 40217, 10, 40232),
            Traz(33, 4022, 40221, 1, 40233),
            Traz(34, 4022, 40221, 1, 40234),
            Traz(35, 4033, 40322, 7, 40337),
            Traz(36, 4033, 40322, 7, 40338),
            Traz(37, 4033, 40324, 8, 40339),
            Traz(38, 4033, 40324, 8, 40340),
            Traz(39, 4033, 40324, 8, 40341),
            Traz(40, 4033, 40325, 2, 40342),
            Traz(41, 4033, 40323, 7, 40343),
            Traz(42, 4033, 40323, 7, 40344),
            Traz(43, 4033, 40323, 7, 40345),
            Traz(44, 4033, 40323, 7, 40348),
            Traz(45, 4033, 40328, 6, 40350),
            Traz(46, 4033, 40328, 6, 40351),
            Traz(47, 4033, 40325, 2, 40357),
            Traz(48, 4033, 40324, 7, 40358),
            Traz(49, 4033, 40325, 2, 40359),
            Traz(50, 4033, 40325, 2, 40360),
            Traz(51, 4033, 40330, 13, 40364),
            Traz(52, 4033, 40330, 13, 40365),
            Traz(53, 4033, 40331, 14, 40366),
            Traz(54, 4033, 40331, 14, 40367),
            Traz(55, 4033, 40327, 11, 40369),
            Traz(56, 4033, 40326, 10, 40372),
            Traz(57, 4033, 40326, 10, 40373),
            Traz(58, 4044, 40432, 15, 40474),
            Traz(59, 4044, 40432, 15, 40475),
            Traz(60, 4044, 40432, 15, 40476),
            Traz(61, 4044, 40432, 15, 40477),
            Traz(62, 4044, 40432, 15, 40478),
            Traz(63, 4044, 40434, 16, 40479),
            Traz(64, 4044, 40435, 16, 40480),
            Traz(65, 4044, 40434, 16, 40481),
            Traz(66, 4044, 40435, 16, 40482),
            Traz(67, 4044, 40432, 15, 40483),
            Traz(68, 4044, 40438, 6, 40484),
            Traz(69, 4044, 40433, 15, 40485),
            Traz(70, 4044, 40433, 15, 40486),
            Traz(71, 4044, 40433, 15, 40487),
            Traz(72, 4044, 40433, 15, 40488),
            Traz(73, 4044, 40440, 8, 40489),
            Traz(74, 4044, 40437, 9, 40490),
            Traz(75, 4044, 40437, 9, 40491),
            Traz(76, 4044, 40438, 6, 40492),
            Traz(77, 4044, 40438, 6, 40493),
            Traz(78, 4044, 40445, 9, 40494),
            Traz(79, 4044, 40439, 9, 40495),
            Traz(80, 4044, 40439, 9, 40496),
            Traz(81, 4044, 40443, 8, 40497),
            Traz(82, 4044, 40443, 8, 40498),
            Traz(83, 4044, 40443, 8, 40499),
            Traz(84, 4044, 40444, 8, 404100),
            Traz(85, 4044, 40444, 8, 404101),
            Traz(86, 4044, 40444, 8, 404102),
            Traz(87, 4044, 40436, 10, 404103),
            Traz(88, 4044, 40436, 10, 404104),
            Traz(89, 4044, 40436, 10, 404105),
            Traz(90, 4044, 40439, 6, 404106),
            Traz(91, 4044, 40439, 6, 404107),
            Traz(92, 4044, 40439, 6, 404108),
            Traz(93, 4044, 40441, 6, 404109),
            Traz(94, 4044, 40441, 6, 404110),
            Traz(95, 4044, 40441, 6, 404111),
            Traz(96, 4044, 40446, 8, 404112),
            Traz(97, 4044, 40446, 8, 404113),
            Traz(98, 4044, 40446, 8, 404114),
            Traz(99, 4044, 40442, 6, 404115),
            Traz(100, 4044, 40442, 6, 404116),
            Traz(101, 4044, 40442, 6, 404117),
            Traz(102, 4044, 40442, 6, 404118),
            Traz(103, 4055, 40547, 2, 405103),
            Traz(104, 4055, 40547, 2, 405104),
            Traz(105, 4086, 40849, 6, 408105),
            Traz(106, 4086, 40848, 9, 408106),
            Traz(107, 4086, 40848, 9, 408107),
            Traz(108, 4086, 40848, 9, 408108),
            Traz(109, 4086, 40850, 6, 408109),
            Traz(110, 4086, 40850, 6, 408110),
            Traz(111, 4086, 40851, 6, 408111),
            Traz(112, 4086, 40852, 5, 408112),
            Traz(113, 4086, 40852, 5, 408113),
            Traz(114, 4086, 40853, 5, 408114),
            Traz(115, 4086, 40853, 5, 408115),
            Traz(116, 4086, 40853, 5, 408116),
            Traz(117, 4086, 40854, 5, 408117),
            Traz(118, 4086, 40854, 5, 408118),
            Traz(119, 4086, 40854, 5, 408119),
            Traz(120, 4086, 40855, 15, 408120),
            Traz(121, 4086, 40855, 15, 408121),
            Traz(122, 4086, 40856, 15, 408122),
            Traz(123, 4086, 40856, 15, 408123),
            Traz(124, 4086, 40849, 6, 408124),
            Traz(125, 4107, 41057, 6, 410125),
            Traz(126, 4107, 41057, 6, 410126),
            Traz(127, 4107, 41058, 6, 410127),
            Traz(128, 4107, 41058, 6, 410128),
            Traz(129, 4107, 41059, 6, 410129),
            Traz(130, 4107, 41059, 6, 410130),
            Traz(131, 4107, 41059, 6, 410131),
            Traz(132, 4107, 41060, 6, 410132),
            Traz(133, 4107, 41060, 6, 410133),
            Traz(134, 4107, 41061, 6, 410134)
        )

        db.beginTransaction()
        try {
            for (t in trazs) {
                val cv = ContentValues()
                cv.put(COL_TRAZ_ID, t.id)
                cv.put(COL_TRAZ_PRODUCTOR_ID, t.productorId)
                cv.put(COL_TRAZ_CODIGO_SAG_ID, t.codigoSagId)
                cv.put(COL_TRAZ_VARIEDAD_ID, t.variedadId)
                cv.put(COL_TRAZ_CUARTEL_ID, t.cuartelId)
                db.insertWithOnConflict(
                    TABLE_TRAZABILIDAD,
                    null,
                    cv,
                    SQLiteDatabase.CONFLICT_REPLACE
                )
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }

    private fun prepopulateEmbalajes(db: SQLiteDatabase) {
        // Insertar lista de embalajes (Id, codigo)
        data class Emb(val id: Int, val codigo: String)

        val embalajes = listOf(
            Emb(1, "CB4"),
            Emb(2, "CB5"),
            Emb(3, "CB81"),
            Emb(4, "CB81B"),
            Emb(5, "CB81A"),
            Emb(6, "CB81D"),
            Emb(7, "CB82"),
            Emb(8, "CB82D"),
            Emb(9, "CB82B"),
            Emb(10, "CB82WD"),
            Emb(11, "CC5"),
            Emb(12, "CC5D"),
            Emb(13, "CC17"),
            Emb(14, "CC18G"),
            Emb(15, "CC18R"),
            Emb(16, "CC18B"),
            Emb(17, "CC18CP"),
            Emb(18, "CC18SG"),
            Emb(19, "CC18SR"),
            Emb(20, "CC19"),
            Emb(21, "CP5"),
            Emb(22, "PB8"),
            Emb(23, "PB8R"),
            Emb(24, "MB8"),
            Emb(25, "BSUSG"),
            Emb(26, "BZUALDI"),
            Emb(27, "BSUCH"),
            Emb(28, "BSUDR"),
            Emb(29, "BSUGF"),
            Emb(30, "BZUNF"),
            Emb(31, "BZUHB"),
            Emb(32, "BSUBI"),
            Emb(33, "BSURD")
        )

        db.beginTransaction()
        try {
            for (e in embalajes) {
                val cv = ContentValues()
                cv.put(COL_EMB_ID, e.id)
                cv.put(COL_EMB_CODIGO, e.codigo)
                db.insertWithOnConflict(TABLE_EMBALAJE, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }

    // Nueva función para poblar PROCEDENCIA_PROD con los pares proporcionados por el usuario
    private fun prepopulateProcedenciaProd(db: SQLiteDatabase) {
        data class ProcProd(val id: Int, val nombre: String, val codigo: String)
        val items = listOf(
            ProcProd(1, "Packing Viña del Cerro", "107131"),
            ProcProd(2, "Packing Nantoco", "107130"),
            ProcProd(3, "Packing Las Compuertas", "107132"),
            ProcProd(4, "Packing Huancara", "107134"),
            ProcProd(5, "Packing Maitencillo", "107135"),
            ProcProd(6, "Frigorifico Nantoco", "87618"),
            ProcProd(7, "Packing Diaguitas", "117471"),
            ProcProd(8, "Frutícola y Exportadora ATACAMA Ltda.", "87620")
        )

        db.beginTransaction()
        try {
            for (p in items) {
                val cv = ContentValues()
                cv.put(COL_PROC_PROD_ID, p.id)
                cv.put(COL_PROC_PROD_NOMBRE, p.nombre)
                cv.put(COL_PROC_PROD_CODIGO, p.codigo)
                db.insertWithOnConflict(TABLE_PROCEDENCIA_PROD, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try { db.endTransaction() } catch (_: Exception) {}
        }
    }

    private fun prepopulatePlu(db: SQLiteDatabase) {
        // Insertar lista de PLU (Id, plu_code, description) provista por el usuario
        data class Plu(val id: Int, val pluCode: Int, val description: String)

        val plus = listOf(
            Plu(1, 4022, "Red Seedless"),
            Plu(2, 3497, "Arra 29"),
            Plu(3, 3505, "Arra 35"),
            Plu(4, 3491, "Krissy"),
            Plu(5, 3490, "Timco"),
            Plu(6, 3502, "Allison"),
            Plu(7, 3506, "Candy Hearts"),
            Plu(8, 3494, "Sweet Celebration"),
            Plu(9, 4023, "Green Seedless"),
            Plu(10, 4638, "Sugraone"),
            Plu(11, 4498, "Prime Seedless"),
            Plu(12, 3493, "Arra 15"),
            Plu(13, 3492, "Autumn Crisp"),
            Plu(14, 3489, "Timpson"),
            Plu(15, 3501, "Great Green"),
            Plu(16, 3503, "Ivory"),
            Plu(17, 3496, "Sweet Globe"),
            Plu(18, 3507, "Sweet Favors"),
            Plu(19, 3498, "Maylen"),
            Plu(20, 4056, "Black Seedless"),
            Plu(21, 4636, "Autumn Royal"),
            Plu(22, 4637, "Red Globe"),
            Plu(23, 3500, "Ruby Rush"),
            Plu(24, 3495, "Applause")
        )

        db.beginTransaction()
        try {
            for (p in plus) {
                val cv = ContentValues()
                cv.put(COL_PLU_ID, p.id)
                cv.put(COL_PLU_CODE, p.pluCode)
                cv.put(COL_PLU_DESCRIPTION, p.description)
                // Usar CONFLICT_REPLACE para mantener id explícito y no duplicar
                db.insertWithOnConflict(TABLE_PLU, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try { db.endTransaction() } catch (_: Exception) {}
        }
    }

    private fun prepopulateVariedadPlu(db: SQLiteDatabase) {
        // Insertar pares variedad_id <-> plu_id (proporcionados en assets/import/VARIEDAD_PLU.csv)
        data class Vp(val variedadId: Int, val pluId: Int)
        val relaciones = listOf(
            Vp(1,1), Vp(2,2), Vp(3,3), Vp(4,4), Vp(5,5), Vp(6,6), Vp(7,7), Vp(8,8), Vp(9,9), Vp(10,10),
            Vp(11,11), Vp(12,12), Vp(13,13), Vp(14,14), Vp(15,15), Vp(16,16), Vp(17,17), Vp(18,18), Vp(19,19), Vp(20,20),
            Vp(21,21), Vp(22,22), Vp(33,23), Vp(39,24), Vp(23,12), Vp(24,10), Vp(25,15), Vp(26,17), Vp(27,5), Vp(28,8),
            Vp(30,6), Vp(31,2), Vp(34,4), Vp(35,18), Vp(36,13), Vp(37,7), Vp(38,14)
        )

        db.beginTransaction()
        try {
            for (r in relaciones) {
                val cv = ContentValues()
                cv.put(COL_VP_VARIEDAD_ID, r.variedadId)
                cv.put(COL_VP_PLU_ID, r.pluId)
                // Usar CONFLICT_REPLACE para no duplicar y permitir actualizaciones seguras
                db.insertWithOnConflict(TABLE_VARIEDAD_PLU, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try { db.endTransaction() } catch (_: Exception) {}
        }
    }

    private fun <T> insertInTransaction(
        db: SQLiteDatabase,
        tableName: String,
        populate: (ContentValues, T) -> Unit,
        items: List<T>
    ) {
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

    private fun getColumnId(
        db: SQLiteDatabase,
        tableName: String,
        columnId: String,
        whereColumn: String,
        whereValue: String
    ): Long {
        var id: Long = -1
        val cursor = db.query(
            tableName,
            arrayOf(columnId),
            "$whereColumn = ?",
            arrayOf(whereValue),
            null,
            null,
            null
        )
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

    private fun prepopulateLogos(db: SQLiteDatabase) {
        // Insertar lista de logos (Id, nom_cod, nombre)
        data class L(val id: Int, val nomCod: String, val nombre: String)

        val logos = listOf(
            L(1, "BP", "Bags (polipropileno)"),
            L(2, "PPZ", "Zipper polipropileno"),
            L(3, "PPS", "Slider polipropileno"),
            L(4, "PG15", "Punnet (10x500 grs.)"),
            L(5, "CL15", "Clamshell (10x500 grs.)"),
            L(6, "CL27", "Clamshell 2 libras (2 x 8)  7,3 kg."),
            L(7, "CL29", "Clamshell 2 libras (2 x 10)  9,1 kg."),
            L(8, "CL38C", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(9, "CL38S", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(10, "CL38D", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(11, "CL38GF", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(12, "CL38KR", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(13, "CL38W", "Clamshell 3 libras (3 x 6) 8,2 kg."),
            L(14, "CL38BI", "Clamshell 3 libras (3 x 6) 8,2 kg. BICOLOR"),
            L(15, "BSUO1", "Laminado slider Clear Bag (SIN GRAFICA) - Sin Logo"),
            L(16, "BSUO2", "Laminado slider Clear Bag (SIN GRAFICA) - Sin Logo"),
            L(17, "BSUG2", "Laminado Slider Stand Up  - Grafica Nueva"),
            L(18, "BSUSW", "Laminado Slider Sweeties ( ARRA 15)"),
            L(19, "BSUPF", "Laminado Slider Passion Fire ( ARRA 29)"),
            L(20, "BZUG1", "Laminado zipper Stand Up Grafica Antigua"),
            L(21, "BZUG2", "Laminado zipper Stand Up Grafica Nueva"),
            L(22, "BZUO1", "Laminado zipper Clear Bag (SIN GRAFICA) - Sin Logo"),
            L(23, "BZUTI", "Laminado zipper Stand Up Grafica  ( TIMPSON )"),
            L(24, "BSUSC", "Laminado Slider Stand Up Grafica  ( SWEET CELEBRATION )"),
            L(25, "BSUSF", "Laminado Slider Stand Up Grafica  ( SWEET FAVORS )"),
            L(26, "BSUSS", "Laminado Slider Stand Up Grafica  ( SWEET SAPPHIRE )"),
            L(27, "BZUAC", "Laminado zipper Stand Up Grafica  ( AUTUMN CRISP )"),
            L(28, "BSUAC", "Laminado Slider Stand Up Grafica  ( AUTUMN CRISP )"),
            L(29, "BSUSG", "Laminado Slider Stand Up Grafica  ( SWEET GLOBE )"),
            L(30, "BZUALDI", "Laminado zipper Stand Up Grafica ALDI (GOIN APE )"),
            L(31, "BSUCH", "Laminado Slider Stand Up Grafica  ( CANDY HEARTS )"),
            L(32, "BSUDR", "Laminado Slider Stand Up Grafica ( DULCINEA RED )"),
            L(33, "BSUGF", "Laminado Slider Stand Up Clear ( GRAPEMAN FARMS )"),
            L(34, "BZUNF", "Laminado zipper Stand Up Grafica (NATURES FINEST )"),
            L(35, "BZUHB", "Laminado zipper Stand Up Grafica (HEB )"),
            L(36, "BSUBI", "Laminado Slider Stand Up Grafica ( BICOLOR )"),
            L(37, "BSURD", "Laminado Slider Stand Up Grafica (RAZZLE DAZZLE )")
        )

        db.beginTransaction()
        try {
            for (l in logos) {
                val cv = ContentValues()
                cv.put(COL_LOGO_ID, l.id)
                cv.put(COL_LOGO_NOM_COD, l.nomCod)
                cv.put(COL_LOGO_NOMBRE, l.nombre)
                db.insertWithOnConflict(TABLE_LOGO, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }

    private fun prepopulateEtiquetas(db: SQLiteDatabase) {
        // Insertar lista de etiquetas (Id, nombre, nombre_imagen)
        data class Etiq(val id: Int, val nombre: String, val nombreImagen: String)

        val etiquetas = listOf(
            Etiq(1, "KOPKE", "etiqueta_atacama"),
            Etiq(2, "E - MART", "etiqueta_atacama"),
            Etiq(3, "SIERRA PRODUCE", "etiqueta_carolina"),
            Etiq(4, "PACIFIC TRELLIS", "etiqueta_produce_kountry"),
            Etiq(5, "WAL MART", "etiqueta_francisca"),
            Etiq(6, "GIUMARRA", "etiqueta_atc"),
            Etiq(7, "GRAPEMAN FARMS", "etiqueta_grapeman_farms"),
            Etiq(8, "GRAPECO", "etiqueta_grapeco"),
            Etiq(9, "SPREAFICO-ITALIA", "etiqueta_valle_verde"),
            Etiq(10, "KINGO 1", "etiqueta_globo"),
            Etiq(11, "KINGO 2", "etiqueta_n1_grapes"),
            Etiq(12, "WING MAU 1", "etiqueta_joy_tree"),
            Etiq(13, "WING MAU 2", "etiqueta_bernandita"),
            Etiq(14, "DALIAN 1", "etiqueta_truenjoy_1"),
            Etiq(15, "DALIAN 2", "etiqueta_truenjoy_2"),
            Etiq(16, "SUNWOO", "etiqueta_wild_tiger"),
            Etiq(17, "UMINA", "etiqueta_n1_grapes"),
            Etiq(18, "KPC", "etiqueta_valle_nantoco")
        )

        db.beginTransaction()
        try {
            for (e in etiquetas) {
                val cv = ContentValues()
                cv.put(COL_ETI_ID, e.id)
                cv.put(COL_ETI_NOMBRE, e.nombre)
                cv.put(COL_ETI_NOMBRE_IMAGEN, e.nombreImagen)
                db.insertWithOnConflict(TABLE_ETIQUETA, null, cv, SQLiteDatabase.CONFLICT_REPLACE)
            }
            db.setTransactionSuccessful()
        } finally {
            try {
                db.endTransaction()
            } catch (_: Exception) {
            }
        }
    }
}
