package cl.Atacama.tarjaatacama

import android.app.Application
import android.util.Log
import cl.Atacama.tarjaatacama.util.CsvImportRunner
import cl.Atacama.tarjaatacama.db.DB

class App : Application() {
    companion object {
        private const val TAG = "App"
    }

    override fun onCreate() {
        super.onCreate()
        // Asegurarnos de que la base de datos esté inicializada / migrada antes de ejecutar la importación
        try {
            val dbHelper = DB(this)
            // Forzar apertura que ejecuta onCreate/onUpgrade si corresponde
            val db = dbHelper.writableDatabase
            db.close()
            Log.i(TAG, "DB inicializada correctamente en Application.onCreate")
        } catch (e: Exception) {
            Log.w(TAG, "No se pudo inicializar la BD antes de la importación: ${e.message}")
            // continuamos de todas formas; el import se ejecutará y el DB helper intentará migrar también
        }
        // Ejecutar importación en background para no bloquear el hilo UI
        Thread {
            try {
                val result = CsvImportRunner.runImportFromDefaultFolder(this)
                Log.i(TAG, "CSV import result: $result")
            } catch (e: Exception) {
                Log.e(TAG, "Error running CSV import: ${e.message}")
            }
        }.start()
    }
}
