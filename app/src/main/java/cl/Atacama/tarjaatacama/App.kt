package cl.Atacama.tarjaatacama

import android.app.Application
import android.util.Log
import cl.Atacama.tarjaatacama.db.DB

// Nota: la importación CSV ya no se ejecuta automáticamente en onCreate porque
// puede abrir la base de datos y provocar migraciones o conflictos en instalaciones
// existentes. Usa el BroadcastReceiver `CsvImportReceiver` o llama manualmente a
// CsvImportRunner cuando quieras ejecutar la importación.

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

        // No ejecutar import automáticamente. Para ejecutar manualmente desde adb:
        // adb shell am broadcast -a cl.Atacama.tarjaatacama.IMPORT_CSV
    }
}
