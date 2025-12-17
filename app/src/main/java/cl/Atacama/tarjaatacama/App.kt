package cl.Atacama.tarjaatacama

import android.app.Application
import android.util.Log
import cl.Atacama.tarjaatacama.util.CsvImportRunner

class App : Application() {
    companion object {
        private const val TAG = "App"
    }

    override fun onCreate() {
        super.onCreate()
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

