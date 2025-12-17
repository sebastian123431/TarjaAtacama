 package cl.Atacama.tarjaatacama.util

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Receiver debug para iniciar importación mediante `adb shell am broadcast -a cl.Atacama.tarjaatacama.IMPORT_CSV`
 * Opcional: pasar extra `path` con la ruta absoluta a la carpeta en el dispositivo.
 */
class CsvImportReceiver : BroadcastReceiver() {
    companion object {
        const val ACTION_IMPORT = "cl.Atacama.tarjaatacama.IMPORT_CSV"
        private const val TAG = "CsvImportReceiver"
        const val EXTRA_PATH = "path"
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != ACTION_IMPORT) return
        val path = intent.getStringExtra(EXTRA_PATH)
        Log.i(TAG, "Received import broadcast. path=$path")
        // Ejecutar en background
        CoroutineScope(Dispatchers.IO).launch {
            val result = if (path.isNullOrBlank()) CsvImportRunner.runImportFromDefaultFolder(context) else CsvImporter.importFromFolder(context, path)
            Log.i(TAG, "Import result: \n$result")
        }
    }
}

