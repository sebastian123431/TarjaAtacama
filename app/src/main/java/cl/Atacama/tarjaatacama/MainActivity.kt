package cl.Atacama.tarjaatacama

import android.content.Intent
import android.os.Bundle
import android.view.View
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat

/**
 * Actividad principal de la aplicación.
 * Funciona como un menú de inicio con dos opciones principales.
 */
class MainActivity : AppCompatActivity() {

    /**
     * Función principal que se llama al crear la actividad.
     */
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        // Ajusta el padding de la vista principal para que no se solape con las barras del sistema.
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main)) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }
    }

    /**
     * Se ejecuta al presionar el botón "Ingresar".
     * Inicia el flujo para crear una nueva tarja, navegando a la pantalla de encabezado.
     */
    fun ingresar(view: View) {
        val intent = Intent(this, encabezado_tarja::class.java)
        startActivity(intent)
    }

    /**
     * Se ejecuta al presionar el botón "Ver Tarjas Ingresadas".
     * Navega a la pantalla que muestra el historial de tarjas.

     */
    fun ver_tarjas_ingresadas (view: View) {
        val intent = Intent(this, tarjas_ingresadas::class.java)
        startActivity(intent)
    }
}