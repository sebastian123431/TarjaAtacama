package cl.Atacama.tarjaatacama.adapter

import android.content.Context
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
import android.widget.Filter
import android.widget.ImageView
import android.widget.TextView
import cl.Atacama.tarjaatacama.R
import cl.Atacama.tarjaatacama.modelo.Etiqueta

class EtiquetaSpinnerAdapter(context: Context, etiquetas: List<Etiqueta>) :
    ArrayAdapter<Etiqueta>(context, 0, etiquetas) {

    override fun getView(position: Int, convertView: View?, parent: ViewGroup): View {
        return createView(position, convertView, parent)
    }

    override fun getDropDownView(position: Int, convertView: View?, parent: ViewGroup): View {
        return createView(position, convertView, parent)
    }

    // La versión original usaba `override` pero en esta plataforma la clase base no declara
    // `convertResultToString`. Quitamos `override` para evitar el error de compilación
    fun convertResultToString(resultValue: Any?): CharSequence {
        return (resultValue as? Etiqueta)?.nombre ?: ""
    }

    private fun createView(position: Int, convertView: View?, parent: ViewGroup): View {
        val view = convertView ?: LayoutInflater.from(context)
            .inflate(R.layout.item_etiqueta_spinner, parent, false)

        val etiqueta = getItem(position)

        val imageView = view.findViewById<ImageView>(R.id.image_etiqueta_spinner)
        val textView = view.findViewById<TextView>(R.id.text_etiqueta_spinner)

        etiqueta?.let {
            // Mostrar solo la imagen ("pura etiqueta") en el selector
            textView.visibility = View.GONE
            // Cargar drawable desde resources usando imagenUri (contiene el nombre del drawable)
            try {
                var resName = it.imagenUri
                // remover extensión si existe, normalizar a nombre válido de recurso
                resName = resName.substringBeforeLast('.').trim().replace(Regex("[^a-zA-Z0-9_]"), "_").lowercase()
                if (resName.isNotBlank()) {
                    val resId = context.resources.getIdentifier(resName, "drawable", context.packageName)
                    if (resId != 0) {
                        imageView.setImageResource(resId)
                    } else {
                        // Si no existe drawable, limpiar la imagen
                        imageView.setImageDrawable(null)
                    }
                } else {
                    imageView.setImageDrawable(null)
                }
            } catch (ex: Exception) {
                // Fallback: limpiar la imagen para evitar mostrar un URI inválido
                imageView.setImageDrawable(null)
            }
        }

        return view
    }
}