package cl.Atacama.tarjaatacama.adapter

import android.content.Context
import android.net.Uri
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
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

    private fun createView(position: Int, convertView: View?, parent: ViewGroup): View {
        val view = convertView ?: LayoutInflater.from(context)
            .inflate(R.layout.item_etiqueta_spinner, parent, false)

        val etiqueta = getItem(position)

        val imageView = view.findViewById<ImageView>(R.id.image_etiqueta_spinner)
        val textView = view.findViewById<TextView>(R.id.text_etiqueta_spinner)

        etiqueta?.let {
            textView.text = it.nombre
            imageView.setImageURI(Uri.parse(it.imagenUri))
        }

        return view
    }
}
