package cl.Atacama.tarjaatacama.adapter

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import cl.Atacama.tarjaatacama.databinding.ItemResumenTarjaCardBinding
import cl.Atacama.tarjaatacama.modelo.Tarja

/**
 * Adaptador para el RecyclerView que muestra la lista de detalles de una tarja.
 */
class ResumenTarjaAdapter(
    // La lista de detalles que se va a mostrar.
    private val tarjas: List<Tarja>,
    // Función que se ejecutará al presionar el botón "Editar".
    private val onEdit: (Tarja) -> Unit,
    // Función que se ejecutará al presionar el botón "Eliminar".
    private val onDelete: (Tarja) -> Unit
) : RecyclerView.Adapter<ResumenTarjaAdapter.TarjaViewHolder>() {

    /**
     * Se llama cuando el RecyclerView necesita crear una nueva fila.
     */
    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): TarjaViewHolder {
        val binding = ItemResumenTarjaCardBinding.inflate(LayoutInflater.from(parent.context), parent, false)
        return TarjaViewHolder(binding)
    }

    /**
     * Se llama para mostrar los datos en una fila específica.
     */
    override fun onBindViewHolder(holder: TarjaViewHolder, position: Int) {
        val tarja = tarjas[position]
        holder.bind(tarja)
    }

    /**
     * Devuelve el número total de elementos en la lista.
     */
    override fun getItemCount() = tarjas.size

    /**
     * Clase interna que representa una única fila de la lista de detalles.
     */
    inner class TarjaViewHolder(private val binding: ItemResumenTarjaCardBinding) : RecyclerView.ViewHolder(binding.root) {
        
        /**
         * Rellena las vistas del layout con los datos de un objeto Tarja (detalle).
         * @param tarja El objeto con los datos a mostrar.
         */
        fun bind(tarja: Tarja) {
            binding.valueFolio.text = tarja.folio.toString()
            binding.valueCsg.text = tarja.csg
            binding.valueLote.text = tarja.lote
            binding.valueSdp.text = tarja.sdp
            binding.valueLinea.text = tarja.linea
            binding.valueCategoria.text = tarja.categoria
            binding.valueCajas.text = tarja.cajas.toString()

            // Asigna las acciones a los botones de esta fila.
            binding.buttonEdit.setOnClickListener { onEdit(tarja) }
            binding.buttonDelete.setOnClickListener { onDelete(tarja) }
        }
    }
}
