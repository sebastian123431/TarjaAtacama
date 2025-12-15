package cl.Atacama.tarjaatacama.adapter

import android.content.res.ColorStateList
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.core.content.ContextCompat
import androidx.recyclerview.widget.RecyclerView
import cl.Atacama.tarjaatacama.R
import cl.Atacama.tarjaatacama.databinding.ItemMoldeTarjaIngresadaBinding
import cl.Atacama.tarjaatacama.modelo.Encabezado
import java.text.SimpleDateFormat
import java.util.Locale

/**
 * Adaptador para el RecyclerView que muestra la lista de tarjas ingresadas (encabezados).
 * Se encarga de tomar la lista de datos y estructurar cada elemento en la pantalla.
 */
class TarjasIngresadasAdapter(
    // La lista de encabezados que se va a mostrar.
    private val encabezados: List<Encabezado>,
    // Función que se ejecutará cuando se presione el botón "Editar".
    private val onEdit: (Encabezado) -> Unit,
    // Función que se ejecutará cuando se presione el botón "Eliminar".
    private val onDelete: (Encabezado) -> Unit
) : RecyclerView.Adapter<TarjasIngresadasAdapter.EncabezadoViewHolder>() {

    /**
     * Se llama cuando el RecyclerView necesita crear una nueva fila (ViewHolder).
     * Infla el layout del item y crea una instancia del ViewHolder.
     */
    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): EncabezadoViewHolder {
        val binding = ItemMoldeTarjaIngresadaBinding.inflate(LayoutInflater.from(parent.context), parent, false)
        return EncabezadoViewHolder(binding)
    }

    /**
     * Se llama para mostrar los datos en una fila (ViewHolder) específica.
     * Conecta los datos del objeto Encabezado con las vistas del layout.
     */
    override fun onBindViewHolder(holder: EncabezadoViewHolder, position: Int) {
        val encabezado = encabezados[position]
        holder.bind(encabezado)
    }

    /**
     * Devuelve el número total de elementos en la lista.
     */
    override fun getItemCount() = encabezados.size

    /**
     * Clase interna que representa una única fila de la lista.
     * Contiene las referencias a las vistas del layout y el método para llenarlas.
     */
    inner class EncabezadoViewHolder(private val binding: ItemMoldeTarjaIngresadaBinding) : RecyclerView.ViewHolder(binding.root) {
        
        /**
         * Rellena las vistas del layout con los datos de un objeto Encabezado.
         * @param encabezado El objeto con los datos a mostrar.
         */
        fun bind(encabezado: Encabezado) {
            binding.valueNTarja.text = encabezado.numTarja.toString()
            binding.valuePallet.text = encabezado.numPallet.toString()
            
            // Formatea la fecha de "yyyy-MM-dd" (BD) a "dd-MM-yyyy" (UI) para una mejor lectura.
            try {
                val dbFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
                val displayFormat = SimpleDateFormat("dd-MM-yyyy", Locale.getDefault())
                val date = dbFormat.parse(encabezado.fechaEmbalaje)
                binding.valueFecha.text = displayFormat.format(date)
            } catch (e: Exception) {
                binding.valueFecha.text = encabezado.fechaEmbalaje // Si falla el formateo, muestra la fecha tal cual.
            }

            binding.valueProd.text = encabezado.procProd.toString()
            binding.valueCom.text = encabezado.procCom.toString()
            binding.valuePlu.text = encabezado.plu.toString()
            binding.valueTotalCajas.text = encabezado.totalCajas.toString()

            // --- Lógica de UI basada en el estado ---
            val context = binding.root.context
            if (encabezado.status == "enviado") {
                // Si está "enviado", el indicador es verde y se oculta el botón de eliminar.
                binding.valueStatus.text = "Enviado"
                binding.valueStatus.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(context, R.color.atacama_green))
                binding.buttonDelete.visibility = View.GONE
            } else { 
                // Si está "pendiente", el indicador es naranja y se muestra el botón de eliminar.
                binding.valueStatus.text = "Pendiente"
                binding.valueStatus.backgroundTintList = ColorStateList.valueOf(ContextCompat.getColor(context, android.R.color.holo_orange_dark))
                binding.buttonDelete.visibility = View.VISIBLE
            }

            // Asigna las acciones a los botones de esta fila específica.
            binding.buttonEdit.setOnClickListener { onEdit(encabezado) }
            binding.buttonDelete.setOnClickListener { onDelete(encabezado) }
        }
    }
}
