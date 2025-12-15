package cl.Atacama.tarjaatacama

import android.content.Intent
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import cl.Atacama.tarjaatacama.adapter.ResumenTarjaAdapter
import cl.Atacama.tarjaatacama.controller.TarjaController
import cl.Atacama.tarjaatacama.databinding.ActivityResumenTarjaBinding
import cl.Atacama.tarjaatacama.modelo.Tarja

/**
 * Actividad para ver, agregar, editar y eliminar los detalles de una tarja específica.
 * También permite enviar la tarja, cambiando su estado a "enviado".
 */
class Resumen_tarja : AppCompatActivity() {

    private lateinit var binding: ActivityResumenTarjaBinding
    private lateinit var tarjaController: TarjaController

    // Lista que contiene los detalles de la tarja actual.
    private val detallesList = mutableListOf<Tarja>()
    private lateinit var adapter: ResumenTarjaAdapter
    // Guarda el detalle que se está editando. Si es null, se está creando uno nuevo.
    private var editingTarja: Tarja? = null
    // Almacena el N° de Tarja que se está gestionando en esta pantalla.
    private var currentNumTarja: Int = -1

    /**
     * Función principal que se llama al crear la actividad.
     */
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        binding = ActivityResumenTarjaBinding.inflate(layoutInflater)
        setContentView(binding.root)

        ViewCompat.setOnApplyWindowInsetsListener(binding.main) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }

        tarjaController = TarjaController(this)

        // Recibe el N° de Tarja desde la actividad anterior.
        currentNumTarja = intent.getIntExtra("NUM_TARJA", -1)
        if (currentNumTarja == -1) {
            Toast.makeText(this, "Error: N° de Tarja no encontrado.", Toast.LENGTH_LONG).show()
            finish() // Si no hay N° de Tarja, cierra la actividad para evitar errores.
            return
        }

        // Configuración inicial.
        setupRecyclerView()
        setupClickListeners()
        setupBackButton()
        // Carga los detalles de la tarja desde la BD.
        loadDetalles()
    }

    /**
     * Configura el RecyclerView que muestra la lista de detalles.
     */
    private fun setupRecyclerView() {
        adapter = ResumenTarjaAdapter(detallesList,
            { tarja -> // onEdit
                editingTarja = tarja
                showForm()
                binding.buttonAgregarTabla.text = "Editar"
                // Rellena el formulario con los datos del detalle a editar.
                binding.editTextNumeroFolio.setText(tarja.folio.toString())
                binding.editTextProductorCsg.setText(tarja.csg)
                binding.editTextLote.setText(tarja.lote)
                binding.editTextSdp.setText(tarja.sdp)
                binding.editTextLineaProceso.setText(tarja.linea)
                binding.editTextCategoria.setText(tarja.categoria)
                binding.editTextCantidadCajas.setText(tarja.cajas.toString())
            },
            { tarja -> // onDelete
                showDeleteConfirmationDialog(tarja)
            }
        )
        binding.recyclerViewResumen.adapter = adapter
    }

    /**
     * Asigna las acciones a todos los botones de la pantalla.
     */
    private fun setupClickListeners() {
        // Botón para ir a editar el encabezado.
        binding.buttonEditHeader.setOnClickListener {
            val intent = Intent(this, encabezado_tarja::class.java)
            intent.putExtra("NUM_TARJA", currentNumTarja) // Envía el N° de Tarja a la pantalla de encabezado.
            startActivity(intent)
        }

        // Botón flotante para agregar un nuevo detalle.
        binding.fabAddNew.setOnClickListener {
            editingTarja = null // Se asegura de que no esté en modo edición.
            showForm()
            binding.buttonAgregarTabla.text = "Agregar"
        }

        binding.buttonAgregarTabla.setOnClickListener { handleSave() } // Botón para guardar (agregar o editar).
        binding.buttonCancelar.setOnClickListener { hideForm() } // Botón para cancelar.
        binding.buttonEnviar.setOnClickListener { showSendConfirmationDialog() } // Botón para enviar la tarja.
    }

    /**
     * Gestiona el guardado de un detalle (ya sea nuevo o editado).
     */
    private fun handleSave() {
        if (!validateDetailFields()) {
            return // Detiene si la validación falla.
        }

        // Recoge los datos del formulario.
        val folio = binding.editTextNumeroFolio.text.toString().toInt()
        val csg = binding.editTextProductorCsg.text.toString()
        val lote = binding.editTextLote.text.toString()
        val sdp = binding.editTextSdp.text.toString()
        val linea = binding.editTextLineaProceso.text.toString()
        val categoria = binding.editTextCategoria.text.toString()
        val cajas = binding.editTextCantidadCajas.text.toString().toInt()

        // Decide si crea un nuevo registro o actualiza uno existente.
        if (editingTarja == null) {
            tarjaController.addDetalle(currentNumTarja, folio, csg, lote, sdp, linea, categoria, cajas)
            Toast.makeText(this, "Detalle agregado", Toast.LENGTH_SHORT).show()
        } else {
            tarjaController.updateDetalle(editingTarja!!.id, folio, csg, lote, sdp, linea, categoria, cajas)
            Toast.makeText(this, "Detalle actualizado", Toast.LENGTH_SHORT).show()
        }

        hideForm()
        loadDetalles() // Recarga la lista desde la BD.
    }

    /**
     * Valida que todos los campos del formulario de detalle estén completos.
     * @return true si todos los campos son válidos, false si alguno está vacío.
     */
    private fun validateDetailFields(): Boolean {
        val fields = mapOf(
            binding.editTextNumeroFolio to "Folio",
            binding.editTextProductorCsg to "CSG",
            binding.editTextLote to "Lote",
            binding.editTextSdp to "SDP",
            binding.editTextLineaProceso to "Línea de Proceso",
            binding.editTextCategoria to "Categoría",
            binding.editTextCantidadCajas to "Cantidad de Cajas"
        )

        for ((field, name) in fields) {
            if (field.text.toString().isBlank()) {
                Toast.makeText(this, "El campo '$name' es obligatorio", Toast.LENGTH_SHORT).show()
                return false
            }
        }
        return true
    }

    /**
     * Carga los detalles de la tarja actual desde la base de datos y actualiza la lista.
     */
    private fun loadDetalles() {
        val detalles = tarjaController.getDetallesPorTarja(currentNumTarja)
        detallesList.clear()
        detallesList.addAll(detalles)
        adapter.notifyDataSetChanged() // Notifica al adaptador que redibuje la lista.
    }

    /**
     * Muestra el formulario de edición/creación y oculta la lista principal.
     */
    private fun showForm() {
        binding.formContainer.visibility = View.VISIBLE
        binding.recyclerViewResumen.visibility = View.GONE
        binding.fabAddNew.visibility = View.GONE
        binding.buttonEnviar.visibility = View.GONE
        binding.buttonEditHeader.visibility = View.GONE
    }

    /**
     * Oculta el formulario y muestra la lista principal.
     */
    private fun hideForm() {
        binding.formContainer.visibility = View.GONE
        binding.recyclerViewResumen.visibility = View.VISIBLE
        binding.fabAddNew.visibility = View.VISIBLE
        binding.buttonEnviar.visibility = View.VISIBLE
        binding.buttonEditHeader.visibility = View.VISIBLE
        clearForm()
    }

    /**
     * Limpia todos los campos del formulario.
     */
    private fun clearForm() {
        binding.editTextNumeroFolio.text = null
        binding.editTextProductorCsg.text = null
        binding.editTextLote.text = null
        binding.editTextSdp.text = null
        binding.editTextLineaProceso.text = null
        binding.editTextCategoria.text = null
        binding.editTextCantidadCajas.text = null
        editingTarja = null // Sale del modo edición.
    }

    /**
     * Muestra un diálogo de confirmación antes de eliminar un detalle.
     */
    private fun showDeleteConfirmationDialog(tarja: Tarja) {
        AlertDialog.Builder(this)
            .setTitle("Confirmar eliminación")
            .setMessage("¿Estás seguro de que quieres eliminar el folio ${tarja.folio}?")
            .setPositiveButton("Sí") { _, _ ->
                tarjaController.deleteDetalle(tarja.id)
                Toast.makeText(this, "Detalle eliminado", Toast.LENGTH_SHORT).show()
                loadDetalles() // Recarga la lista.
            }
            .setNegativeButton("No", null)
            .show()
    }

    /**
     * Muestra un diálogo de confirmación antes de enviar la tarja.
     */
    private fun showSendConfirmationDialog() {
        AlertDialog.Builder(this)
            .setTitle("Confirmar envío")
            .setMessage("¿Estás seguro de que quieres enviar la tarja N° $currentNumTarja?")
            .setPositiveButton("Sí") { _, _ ->
                // Cambia el estado de la tarja a "enviado" en la base de datos.
                tarjaController.updateStatusEnviado(currentNumTarja)
                Toast.makeText(this, "Tarja N° $currentNumTarja enviada", Toast.LENGTH_SHORT).show()

                // Navega a la pantalla de tarjas ingresadas y cierra esta.
                val intent = Intent(this, tarjas_ingresadas::class.java)
                startActivity(intent)
                finish()
            }
            .setNegativeButton("No", null)
            .show()
    }

    /**
     * Configura el comportamiento del botón de retroceso del sistema.
     */
    private fun setupBackButton() {
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                // Si el formulario está visible, lo oculta. Si no, cierra la actividad.
                if (binding.formContainer.visibility == View.VISIBLE) {
                    hideForm()
                } else {
                    finish()
                }
            }
        })
    }
}