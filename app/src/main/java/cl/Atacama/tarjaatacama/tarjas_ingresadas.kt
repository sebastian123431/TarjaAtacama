package cl.Atacama.tarjaatacama

import android.content.Intent
import android.os.Bundle
import android.view.LayoutInflater
import android.widget.ArrayAdapter
import android.widget.AutoCompleteTextView
import android.widget.Toast
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.FileProvider
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.recyclerview.widget.LinearLayoutManager
import cl.Atacama.tarjaatacama.adapter.TarjasIngresadasAdapter
import cl.Atacama.tarjaatacama.controller.TarjaController
import cl.Atacama.tarjaatacama.databinding.ActivityTarjasIngresadasBinding
import cl.Atacama.tarjaatacama.databinding.DialogExportOptionsBinding
import cl.Atacama.tarjaatacama.modelo.Encabezado
import cl.Atacama.tarjaatacama.util.ExcelGenerator
import cl.Atacama.tarjaatacama.util.PdfGenerator
import com.google.android.material.datepicker.MaterialDatePicker
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

class tarjas_ingresadas : AppCompatActivity() {

    private lateinit var binding: ActivityTarjasIngresadasBinding
    private lateinit var tarjaController: TarjaController
    private var allEncabezados = listOf<Encabezado>()
    private val filteredEncabezados = mutableListOf<Encabezado>()
    private lateinit var adapter: TarjasIngresadasAdapter

    private var fechaDesde: String? = null
    private var fechaHasta: String? = null

    private enum class ExportType { PDF, EXCEL }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        binding = ActivityTarjasIngresadasBinding.inflate(layoutInflater)
        setContentView(binding.root)

        ViewCompat.setOnApplyWindowInsetsListener(binding.main) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }

        tarjaController = TarjaController(this)

        setupRecyclerView()
        setupFilterMenu()
        setupClickListeners()
    }

    override fun onResume() {
        super.onResume()
        loadAndFilterData()
    }

    private fun setupRecyclerView() {
        adapter = TarjasIngresadasAdapter(filteredEncabezados, { encabezado -> handleEdit(encabezado) }, { encabezado -> showDeleteConfirmationDialog(encabezado) })
        binding.recyclerViewTarjas.adapter = adapter
        binding.recyclerViewTarjas.layoutManager = LinearLayoutManager(this)
    }

    private fun setupFilterMenu() {
        val filterOptions = listOf("Ver Todas", "Pendientes", "Enviadas")
        val filterAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, filterOptions)
        (binding.menuFilterStatus.editText as? AutoCompleteTextView)?.setAdapter(filterAdapter)
        (binding.menuFilterStatus.editText as? AutoCompleteTextView)?.setText(filterOptions[0], false)
    }

    private fun setupClickListeners() {
        binding.buttonAgregarTarja.setOnClickListener { startActivity(Intent(this, encabezado_tarja::class.java)) }
        binding.buttonExportPdf.setOnClickListener { showExportOptionsDialog(ExportType.PDF) }
        binding.buttonExportExcel.setOnClickListener { showExportOptionsDialog(ExportType.EXCEL) }

        binding.textFieldFilterFechaDesde.editText?.setOnClickListener { showDatePicker(true) }
        binding.textFieldFilterFechaHasta.editText?.setOnClickListener { showDatePicker(false) }

        binding.buttonApplyFilters.setOnClickListener { loadAndFilterData() }
        binding.buttonClearFilters.setOnClickListener {
            (binding.menuFilterStatus.editText as? AutoCompleteTextView)?.setText("Ver Todas", false)
            binding.textFieldFilterFechaDesde.editText?.text = null
            binding.textFieldFilterFechaHasta.editText?.text = null
            fechaDesde = null
            fechaHasta = null
            loadAndFilterData()
        }
    }

    private fun showDatePicker(isDesde: Boolean) {
        val datePicker = MaterialDatePicker.Builder.datePicker().build()
        datePicker.addOnPositiveButtonClickListener { selection ->
            val sdfDb = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }
            val sdfDisplay = SimpleDateFormat("dd-MM-yyyy", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }
            if (isDesde) {
                fechaDesde = sdfDb.format(Date(selection))
                binding.textFieldFilterFechaDesde.editText?.setText(sdfDisplay.format(Date(selection)))
            } else {
                fechaHasta = sdfDb.format(Date(selection))
                binding.textFieldFilterFechaHasta.editText?.setText(sdfDisplay.format(Date(selection)))
            }
        }
        datePicker.show(supportFragmentManager, if (isDesde) "DATE_PICKER_DESDE" else "DATE_PICKER_HASTA")
    }

    private fun loadAndFilterData() {
        val status = when ((binding.menuFilterStatus.editText as? AutoCompleteTextView)?.text.toString()) {
            "Pendientes" -> "pendiente"
            "Enviadas" -> "enviado"
            else -> null
        }
        
        if ((fechaDesde != null && fechaHasta == null) || (fechaDesde == null && fechaHasta != null)) {
            Toast.makeText(this, "Debe seleccionar un rango de fechas completo", Toast.LENGTH_SHORT).show()
            return
        }

        allEncabezados = tarjaController.getFilteredEncabezados(status, fechaDesde, fechaHasta)
        filteredEncabezados.clear()
        filteredEncabezados.addAll(allEncabezados)
        adapter.notifyDataSetChanged()
    }
    
    private fun showExportOptionsDialog(exportType: ExportType) {
        val dialogBinding = DialogExportOptionsBinding.inflate(LayoutInflater.from(this))
        val dialogTitle = if (exportType == ExportType.PDF) "Exportar a PDF" else "Exportar a Excel"
        val dialog = AlertDialog.Builder(this)
            .setTitle(dialogTitle)
            .setView(dialogBinding.root)
            .setPositiveButton("Exportar", null)
            .setNegativeButton("Cancelar", null)
            .create()

        val statusOptions = listOf("Todas", "Pendientes", "Enviadas")
        val statusAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, statusOptions)
        (dialogBinding.menuExportStatus.editText as? AutoCompleteTextView)?.setAdapter(statusAdapter)

        var exportFechaDesde: String? = null
        var exportFechaHasta: String? = null

        dialogBinding.textFieldFechaDesde.editText?.setOnClickListener {
            val datePicker = MaterialDatePicker.Builder.datePicker().setTitleText("Fecha Desde").build()
            datePicker.addOnPositiveButtonClickListener { selection ->
                val sdf = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }
                exportFechaDesde = sdf.format(Date(selection))
                dialogBinding.textFieldFechaDesde.editText?.setText(SimpleDateFormat("dd-MM-yyyy", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }.format(Date(selection)))
            }
            datePicker.show(supportFragmentManager, "DATE_PICKER_EXPORT_DESDE")
        }

        dialogBinding.textFieldFechaHasta.editText?.setOnClickListener {
            val datePicker = MaterialDatePicker.Builder.datePicker().setTitleText("Fecha Hasta").build()
            datePicker.addOnPositiveButtonClickListener { selection ->
                val sdf = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }
                exportFechaHasta = sdf.format(Date(selection))
                dialogBinding.textFieldFechaHasta.editText?.setText(SimpleDateFormat("dd-MM-yyyy", Locale.getDefault()).apply { timeZone = TimeZone.getTimeZone("UTC") }.format(Date(selection)))
            }
            datePicker.show(supportFragmentManager, "DATE_PICKER_EXPORT_HASTA")
        }

        dialog.setOnShowListener { 
            val exportButton = dialog.getButton(AlertDialog.BUTTON_POSITIVE)
            exportButton.setOnClickListener { 
                val selectedStatusText = (dialogBinding.menuExportStatus.editText as? AutoCompleteTextView)?.text.toString()
                val status = when (selectedStatusText) {
                    "Pendientes" -> "pendiente"
                    "Enviadas" -> "enviado"
                    else -> null
                }
                
                if (exportFechaDesde == null || exportFechaHasta == null) {
                    Toast.makeText(this, "Por favor, seleccione un rango de fechas para exportar", Toast.LENGTH_SHORT).show()
                    return@setOnClickListener
                }

                val filteredData = tarjaController.getFilteredEncabezados(status, exportFechaDesde, exportFechaHasta)
                if (filteredData.isEmpty()) {
                    Toast.makeText(this, "No hay datos para exportar con esos filtros", Toast.LENGTH_SHORT).show()
                    return@setOnClickListener
                }

                val dateRangeText = "${dialogBinding.textFieldFechaDesde.editText?.text} al ${dialogBinding.textFieldFechaHasta.editText?.text}"
                var file: File?
                var mimeType: String

                if (exportType == ExportType.PDF) {
                    file = PdfGenerator.createPdf(this, filteredData, "Resumen Tarjas Packing", dateRangeText)
                    mimeType = "application/pdf"
                } else {
                    file = ExcelGenerator.createExcel(this, filteredData, dateRangeText)
                    mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                }

                if (file != null) {
                    val fileUri = FileProvider.getUriForFile(this, "${applicationContext.packageName}.provider", file)
                    val shareIntent = Intent().apply {
                        action = Intent.ACTION_SEND
                        putExtra(Intent.EXTRA_STREAM, fileUri)
                        type = mimeType
                        addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    }
                    startActivity(Intent.createChooser(shareIntent, "Compartir Archivo"))
                } else {
                    Toast.makeText(this, "Error al crear el archivo", Toast.LENGTH_SHORT).show()
                }
                dialog.dismiss()
            }
        }

        dialog.show()
    }
    
    private fun handleEdit(encabezado: Encabezado) {
        if (encabezado.status == "enviado") {
            showEditConfirmationDialog(encabezado)
        } else {
            navigateToEditScreen(encabezado.numTarja)
        }
    }

    private fun navigateToEditScreen(numTarja: Int) {
        val intent = Intent(this, Resumen_tarja::class.java)
        intent.putExtra("NUM_TARJA", numTarja)
        startActivity(intent)
    }

    private fun showEditConfirmationDialog(encabezado: Encabezado) {
        AlertDialog.Builder(this)
            .setTitle("Editar Tarja Enviada")
            .setMessage("Al editar esta tarja, cambiará su estado a 'pendiente' hasta que la envíe de nuevo. ¿Desea continuar?")
            .setPositiveButton("Sí") { _, _ ->
                tarjaController.updateStatusPendiente(encabezado.numTarja)
                navigateToEditScreen(encabezado.numTarja)
            }
            .setNegativeButton("No", null)
            .show()
    }

    private fun showDeleteConfirmationDialog(encabezado: Encabezado) {
        AlertDialog.Builder(this)
            .setTitle("Confirmar eliminación")
            .setMessage("¿Estás seguro de que quieres eliminar la tarja N° ${encabezado.numTarja}? Esto borrará también todos sus detalles.")
            .setPositiveButton("Sí") { _, _ ->
                val success = tarjaController.deleteTarjaCompleta(encabezado.numTarja)
                if (success) {
                    Toast.makeText(this, "Tarja eliminada", Toast.LENGTH_SHORT).show()
                    loadAndFilterData()
                } else {
                    Toast.makeText(this, "Error al eliminar la tarja", Toast.LENGTH_SHORT).show()
                }
            }
            .setNegativeButton("No", null)
            .show()
    }
}