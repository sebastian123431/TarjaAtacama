package cl.Atacama.tarjaatacama

import android.content.Intent
import android.os.Bundle
import android.view.View
import android.widget.AdapterView
import android.widget.ArrayAdapter
import android.widget.AutoCompleteTextView
import android.widget.Toast
import androidx.activity.enableEdgeToEdge
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.isVisible
import cl.Atacama.tarjaatacama.adapter.EtiquetaSpinnerAdapter
import cl.Atacama.tarjaatacama.controller.TarjaController
import cl.Atacama.tarjaatacama.databinding.ActivityEncabezadoTarjaBinding
import cl.Atacama.tarjaatacama.modelo.Etiqueta
import com.google.android.material.datepicker.MaterialDatePicker
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

class encabezado_tarja : AppCompatActivity() {

    private lateinit var binding: ActivityEncabezadoTarjaBinding
    private lateinit var tarjaController: TarjaController
    private var selectedDateInMillis: Long = 0
    private var editModeNumTarja: Int = -1
    private var selectedEtiqueta: Etiqueta? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        binding = ActivityEncabezadoTarjaBinding.inflate(layoutInflater)
        setContentView(binding.root)

        ViewCompat.setOnApplyWindowInsetsListener(binding.main) { v, insets ->
            val systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.ime())
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom)
            insets
        }

        tarjaController = TarjaController(this)
        editModeNumTarja = intent.getIntExtra("NUM_TARJA", -1)

        setupDatePicker()
        setupDropdownMenus()

        if (editModeNumTarja != -1) {
            setupEditMode()
        } else {
            setupCreateMode()
        }
    }

    private fun setupEditMode() {
        binding.toolbar.title = "Editar Encabezado"
        binding.buttonAgregarTarja.text = "Guardar Cambios"
        binding.buttonCancelar.isVisible = false
        binding.textFieldTarja.editText?.isEnabled = false

        val encabezado = tarjaController.getEncabezado(editModeNumTarja)
        if (encabezado != null) {
            binding.textFieldTarja.editText?.setText(encabezado.numTarja.toString())
            binding.textFieldPallet.editText?.setText(encabezado.numPallet.toString())
            (binding.menuEmbalaje.editText as? AutoCompleteTextView)?.setText(encabezado.embalaje, false)

            val dbFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
            val displayFormat = SimpleDateFormat("dd-MM-yyyy", Locale.getDefault())
            try {
                val date = dbFormat.parse(encabezado.fechaEmbalaje)
                selectedDateInMillis = date.time
                binding.textFieldFecha.editText?.setText(displayFormat.format(date))
            } catch (e: Exception) { /* Ignore */ }

            val etiquetaId = encabezado.etiqueta.toLongOrNull()
            if (etiquetaId != null) {
                val etiquetas = tarjaController.getAllEtiquetas()
                val etiqueta = etiquetas.find { it.id == etiquetaId }
                etiqueta?.let {
                    selectedEtiqueta = it
                    (binding.menuEtiqueta.editText as? AutoCompleteTextView)?.setText(it.nombre, false)
                }
            }
            
            (binding.menuVariedad.editText as? AutoCompleteTextView)?.setText(encabezado.variedad, false)
            (binding.menuRecibidor.editText as? AutoCompleteTextView)?.setText(encabezado.recibidor, false)
            (binding.menuLogo.editText as? AutoCompleteTextView)?.setText(encabezado.logo, false)
            (binding.menuProcedenciaProd.editText as? AutoCompleteTextView)?.setText(encabezado.procProd.toString(), false)
            (binding.menuProcedenciaCom.editText as? AutoCompleteTextView)?.setText(encabezado.procCom.toString(), false)
            binding.menuPlu.editText?.setText(encabezado.plu.toString())
        }
    }

    private fun setupCreateMode() {
        binding.toolbar.title = "Crear Encabezado"
        binding.buttonAgregarTarja.text = "Agregar Tarja"
        binding.buttonCancelar.setOnClickListener { finish() }
    }

    private fun setupDatePicker() {
        binding.textFieldFecha.editText?.setOnClickListener {
            val datePicker = MaterialDatePicker.Builder.datePicker()
                .setTitleText("Seleccionar Fecha de Embalaje")
                .setSelection(MaterialDatePicker.todayInUtcMilliseconds())
                .build()

            datePicker.addOnPositiveButtonClickListener { selection ->
                selectedDateInMillis = selection
                val displayFormat = SimpleDateFormat("dd-MM-yyyy", Locale.getDefault())
                displayFormat.timeZone = TimeZone.getTimeZone("UTC")
                binding.textFieldFecha.editText?.setText(displayFormat.format(Date(selection)))
            }

            datePicker.show(supportFragmentManager, "DATE_PICKER")
        }
    }

    private fun setupDropdownMenus() {
        val etiquetas = tarjaController.getAllEtiquetas()
        val etiquetaAdapter = EtiquetaSpinnerAdapter(this, etiquetas)
        (binding.menuEtiqueta.editText as? AutoCompleteTextView)?.setAdapter(etiquetaAdapter)

        (binding.menuEtiqueta.editText as? AutoCompleteTextView)?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
            selectedEtiqueta = parent.adapter.getItem(position) as Etiqueta
        }

        val embalajes = tarjaController.getAllEmbalajes()
        val embalajeAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, embalajes)
        (binding.menuEmbalaje.editText as? AutoCompleteTextView)?.setAdapter(embalajeAdapter)
        
        val variedades = tarjaController.getAllVariedades()
        val variedadAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, variedades)
        (binding.menuVariedad.editText as? AutoCompleteTextView)?.setAdapter(variedadAdapter)

        (binding.menuVariedad.editText as? AutoCompleteTextView)?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
            val selectedVariedad = parent.getItemAtPosition(position) as String
            val plu = tarjaController.getPluForVariedad(selectedVariedad)
            binding.menuPlu.editText?.setText(plu?.toString() ?: "")
        }


        // TODO: Reemplazar "items" con los datos reales para cada menú.
        val items = listOf("abc 1", "abc 2", "abc 3", "abc 4")
        val adapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, items)

        (binding.menuRecibidor.editText as? AutoCompleteTextView)?.setAdapter(adapter)
        (binding.menuLogo.editText as? AutoCompleteTextView)?.setAdapter(adapter)
        (binding.menuProcedenciaProd.editText as? AutoCompleteTextView)?.setAdapter(adapter)
        (binding.menuProcedenciaCom.editText as? AutoCompleteTextView)?.setAdapter(adapter)
    }

    fun hecho(view: View) {
        if (!validateAllFields()) {
            return
        }

        val numTarjaStr = binding.textFieldTarja.editText?.text.toString()
        val numPalletStr = binding.textFieldPallet.editText?.text.toString()
        val embalaje = binding.menuEmbalaje.editText?.text.toString()
        val variedad = binding.menuVariedad.editText?.text.toString()
        val recibidor = binding.menuRecibidor.editText?.text.toString()
        val logo = binding.menuLogo.editText?.text.toString()
        val procProdStr = binding.menuProcedenciaProd.editText?.text.toString()
        val procComStr = binding.menuProcedenciaCom.editText?.text.toString()
        val pluStr = binding.menuPlu.editText?.text.toString()

        val dbFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
        dbFormat.timeZone = TimeZone.getTimeZone("UTC")
        val fechaEmbalajeForDb = dbFormat.format(Date(selectedDateInMillis))

        val numTarja = numTarjaStr.toInt()
        val numPallet = numPalletStr.toIntOrNull() ?: 0
        val procProd = procProdStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        val procCom = procComStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        val plu = pluStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        val etiquetaId = selectedEtiqueta?.id?.toInt() ?: -1

        if (editModeNumTarja != -1) {
            val result = tarjaController.updateEncabezado(
                numTarja, numPallet, fechaEmbalajeForDb, embalaje, etiquetaId.toString(), variedad, recibidor, logo, procProd, procCom, plu
            )
            if (result > 0) {
                Toast.makeText(this, "Encabezado actualizado", Toast.LENGTH_SHORT).show()
                finish()
            } else {
                Toast.makeText(this, "Error al actualizar", Toast.LENGTH_SHORT).show()
            }
        } else {
            val result = tarjaController.addEncabezado(
                numTarja, numPallet, fechaEmbalajeForDb, embalaje, etiquetaId.toString(), variedad, recibidor, logo, procProd, procCom, plu
            )
            if (result > -1) {
                Toast.makeText(this, "Encabezado guardado", Toast.LENGTH_SHORT).show()
                val intent = Intent(this, Resumen_tarja::class.java)
                intent.putExtra("NUM_TARJA", numTarja)
                startActivity(intent)
            } else {
                Toast.makeText(this, "Error al guardar. El N° de Tarja ya podría existir.", Toast.LENGTH_LONG).show()
            }
        }
    }

    private fun validateAllFields(): Boolean {
        val fieldsToValidate = mapOf(
            binding.textFieldTarja to "N° Tarja",
            binding.textFieldPallet to "N° Pallet",
            binding.menuEmbalaje to "Embalaje",
            binding.menuEtiqueta to "Etiqueta",
            binding.menuVariedad to "Variedad",
            binding.menuRecibidor to "Recibidor",
            binding.menuLogo to "Logo",
            binding.menuProcedenciaProd to "Procedencia PROD",
            binding.menuProcedenciaCom to "Procedencia COM",
            binding.menuPlu to "PLU"
        )

        for ((field, name) in fieldsToValidate) {
            if (field.editText?.text.toString().isBlank()) {
                Toast.makeText(this, "El campo '$name' es obligatorio", Toast.LENGTH_SHORT).show()
                return false
            }
        }

        if (selectedDateInMillis == 0L) {
            Toast.makeText(this, "El campo 'Fecha Embalaje' es obligatorio", Toast.LENGTH_SHORT).show()
            return false
        }

        return true
    }
}
