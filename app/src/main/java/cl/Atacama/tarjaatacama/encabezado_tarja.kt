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
import androidx.core.content.ContextCompat
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
import android.util.Log

class encabezado_tarja : AppCompatActivity() {

    private lateinit var binding: ActivityEncabezadoTarjaBinding
    private lateinit var tarjaController: TarjaController
    private var selectedDateInMillis: Long = 0
    private var editModeNumTarja: Int = -1
    private var selectedEtiqueta: Etiqueta? = null
    private var selectedEmbalaje: Any? = null
    // IDs seleccionados (asegurar que enviemos IDs numéricos al controller)
    private var selectedEmbalajeId: Int = 0
    private var selectedProcProdId: Int = 0
    private var selectedProcComId: Int = 0
    private var selectedLogoNomCod: String? = null
    // Bandera para evitar recursión cuando actualizamos programáticamente el campo "recibidor"
    private var recibidorTextUpdating: Boolean = false

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
            // Mostrar el embalaje en forma limpia (sin id) y seleccionar el objeto correspondiente
            val embalajeDisplay = sanitizeEmbalajeDisplay(encabezado.embalaje)
            (binding.menuEmbalaje.editText as? AutoCompleteTextView)?.setText(embalajeDisplay, false)
            selectedEmbalaje = findEmbalajeByCode(encabezado.embalaje)
            // Si encabezado.embalaje contiene un id numérico, usarlo
            selectedEmbalajeId = encabezado.embalaje.toIntOrNull() ?: 0

            val dbFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
            val displayFormat = SimpleDateFormat("dd-MM-yyyy", Locale.getDefault())
            try {
                val date = dbFormat.parse(encabezado.fechaEmbalaje)
                date?.let {
                    selectedDateInMillis = it.time
                    binding.textFieldFecha.editText?.setText(displayFormat.format(it))
                }
            } catch (e: Exception) { /* Ignore */ }

            val etiquetaId = encabezado.etiqueta.toLongOrNull()
            if (etiquetaId != null) {
                val etiquetas = tarjaController.getAllEtiquetas()
                val etiqueta = etiquetas.find { it.id == etiquetaId }
                etiqueta?.let { et ->
                    selectedEtiqueta = et
                    // marcar etiqueta seleccionada en el campo visual (aunque el spinner muestra imágenes)
                    (binding.menuEtiqueta.editText as? AutoCompleteTextView)?.setText("", false)
                    // también setear el recibidor con el display correcto
                    val recibidorField = (binding.menuRecibidor.editText as? AutoCompleteTextView)
                    recibidorField?.let { field ->
                        recibidorTextUpdating = true
                        try {
                            field.setText(findRecibidorDisplayForEtiqueta(et), false)
                        } finally {
                            recibidorTextUpdating = false
                        }
                    }
                    // Mostrar la imagen de la etiqueta en el start icon
                    setEtiquetaIcon(selectedEtiqueta)
                }
            }

            (binding.menuVariedad.editText as? AutoCompleteTextView)?.setText(encabezado.variedad, false)
            (binding.menuRecibidor.editText as? AutoCompleteTextView)?.setText(encabezado.recibidor, false)
            (binding.menuLogo.editText as? AutoCompleteTextView)?.setText(encabezado.logo, false)
            (binding.menuProcedenciaProd.editText as? AutoCompleteTextView)?.setText(encabezado.procProd.toString(), false)
            // Si encabezado.procProd/procCom vienen como ids, capturarlos
            selectedProcProdId = encabezado.procProd
            selectedProcComId = encabezado.procCom
            (binding.menuProcedenciaCom.editText as? AutoCompleteTextView)?.setText(encabezado.procCom.toString(), false)
            binding.menuPlu.editText?.setText(encabezado.plu.toString())
        }
    }

    // Buscar el objeto embalaje cuyo código coincida con 'code' o cuyo display sanitizado coincida
    private fun findEmbalajeByCode(code: String?): Any? {
        if (code.isNullOrBlank()) return null
        val embalajes = tarjaController.getAllEmbalajes()
        for (obj in embalajes) {
            try {
                val objCode = getEmbalajeCode(obj)
                if (!objCode.isNullOrBlank() && objCode.equals(code, ignoreCase = true)) return obj
                val disp = sanitizeEmbalajeDisplay(obj.toString())
                if (!disp.isNullOrBlank() && disp.equals(code, ignoreCase = true)) return obj
            } catch (_: Exception) {}
        }
        return null
    }

    private fun setupCreateMode() {
        binding.toolbar.title = "Crear Encabezado"
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
            // cuando se selecciona una etiqueta, colocar su nombre en recibidor (desambiguado si es necesario)
            val recibidorDisplay = findRecibidorDisplayForEtiqueta(selectedEtiqueta)
            val recibidorField = (binding.menuRecibidor.editText as? AutoCompleteTextView)
            recibidorField?.let { field ->
                recibidorTextUpdating = true
                try {
                    field.setText(recibidorDisplay, false)
                } finally {
                    recibidorTextUpdating = false
                }
            }
            // Mostrar la imagen de la etiqueta en el start icon del TextInputLayout y limpiar el texto
            setEtiquetaIcon(selectedEtiqueta)
            val etiquetaField = (binding.menuEtiqueta.editText as? AutoCompleteTextView)
            etiquetaField?.post {
                etiquetaField.setText("", false)
                etiquetaField.clearFocus()
            }
        }

        // Preparar lista de recibidores (basada en etiquetas). Si hay nombres duplicados, desambiguar por id
        val recibidorMap = mutableMapOf<String, Etiqueta>()
        val recibidorList = ArrayList<String>()
        val grouped = etiquetas.groupBy { it.nombre.trim() }
        for ((name, group) in grouped) {
            if (group.size == 1) {
                recibidorMap[name] = group[0]
                recibidorList.add(name)
            } else {
                // si hay duplicados, enlistarlos con id para que el usuario pueda elegir
                for (e in group) {
                    val display = "$name (id:${e.id})"
                    recibidorMap[display] = e
                    recibidorList.add(display)
                }
            }
        }

        val recibidorAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, recibidorList)
        val recibidorField = (binding.menuRecibidor.editText as? AutoCompleteTextView)
        recibidorField?.setAdapter(recibidorAdapter)
        recibidorField?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
            val display = parent.getItemAtPosition(position) as String
            val etiqueta = recibidorMap[display]
            etiqueta?.let {
                selectedEtiqueta = it
                // sincronizar menuEtiqueta (no muestra texto, pero guardamos selección)
                val etiquetaField = (binding.menuEtiqueta.editText as? AutoCompleteTextView)
                etiquetaField?.post {
                    etiquetaField.setText("", false)
                    etiquetaField.clearFocus()
                }
                // Mostrar imagen de la etiqueta seleccionada
                setEtiquetaIcon(selectedEtiqueta)
            }
        }

        // Si el usuario escribe en el campo recibidor, intentar hacer match automático
        recibidorField?.addTextChangedListener(object : android.text.TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {}
            override fun afterTextChanged(s: android.text.Editable?) {
                if (recibidorTextUpdating) return
                val text = s?.toString()?.trim() ?: ""
                if (text.isEmpty()) {
                    selectedEtiqueta = null
                    return
                }

                // Si el usuario escribió algo con (id:NUMBER), priorizar búsqueda por id
                val idRegex = Regex("\\(\\s*id\\s*:\\s*(\\d+)\\s*\\)", RegexOption.IGNORE_CASE)
                val idMatch = idRegex.find(text)
                if (idMatch != null) {
                    val id = idMatch.groupValues[1].toLongOrNull()
                    if (id != null) {
                        val et = tarjaController.getAllEtiquetas().find { it.id == id }
                        if (et != null) {
                            selectedEtiqueta = et
                            // actualizar display sin provocar recursión
                            recibidorTextUpdating = true
                            try {
                                recibidorField.setText(findRecibidorDisplayForEtiqueta(selectedEtiqueta), false)
                            } finally {
                                recibidorTextUpdating = false
                            }
                            // Mostrar imagen si encontramos etiqueta por id
                            setEtiquetaIcon(selectedEtiqueta)
                            return
                        }
                    }
                }

                // Buscar etiquetas por nombre exacto (ignorar mayúsculas)
                val etiquetas = tarjaController.getAllEtiquetas()
                val matches = etiquetas.filter { it.nombre.trim().equals(text, ignoreCase = true) }
                if (matches.size == 1) {
                    selectedEtiqueta = matches[0]
                    // si es única, asegurar display normalizado en recibidor
                    recibidorTextUpdating = true
                    try {
                        recibidorField.setText(findRecibidorDisplayForEtiqueta(selectedEtiqueta), false)
                    } finally {
                        recibidorTextUpdating = false
                    }
                    // Mostrar la imagen de la etiqueta encontrada
                    setEtiquetaIcon(selectedEtiqueta)
                    return
                } else if (matches.size > 1) {
                    // Mostrar opciones desambiguadas en el dropdown
                    val options = matches.map { "${it.nombre.trim()} (id:${it.id})" }
                    val optAdapter = ArrayAdapter(this@encabezado_tarja, android.R.layout.simple_dropdown_item_1line, options)
                    recibidorField.setAdapter(optAdapter)
                    recibidorField.showDropDown()
                    selectedEtiqueta = null
                    // Quitar icono si hay ambigüedad
                    setEtiquetaIcon(null)
                    return
                }

                // Si no hay coincidencias exactas por nombre, no seleccionar etiqueta
                selectedEtiqueta = null
                setEtiquetaIcon(null)
            }
        })

        val embalajes = tarjaController.getAllEmbalajes()
        // Mapear embalajes (Pair<id,codigo>) a display->id para enviar el Id al guardar
        val embalajeDisplayList = embalajes.mapNotNull { pair -> pair.second?.trim() }.filter { it.isNotBlank() }.distinct()
        val embalajeMap = mutableMapOf<String, Int>()
        for (p in embalajes) {
            val display = p.second.trim()
            if (!embalajeMap.containsKey(display)) embalajeMap[display] = p.first
        }
        val embalajeAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, embalajeDisplayList)
        val embalajeField = (binding.menuEmbalaje.editText as? AutoCompleteTextView)
        embalajeField?.setAdapter(embalajeAdapter)
        embalajeField?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
            val display = parent.getItemAtPosition(position) as String
            selectedEmbalajeId = embalajeMap[display] ?: 0
            selectedEmbalaje = embalajes.find { it.first == selectedEmbalajeId }
            embalajeField.setText(display, false)
        }

        val variedades = tarjaController.getAllVariedades()
        val variedadAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, variedades)
        (binding.menuVariedad.editText as? AutoCompleteTextView)?.setAdapter(variedadAdapter)

        (binding.menuVariedad.editText as? AutoCompleteTextView)?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
            val selectedVariedad = parent.getItemAtPosition(position) as String
            val plu = tarjaController.getPluForVariedad(selectedVariedad)
            binding.menuPlu.editText?.setText(plu?.toString() ?: "")
        }

        // CARGAR LOGOS DESDE BD y mostrar sólo el campo "nom_cod" en el dropdown
        // TarjaController.getAllLogos() devuelve List<Pair<Int,String>> (id, nom_cod)
        try {
            val logoPairs = try { tarjaController.getAllLogos() } catch (e: Exception) { emptyList<Pair<Int,String>>() }
            val logoDisplayList = logoPairs.map { it.second.trim() }.filter { it.isNotBlank() }.distinct()
            val logoMap = mutableMapOf<String, Int>()
            for (p in logoPairs) {
                val display = p.second.trim()
                if (!logoMap.containsKey(display)) logoMap[display] = p.first
            }

            val logoAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, logoDisplayList)
            val logoField = (binding.menuLogo.editText as? AutoCompleteTextView)
            logoField?.setAdapter(logoAdapter)
            logoField?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
                val display = parent.getItemAtPosition(position) as String
                selectedLogoNomCod = display
                logoField.setText(display, false)
            }
        } catch (_: Exception) {
            // No bloquear si algo falla; dejar el campo vacío
        }

        // CARGAR PROCEDENCIA PROD DESDE BD y mostrar sólo el campo 'codigo_procedencia'
        try {
            val procPairs = try { tarjaController.getAllProcedenciaProd() } catch (e: Exception) { emptyList<Pair<Int,String>>() }
            val procDisplayListProd = procPairs.map { it.second.trim() }.filter { it.isNotBlank() }.distinct()
            val procMap = mutableMapOf<String, Int>()
            for (p in procPairs) {
                val display = p.second.trim()
                if (!procMap.containsKey(display)) procMap[display] = p.first
            }
            val procAdapter = ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, procDisplayListProd)
            val procField = (binding.menuProcedenciaProd.editText as? AutoCompleteTextView)
            procField?.setAdapter(procAdapter)
            procField?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
                val display = parent.getItemAtPosition(position) as String
                selectedProcProdId = procMap[display] ?: 0
                procField.setText(display, false)
            }

            // Procedencia COM: lista independiente con sólo la opción "Opcional" (id = 0)
            val procDisplayListCom = listOf("Opcional")
            val procComMap = mapOf("Opcional" to 0)
            val procComField = (binding.menuProcedenciaCom.editText as? AutoCompleteTextView)
            procComField?.setAdapter(ArrayAdapter(this, android.R.layout.simple_dropdown_item_1line, procDisplayListCom))
            procComField?.onItemClickListener = AdapterView.OnItemClickListener { parent, _, position, _ ->
                val display = parent.getItemAtPosition(position) as String
                selectedProcComId = procComMap[display] ?: 0
                procComField.setText(display, false)
            }
        } catch (_: Exception) {
            // dejar vacío si falla
        }

    }

    // Carga el drawable de la etiqueta por su imagenUri (se asume nombre de drawable) y lo pone como startIcon en el TextInputLayout
    private fun setEtiquetaIcon(et: Etiqueta?) {
        try {
            if (et == null) {
                binding.menuEtiqueta.isStartIconVisible = false
                binding.menuEtiqueta.startIconDrawable = null
                return
            }
            val imgName = et.imagenUri?.trim()
            if (imgName.isNullOrBlank()) {
                binding.menuEtiqueta.isStartIconVisible = false
                binding.menuEtiqueta.startIconDrawable = null
                return
            }
            val resId = resources.getIdentifier(imgName, "drawable", packageName)
            if (resId != 0) {
                val dr = ContextCompat.getDrawable(this, resId)
                binding.menuEtiqueta.startIconDrawable = dr
                binding.menuEtiqueta.isStartIconVisible = true
            } else {
                // Si no existe drawable con ese nombre, ocultar icono
                binding.menuEtiqueta.isStartIconVisible = false
                binding.menuEtiqueta.startIconDrawable = null
            }
        } catch (_: Exception) {
            // No bloquear la UI si algo falla
            binding.menuEtiqueta.isStartIconVisible = false
            binding.menuEtiqueta.startIconDrawable = null
        }
    }

    // Devuelve el display que usamos en el menú Recibidor para una Etiqueta concreta (nombre o "nombre (id:xx)" si necesario)
    private fun findRecibidorDisplayForEtiqueta(et: Etiqueta?): String {
        if (et == null) return ""
        val etiquetas = tarjaController.getAllEtiquetas()
        val group = etiquetas.filter { it.nombre.trim().equals(et.nombre.trim(), ignoreCase = true) }
        return if (group.size == 1) et.nombre.trim() else "${et.nombre.trim()} (id:${et.id})"
    }

    // Quitar el id si viene en la forma "(id, CODE)" o "id - CODE" etc., devolver sólo CODE
    private fun sanitizeEmbalajeDisplay(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        // Si tiene paréntesis tipo "(32, BSUBI)" o "(32, BSUBI)": eliminar el prefijo numérico
        var s = raw.trim()
        // Remover paréntesis externos
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length - 1).trim()
        }
        // Si hay una coma separando id y código, devolver la parte después de la coma
        val commaIndex = s.indexOf(',')
        if (commaIndex >= 0) {
            return s.substring(commaIndex + 1).trim().removeSurrounding("\"", "\"")
        }
        // Si hay un guion o espacio separado con números, intentar quitar los números iniciales
        val regex = Regex("^\\s*\\d+['\"][,\\-\\s]+")
        val cleaned = s.replace(regex, "").trim()
        return cleaned.ifEmpty { s }
    }

    // Intentar extraer el código real del objeto embalaje (campo 'codigo' o método 'getCodigo'); si no, devolver el display
    private fun getEmbalajeCode(obj: Any?): String {
        if (obj == null) return ""
        // Si es String asumimos que ya es el código
        if (obj is String) return obj
        try {
            // Buscar método getCodigo / codigo
            val method = obj.javaClass.methods.firstOrNull { it.parameterCount == 0 && it.name.equals("getCodigo", true) }
                ?: obj.javaClass.methods.firstOrNull { it.parameterCount == 0 && it.name.equals("codigo", true) }
            if (method != null) {
                val res = method.invoke(obj) as? String
                if (!res.isNullOrBlank()) return res
            }
        } catch (_: Exception) {}

        try {
            val field = obj.javaClass.declaredFields.firstOrNull { it.name.equals("codigo", true) || it.name.equals("code", true) || it.name.equals("nombre", true) }
            if (field != null) {
                field.isAccessible = true
                val v = field.get(obj)
                if (v != null) return v.toString()
            }
        } catch (_: Exception) {}

        // Fallback: usar el display sanitizado
        return sanitizeEmbalajeDisplay(obj.toString())
    }

    // Extraer nom_cod del objeto logo (soporta String o cualquier objeto con getNom_cod/getNomCod/field nom_cod)
    private fun getLogoNomCod(obj: Any?): String {
        if (obj == null) return ""
        if (obj is String) return obj
        try {
            val method = obj.javaClass.methods.firstOrNull { it.parameterCount == 0 && (it.name.equals("getNom_cod", true) || it.name.equals("getNomCod", true) || it.name.equals("getNom", true)) }
            if (method != null) {
                val res = method.invoke(obj) as? String
                if (!res.isNullOrBlank()) return res
            }
        } catch (_: Exception) {}

        try {
            val field = obj.javaClass.declaredFields.firstOrNull { it.name.equals("nom_cod", true) || it.name.equals("nomCod", true) || it.name.equals("nom", true) || it.name.equals("codigo", true) }
            if (field != null) {
                field.isAccessible = true
                val v = field.get(obj)
                if (v != null) return v.toString()
            }
        } catch (_: Exception) {}

        return obj.toString()
    }

    fun hecho(view: View) {
        if (!validateAllFields()) {
            return
        }

        val numTarjaStr = binding.textFieldTarja.editText?.text.toString()
        val numPalletStr = binding.textFieldPallet.editText?.text.toString()
        // Usar el ID del embalaje si fue seleccionado; si no, intentar parsear el texto, si falla -> 0
        val embalaje = if (selectedEmbalajeId != 0) selectedEmbalajeId.toString() else (binding.menuEmbalaje.editText?.text.toString().toIntOrNull()?.toString() ?: "0")
        val variedad = binding.menuVariedad.editText?.text.toString()
        val recibidor = binding.menuRecibidor.editText?.text.toString()
        val logo = binding.menuLogo.editText?.text.toString()
        // Usar IDs seleccionados para procedencias, si no fueron seleccionadas, intentar parsear el texto
        val procProdStr = binding.menuProcedenciaProd.editText?.text.toString()
        val procComStr = binding.menuProcedenciaCom.editText?.text.toString()
        val pluStr = binding.menuPlu.editText?.text.toString()

        val dbFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
        dbFormat.timeZone = TimeZone.getTimeZone("UTC")
        val fechaEmbalajeForDb = dbFormat.format(Date(selectedDateInMillis))

        val numTarja = numTarjaStr.toInt()
        val numPallet = numPalletStr.toIntOrNull() ?: 0
        val procProd = if (selectedProcProdId != 0) selectedProcProdId else procProdStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        val procCom = if (selectedProcComId != 0) selectedProcComId else procComStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        val plu = pluStr.filter { it.isDigit() }.toIntOrNull() ?: 0
        // Preparar valores seguros para enviar al controller: Embalaje como string numérico cuando sea posible, etiquetaId como entero >=0
        val etiquetaId = selectedEtiqueta?.id?.toInt() ?: 0
        val logoToSend = selectedLogoNomCod ?: logo

        if (editModeNumTarja != -1) {
            // Log de valores que vamos a enviar al controller para facilitar depuración
            Log.d("EncabezadoTarja", "Actualizar Encabezado -> numTarja=$numTarja, numPallet=$numPallet, fecha=$fechaEmbalajeForDb, embalajeId=$embalaje, etiquetaId=${etiquetaId}, variedad=$variedad, recibidor=$recibidor, logo=$logoToSend, procProdId=$procProd, procComId=$procCom, plu=$plu")
             val result = tarjaController.updateEncabezado(
                 numTarja, numPallet, fechaEmbalajeForDb, embalaje, etiquetaId.toString(), variedad, recibidor, logoToSend, procProd, procCom, plu
             )
             if (result > 0) {
                 Toast.makeText(this, "Encabezado actualizado", Toast.LENGTH_SHORT).show()
                 finish()
             } else {
                 Toast.makeText(this, "Error al actualizar", Toast.LENGTH_SHORT).show()
             }
         } else {
            // Log de valores que vamos a enviar al controller al crear
            Log.d("EncabezadoTarja", "Crear Encabezado -> numTarja=$numTarja, numPallet=$numPallet, fecha=$fechaEmbalajeForDb, embalajeId=$embalaje, etiquetaId=${etiquetaId}, variedad=$variedad, recibidor=$recibidor, logo=$logoToSend, procProdId=$procProd, procComId=$procCom, plu=$plu")
             val result = tarjaController.addEncabezado(
                 numTarja, numPallet, fechaEmbalajeForDb, embalaje, etiquetaId.toString(), variedad, recibidor, logoToSend, procProd, procCom, plu
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
            // binding.menuEtiqueta to "Etiqueta", // manejamos etiqueta por selectedEtiqueta
            binding.menuVariedad to "Variedad",
            binding.menuRecibidor to "Recibidor",
            binding.menuLogo to "Logo",
            binding.menuProcedenciaProd to "Procedencia PROD",
            binding.menuProcedenciaCom to "Procedencia COM"
            // Nota: PLU ahora es opcional, no se incluye en la lista de campos obligatorios
        )

        for ((field, name) in fieldsToValidate) {
            if (field.editText?.text.toString().isBlank()) {
                Toast.makeText(this, "El campo '$name' es obligatorio", Toast.LENGTH_SHORT).show()
                return false
            }
        }

        // Validación especial para etiqueta: aceptamos que el campo pueda estar vacío si selectedEtiqueta está seteada
        val etiquetaText = binding.menuEtiqueta.editText?.text.toString()
        if (etiquetaText.isBlank() && selectedEtiqueta == null) {
            Toast.makeText(this, "El campo 'Etiqueta' es obligatorio", Toast.LENGTH_SHORT).show()
            return false
        }

        if (selectedDateInMillis == 0L) {
            Toast.makeText(this, "El campo 'Fecha Embalaje' es obligatorio", Toast.LENGTH_SHORT).show()
            return false
        }

        return true
    }
}
