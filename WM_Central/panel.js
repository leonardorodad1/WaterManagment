// estados
var ESTADOS = [
  { clave: "AVAILABLE",      texto: "Available" },
  { clave: "WATERING",       texto: "Watering" },
  { clave: "LEAK",           texto: "Leak" },
  { clave: "OUT_OF_SERVICE", texto: "Out of Service" },  
  { clave: "DISCONNECTED",   texto: "Disconnected" }
];
var TEXTO_ESTADO = {};
for (var k = 0; k < ESTADOS.length; k++) { TEXTO_ESTADO[ESTADOS[k].clave] = ESTADOS[k].texto; }

// motivo fin riego anteriorr
var TEXTO_MOTIVO = { "TIEMPO": "time limit", "PARADAMANUAL": "stopped at the station", "FUGA": "leak", "BLOQUEO": "blocked by CENTRAL" };

var seleccionada = null;   // id de la WS en la que se ha hecho clic
var ahora_central = 0;     
var ultimas_estaciones = []; 
var ultimos_mensajes = [];  
var filtro = "todos";        

// tiempo de riego
function progresoRiego(ws) {
  if (ws.estado != "WATERING" || ws.riego == null) { return null; }
  var lleva = Math.round(ahora_central - ws.riego.inicio);
  if (lleva > ws.riego.tiempo) { lleva = ws.riego.tiempo; }
  if (lleva < 0) { lleva = 0; }
  return { lleva: lleva, total: ws.riego.tiempo };
}

function horaRiego(ts) {
  var opciones = { timeZone: "Europe/Madrid", hour: "2-digit", minute: "2-digit", second: "2-digit" };
  var hora = new Date(ts * 1000).toLocaleTimeString("es-ES", opciones);
  var dia = new Date(ts * 1000).toLocaleDateString("es-ES", { timeZone: "Europe/Madrid", day: "2-digit", month: "2-digit" });
  var hoy = new Date(ahora_central * 1000).toLocaleDateString("es-ES", { timeZone: "Europe/Madrid", day: "2-digit", month: "2-digit" });
  return dia == hoy ? hora : dia + " " + hora;
}


function actualizar() {
  fetch("/estado")
    .then(function (respuesta) { return respuesta.json(); })
    .then(function (datos) {
      ahora_central = datos.ahora;
      ultimas_estaciones = datos.estaciones;
      ultimos_mensajes = datos.mensajes;
      pintarResumen(datos.estaciones);
      pintarMapa(datos.estaciones);
      pintarTabla(datos.estaciones);
      pintarDetalle(datos.estaciones);
      rellenarDesplegable(datos.estaciones);
      actualizarBotones();
      pintarMensajes(datos.mensajes);
      document.getElementById("vivo").className = "vivo";
      document.getElementById("estado_central").textContent = "WM_Central online";
      document.getElementById("hora").textContent = datos.hora;
      document.getElementById("kafka").textContent = "Kafka broker " + datos.kafka;
      // estado real de kafka cada 5 seg
      if (datos.kafka_ok) {
        document.getElementById("vivo_kafka").className = "vivo";
        document.getElementById("estado_kafka").textContent = "Kafka connected";
      } else {
        document.getElementById("vivo_kafka").className = "vivo caida";
        document.getElementById("estado_kafka").textContent = "Kafka unreachable";
      }
    })
    .catch(function () {
      document.getElementById("vivo").className = "vivo caida";
      document.getElementById("estado_central").textContent = "Imposible conectar con WM_Central";
      document.getElementById("vivo_kafka").className = "vivo caida";
      document.getElementById("estado_kafka").textContent = "Kafka unknown";
    });
}


function pintarResumen(estaciones) {
  var cuenta = { "AVAILABLE": 0, "WATERING": 0, "LEAK": 0, "OUT_OF_SERVICE": 0, "DISCONNECTED": 0 };
  for (var i = 0; i < estaciones.length; i++) { cuenta[estaciones[i].estado] += 1; }
  var html = "";
  for (var j = 0; j < ESTADOS.length; j++) {
    html += "<div class='estado'><span class='muestra " + ESTADOS[j].clave + "'></span>"
      + "<span class='cuenta numero'>" + cuenta[ESTADOS[j].clave] + "</span>"
      + "<span class='nombre'>" + ESTADOS[j].texto + "</span></div>";
  }
  document.getElementById("resumen").innerHTML = html;
}


function pintarMapa(estaciones) {
  var zonas = {};   
  var orden = [];   
  for (var i = 0; i < estaciones.length; i++) {
    var ws = estaciones[i];
    if (!(ws.ubicacion in zonas)) { zonas[ws.ubicacion] = []; orden.push(ws.ubicacion); }
    zonas[ws.ubicacion].push(ws);
  }
  var html = "";
  for (var z = 0; z < orden.length; z++) {
    var ubicacion = orden[z];
    html += "<div class='zona forma" + (z % 4) + "'><div class='nombre'>" + ubicacion + "</div><div class='puntos'>";
    for (var j = 0; j < zonas[ubicacion].length; j++) {
      var est = zonas[ubicacion][j];
      var elegido = est.id == seleccionada ? " elegido" : "";
      html += "<button class='punto" + elegido + "' title='" + est.id + ": " + TEXTO_ESTADO[est.estado] + "' onclick=\"seleccionar('" + est.id + "')\">"
        + "<span class='circulo " + est.estado + "'></span>" + est.id + "</button>";
    }
    html += "</div></div>";
  }
  if (orden.length == 0) { html = "<p class='aviso'>No stations registered yet. They appear here when a monitor registers with WM_Central.</p>"; }
  document.getElementById("mapa").innerHTML = html;
  document.getElementById("num_zonas").textContent = orden.length + (orden.length == 1 ? " zone" : " zones");
}

function pintarTabla(estaciones) {
  var html = "";
  for (var i = 0; i < estaciones.length; i++) {
    var ws = estaciones[i];
    var regando = ws.estado == "WATERING";
    var clase = ws.id == seleccionada ? " class='elegida'" : "";
    html += "<tr" + clase + " onclick=\"seleccionar('" + ws.id + "')\">"
      + "<td class='id'>" + ws.id + "</td>"
      + "<td>" + ws.ubicacion + "</td>"
      + "<td><span class='etiqueta " + ws.estado + "'>" + TEXTO_ESTADO[ws.estado] + "</span>"
      + (progresoRiego(ws) ? "<span class='progreso-tabla numero'>" + progresoRiego(ws).lleva + " / " + progresoRiego(ws).total + " s</span>" : "")
      + "</td>"
      + "<td class='derecha numero'>" + ws.caudal.toFixed(1) + " L/min</td>"
      + "<td class='derecha numero'>" + ws.volumen.toFixed(2) + " L</td>"
      + "<td>" + (regando ? ws.operador : "<span class='vacio'>—</span>") + "</td>"
      + "<td>" + (ws.ultimo_fin ? "<span class='numero'>" + horaRiego(ws.ultimo_fin) + "</span>" : "<span class='vacio'>Never</span>") + "</td>"
      + "</tr>";
  }
  if (estaciones.length == 0) { html = "<tr><td colspan='7' class='aviso'>No stations registered yet.</td></tr>"; }
  document.getElementById("tabla").innerHTML = html;
  document.getElementById("num_estaciones").textContent = estaciones.length + " registered";
}

function pintarDetalle(estaciones) {
  if (seleccionada == null) { return; }
  for (var i = 0; i < estaciones.length; i++) {
    var ws = estaciones[i];
    if (ws.id == seleccionada) {
      var regando = ws.estado == "WATERING";
      var progreso = progresoRiego(ws);
      var barra = "";
      if (progreso) {
        var porcentaje = Math.round(progreso.lleva * 100 / progreso.total);
        barra = "<div class='fila-dato'><span>Watering progress</span><b class='numero'>" + progreso.lleva + " of " + progreso.total + " s</b></div>"
          + "<div class='progreso'><div style='width:" + porcentaje + "%'></div></div>";
      }
      var ultimo;
      if (ws.ultimo_fin) {
        ultimo = "<div class='fila-dato'><span>Finished at</span><b class='numero'>" + horaRiego(ws.ultimo_fin) + "</b></div>"
          + "<div class='fila-dato'><span>Requested by</span><b>" + ws.ultimo_operador + "</b></div>"
          + "<div class='fila-dato'><span>Water used</span><b class='numero'>" + ws.ultimo_volumen.toFixed(2) + " L</b></div>"
          + "<div class='fila-dato'><span>Ended by</span><b>" + (TEXTO_MOTIVO[ws.ultimo_motivo] || ws.ultimo_motivo) + "</b></div>";
      } else {
        ultimo = "<div class='fila-dato'><span>This station has not watered yet</span></div>";
      }
      document.getElementById("detalle").innerHTML =
          "<div class='detalle'>"
        + "<p class='titulo'>" + ws.id + "</p>"
        + "<p class='lugar'>" + ws.ubicacion + "</p>"
        + "<span class='etiqueta " + ws.estado + "'>" + TEXTO_ESTADO[ws.estado] + "</span>"
        + "<div class='lectura'>"
        + "<div><span>Current flow</span><b class='numero'>" + ws.caudal.toFixed(1) + "</b> L/min</div>"
        + "<div><span>Water supplied</span><b class='numero'>" + ws.volumen.toFixed(2) + "</b> L</div>"
        + "</div>"
        + "<div class='fila-dato'><span>Operator</span><b>" + (regando ? ws.operador : "—") + "</b></div>"
        + "<div class='fila-dato'><span>Valve</span><b>" + (regando ? "Open" : "Closed") + "</b></div>"
        + barra
        + "<p class='subtitulo'>Last watering</p>"
        + ultimo
        + "</div>";
    }
  }
}

var lista_desplegable = "";
function rellenarDesplegable(estaciones) {
  var lista = "";
  for (var i = 0; i < estaciones.length; i++) { lista += estaciones[i].id + ","; }
  if (lista == lista_desplegable) { return; }
  lista_desplegable = lista;

  var desplegable = document.getElementById("ws_elegida");
  var elegida = desplegable.value;
  var html = "<option value='ALL'>All stations</option>";
  for (var i = 0; i < estaciones.length; i++) {
    html += "<option value='" + estaciones[i].id + "'>" + estaciones[i].id + "  (" + estaciones[i].ubicacion + ")</option>";
  }
  desplegable.innerHTML = html;
  desplegable.value = elegida;

  var filtro_ws = document.getElementById("filtro_ws");
  var elegida_filtro = filtro_ws.value;
  var html_filtro = "<option value=''>All stations</option>";
  for (var j = 0; j < estaciones.length; j++) {
    html_filtro += "<option value='" + estaciones[j].id + "'>" + estaciones[j].id + "</option>";
  }
  filtro_ws.innerHTML = html_filtro;
  filtro_ws.value = elegida_filtro;
}


function actualizarBotones() {
  var ws = document.getElementById("ws_elegida").value;
  var regar = true, bloquear = true, activar = true;
  if (ws != "ALL") {
    var estado = "";
    for (var i = 0; i < ultimas_estaciones.length; i++) {
      if (ultimas_estaciones[i].id == ws) { estado = ultimas_estaciones[i].estado; }
    }
    regar = estado == "AVAILABLE";                                  
    bloquear = estado != "DISCONNECTED" && estado != "OUT_OF_SERVICE"; 
    activar = estado == "OUT_OF_SERVICE";                           
  }
  document.getElementById("boton_START").disabled = !regar;
  document.getElementById("boton_BLOCK").disabled = !bloquear;
  document.getElementById("boton_ACTIVATE").disabled = !activar;
}

// indica si el filtro y el tipo de mensaje coinciden
function pasaFiltro(texto) {
  var ws = document.getElementById("filtro_ws").value;
  if (ws != "" && texto.indexOf(ws) < 0) { return false; }
  if (filtro == "peticiones") { return texto.indexOf("petición") >= 0 || texto.indexOf("inicio de riego") >= 0 || texto.indexOf("Fin por") >= 0; }
  if (filtro == "fugas")      { return texto.indexOf("fuga") >= 0 || texto.indexOf("LEAK") >= 0; }
  if (filtro == "central")    { return texto.indexOf("CENTRAL -") >= 0; }
  if (filtro == "conexion")   { return texto.indexOf("autenticada") >= 0 || texto.indexOf("registrada") >= 0 || texto.indexOf("DISCONNECTED") >= 0 || texto.indexOf("Kafka") >= 0; }
  return true;  
}

function elegirFiltro(nombre) {
  filtro = nombre;
  var nombres = ["todos", "peticiones", "fugas", "central", "conexion"];
  for (var i = 0; i < nombres.length; i++) {
    document.getElementById("filtro_" + nombres[i]).className = nombres[i] == nombre ? "filtro activo" : "filtro";
  }
  pintarMensajes(ultimos_mensajes);
}

function pintarMensajes(mensajes) {
  var html = "";
  var mostrados = 0;
  for (var i = mensajes.length - 1; i >= 0; i--) {   // el mas nuevo arriba
    var texto = mensajes[i];
    if (!pasaFiltro(texto)) { continue; }
    mostrados += 1;
    var hora = texto.substring(1, 9);       
    var resto = texto.substring(11);
    var color = "";
    if (texto.indexOf("resuelta") >= 0) { color = "verde"; }
    else if (texto.indexOf("LEAK") >= 0 || texto.indexOf("fuga") >= 0) { color = "rojo"; }
    else if (texto.indexOf("DISCONNECTED") >= 0) { color = "gris"; }
    else if (texto.indexOf("perdido") >= 0) { color = "rojo"; }
    else if (texto.indexOf("DENEGADA") >= 0 || texto.indexOf("OUT_OF_SERVICE") >= 0 || texto.indexOf("no se puede") >= 0) { color = "naranja"; }
    else if (texto.indexOf("AUTORIZADA") >= 0 || texto.indexOf("autenticada") >= 0 || texto.indexOf("registrada") >= 0 || texto.indexOf("activada") >= 0 || texto.indexOf("conectada con Kafka") >= 0) { color = "verde"; }
    html += "<div class='msg'><span class='hora'>" + hora + "</span><span class='tipo " + color + "'></span><span>" + resto + "</span></div>";
  }
  if (mensajes.length == 0) { html = "<p class='aviso' style='padding:6px 0'>No activity yet. Requests, leaks and disconnections will be listed here.</p>"; }
  else if (mostrados == 0) { html = "<p class='aviso' style='padding:6px 0'>No messages match this filter.</p>"; }
  document.getElementById("mensajes").innerHTML = html;
}

function seleccionar(id) {
  seleccionada = id;
  document.getElementById("ws_elegida").value = id;
  actualizar();
}

// 
//botones del menu
var orden_pendiente = null;   //espera a que el usuario confirme
var TEXTO_CONFIRMACION = {
  "START":    { titulo: "Start watering at all stations?", texto: "Every available station will open its valve for the watering time you chose.", boton: "Start watering" },
  "BLOCK":    { titulo: "Put all stations out of service?", texto: "Any watering in progress will stop and no station will accept new requests until you activate it again.", boton: "Put out of service" },
  "ACTIVATE": { titulo: "Activate all stations?", texto: "Every station that is out of service will become available again.", boton: "Activate" }
};

function enviarOrden(orden) {
  if (document.getElementById("ws_elegida").value == "ALL") {
    orden_pendiente = orden;
    document.getElementById("conf_titulo").textContent = TEXTO_CONFIRMACION[orden].titulo;
    document.getElementById("conf_texto").textContent = TEXTO_CONFIRMACION[orden].texto;
    document.getElementById("conf_aceptar").textContent = TEXTO_CONFIRMACION[orden].boton;
    document.getElementById("conf_aceptar").className = "conf-aceptar " + orden;
    document.getElementById("confirmacion").showModal();   //se abre encima de la pagina y bloquea el resto
    return;
  }
  mandarOrden(orden);
}

function cerrarConfirmacion() {
  orden_pendiente = null;
  document.getElementById("confirmacion").close();
}

function confirmarOrden() {
  var orden = orden_pendiente;
  cerrarConfirmacion();
  mandarOrden(orden);
}

//manda orden y muestra respuesta 
function mandarOrden(orden) {
  var ws = document.getElementById("ws_elegida").value;
  var tiempo = document.getElementById("tiempo").value;
  var respuesta_orden = document.getElementById("respuesta_orden");
  respuesta_orden.textContent = "Sending order to " + ws + "...";
  respuesta_orden.className = "";
  fetch("/orden?accion=" + orden + "&ws=" + ws + "&tiempo=" + tiempo)
    .then(function (respuesta) { return respuesta.text(); })
    .then(function (texto) {
      respuesta_orden.textContent = texto;
      //si la central contesta con un problema lo pintamos en rojo
      if (texto.indexOf("Kafka no disponible") >= 0 || texto.indexOf("no valid") >= 0) { respuesta_orden.className = "fallo"; }
      else { respuesta_orden.className = "hecho"; }
      actualizar();
    })
    .catch(function () { respuesta_orden.textContent = "The order was not sent: WM_Central is not responding"; respuesta_orden.className = "fallo"; });
}


function verSeccion(nombre) {
  var secciones = ["estaciones", "mapa"];
  for (var i = 0; i < secciones.length; i++) {
    var visible = secciones[i] == nombre;
    document.getElementById("seccion_" + secciones[i]).className = visible ? "panel" : "panel escondida";
    document.getElementById("pestana_" + secciones[i]).className = visible ? "pestana activa" : "pestana";
  }
  history.replaceState(null, "", nombre == "mapa" ? "#map" : "#stations");
}

if (location.hash == "#map") { verSeccion("mapa"); }
actualizar();
setInterval(actualizar, 1000);   // repetir cada 1 segundo