import os
import socket 
import threading
import sys
import sqlite3
import time
from confluent_kafka import Consumer,Producer, KafkaError
import json

#Const de configuración
HEADER = 64
FORMAT = 'utf-8'
TOPIC_PETICIONES = "wm.peticiones"
TOPIC_RESPUESTAS = "wm.respuestas"
TOPIC_ORDENES="wm.ordenes"
TOPIC_CAUDAL="wm.caudal"
ESPERA_REINTENTO_KAFKA = 5 #segundos que espera si falla la conexion con kafka antes de reintentar
DB_PATH = os.environ.get("WM_DB_PATH", "watermanagment.db")  # Ruta de la base de datos, se puede configurar mediante variable de entorno
TIEMPO_SIN_LATIDO = 6 #si una WS pasa 6 s sin mandar LATIDO la marcamos DISCONNECTED
ultimo_latido = {}

def handle_client(conn, addr):
    
    print(f"[NUEVA CONEXION] {addr} connected.")

    msg_length = conn.recv(HEADER).decode(FORMAT)#leo los datos recibidos del cliente y los decodifico a texto
    
    if msg_length:
        
        msg_length = int(msg_length)
        msg = conn.recv(msg_length).decode(FORMAT)
        
        partes = msg.split("#")
        
        #caso de registar una 
        if len(partes) ==3 and partes[0]!="AVERIA":
            tipo = partes[0]
            idestacion=partes[1]
            ubicacion=partes[2]        
            #comprobamos campos
            if tipo!="REGISTRO":
                print("Error intentando leer los argumentos")
                conn.send("Los argumentos pasados no son correctos, formato correcto: 'REGISTRO#ID_ESTACION#UBICACION'".encode(FORMAT))
            else:
                if idestacion=="" or ubicacion=="":
                    print("Error intentando leer los argumentos")
                    conn.send("Los argumentos pasados no son correctos, formato correcto: 'REGISTRO#ID_ESTACION#UBICACION'".encode(FORMAT))
                else:
                    print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
                    print(f"Estacion: {idestacion}")
                    print(f"Ubicacion: {ubicacion}")
                    #compruebo que no existe ya en la base de datos
                    
                    conexion = sqlite3.connect(DB_PATH)
                    cursor= conexion.cursor()
                    
                    cursor.execute("SELECT * FROM estaciones")                    
                    listaestaciones =cursor.fetchall()#obtengo todas las filas
                    #esto devuelve [ (idestacion1,estacion1, estado), (idestacion2,estacion2, estado)]

                    existe=False
                    if len(listaestaciones)!=0:
                        #buscamos si hay algo asi:
                        #('WS-04', 'River Park', 'AVAILABLE') buscamos id
                        for estacion in listaestaciones:
                            if estacion[0] == idestacion:
                                existe=True
                                break
                    #si existe una id igual, no insertamos 
                    if existe:
                        print("Error intentando introducir a la bd")
                        conn.send(f"El id introducido ya está registrado".encode(FORMAT))
                    else:                                
                        #lo meto a la base de datos
                        cursor.execute("INSERT INTO estaciones (id_estacion, ubicacion, estado) VALUES (?,?,'AVAILABLE')",(idestacion,ubicacion))
                        conexion.commit()
                        ultimo_latido[id_ws]=time.time()
                        #TODO VER SI RENTA MAS USARLO CON UN DICCIONARIO Y ASI NO USAR ? ? ?            
                        conn.send(f"STATUS#OK#Estacion registrada correctamente".encode(FORMAT))
                    
                    conexion.close()
                    
        elif partes[0]=="AUT" and len(partes)==2:
            id_ws=partes[1]
            conexion=sqlite3.connect(DB_PATH)
            
            cursor=conexion.cursor()
            cursor.execute("SELECT * FROM estaciones")                    

            listaestaciones=cursor.fetchall()
            
            existe=False
            #buscamos la estaicon
            for estacion in listaestaciones:
                if estacion[0]==id_ws:
                    existe=True
                    ubicacion=estacion[1]
                    break
            if existe:
                #en caso de que exista, acutalizamos estaddo
                cursor.execute("UPDATE estaciones SET estado='AVAILABLE' where id_estacion=:idbuscado", {"idbuscado" : id_ws})
                conexion.commit()
                ultimo_latido[id_ws]=time.time()
                conn.send(f"STATUS#OK#Autenticada en {ubicacion}".encode(FORMAT))
            else:
                conn.send("STATUS#ERROR#Estacion no registrada".encode(FORMAT))
            
            conexion.close()   
        #CONTROLO CASO DE AVERIA RECIBO AVERIA#IDESTACION#ESTADO
        elif partes[0]=="AVERIA" and len(partes)==3:
            
            id_ws=partes[1]
            estado_ws=partes[2]
            conexion=sqlite3.connect(DB_PATH)
            
            cursor=conexion.cursor()
            if estado_ws=="LEAK":
                
                cursor.execute("UPDATE estaciones set estado='LEAK' where id_estacion=:idbuscado",{"idbuscado": id_ws})
                print(f"Estación {id_ws} cambia su estado a LEAK, fuga detectada")
            elif estado_ws=="AVAILABLE":
                cursor.execute("UPDATE estaciones set estado='AVAILABLE' where id_estacion=:idbuscado",{"idbuscado": id_ws})
                print(f"Estación {id_ws} cambia su estado a AVAILABLE, fuga resuelta")
            conexion.commit()
            cursor.close()
            conn.send(f"STATUS#OK#Estado de {id_ws} cambia a {estado_ws}".encode(FORMAT))
            conexion.close()
        elif partes[0]=="LATIDO" and len(partes)==2:
            id_ws=partes[1]
            ultimo_latido[id_ws]=time.time()
            
            conexion=sqlite3.connect(DB_PATH)
            cursor=conexion.cursor()
            cursor.execute("SELECT estado FROM estaciones where id_estacion=:idbuscado", {"idbuscado": id_ws})
            fila=cursor.fetchone()
            conexion.close()
            #si la teniamos como desconectada se lo decimos para que vuelva a autenticarse
            if fila is not None and fila[0]=="DISCONNECTED":
                conn.send("STATUS#ERROR#DISCONNECTED".encode(FORMAT))
            else:
                conn.send("STATUS#OK#Latido recibido".encode(FORMAT))
        else:
            print("Error intentando leer el mensaje")
            conn.send("El mensaje pasado no es correcto, formato registro: 'REGISTRO#ID_ESTACION#UBICACION', formato autenticar: 'AUT#ID_ESTACION'".encode(FORMAT))
            
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()

def atender_client(conn, addr):
    #Envoltorio: un cliente que se comporta mal (mensaje raro, corte de red...)
    #solo afecta a su propio hilo y la central sigue atendiendo al resto
    try:
        handle_client(conn, addr)
    except (OSError, ValueError, sqlite3.Error) as e:
        print(f"[ERROR] Atendiendo a {addr}: {e}")
    finally:
        conn.close()
  


def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    
    while True:
        conn, addr = server.accept() # aceptamos la conexion de un cliente y devuelve un socket para comunicarse con el y la direccion del cliente
        
        thread = threading.Thread(target=atender_client,args=(conn, addr))   
        
        thread.start()
        
        CONEX_ACTIVAS = threading.active_count() # esto crea un hilo para cada cliente que se conecta y lo maneja de forma independiente 

def cargarEstaciones():
    conexion = sqlite3.connect(DB_PATH)
    cursor= conexion.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS estaciones (id_estacion TEXT PRIMARY KEY,ubicacion TEXT NOT NULL,estado TEXT NOT NULL)")

    #añadimos columnas para controlar caudal y volumen
    
    cursor.execute("PRAGMA table_info(estaciones)") #devuelve una fila por cada columna, la tupla tendrá nºcol nombre tipo ...
    filas = cursor.fetchall()
    columnas = []
    for fila in filas:
        columnas.append(fila[1]) #nos guardamos los nombres de los atributos que ya existen

    # añadimos solo las que faltan
    if "caudal" not in columnas:
        cursor.execute("ALTER TABLE estaciones ADD COLUMN caudal REAL NOT NULL DEFAULT 0")
    if "volumen" not in columnas:
        cursor.execute("ALTER TABLE estaciones ADD COLUMN volumen REAL NOT NULL DEFAULT 0")
    if "operador" not in columnas:
        cursor.execute("ALTER TABLE estaciones ADD COLUMN operador TEXT")
    
    #esto se hace asi para no modificar el volume de railway ni perder datos
    
    #las dejamos en desconectadas al iniciarla
    cursor.execute("UPDATE estaciones SET estado='DISCONNECTED', caudal=0, volumen=0, operador='-'")
    
    conexion.commit()
    
    cursor.execute("SELECT * FROM estaciones")                    
    listaestaciones =cursor.fetchall()#obtengo todas las filas
    #esto devuelve [ (idestacion1,estacion1, estado), (idestacion2,estacion2, estado)]

    for estacion in listaestaciones:
        
        print(f"Id: {estacion[0]} Ubicacion: {estacion[1]} Estado: {estacion[2]} Caudal: {estacion[3]} L/min Volumen: {estacion[4]} L Operador: {estacion[5]}")    

#Devolvemos el resultado de la peticion del FO dependiendo del estado de la estacion
#Autorizada si estacion dispo
#Denegada si no esta dispo
def evaluarPeticion(peticion):
    conexion = sqlite3.connect(DB_PATH)
    try:
        cursor = conexion.cursor()
        #intentamos reservar la estacion: solo cambia si esta AVAILABLE en este mismo instante
        cursor.execute("UPDATE estaciones SET estado='WATERING', operador=:op, caudal=0, volumen=0 WHERE id_estacion=:idbuscado AND estado='AVAILABLE'",{"op": peticion["operador"], "idbuscado": peticion["ws"]})
        conexion.commit()
        
        if cursor.rowcount == 1:
            return "AUTORIZADA", "estacion disponible"
        #si no se ha podido reservar miramos el porque
        cursor.execute("SELECT estado FROM estaciones where id_estacion=:idbuscado", {"idbuscado": peticion["ws"]})
        fila=cursor.fetchone()
    finally:
        conexion.close()
    
    if fila is None:
        return "DENEGADA", "estación no registrada"
    return "DENEGADA", f"estación en estado {fila[0]}"

#dejamos en el wm.respuesta un mensajae para el FO que hizo la peticion
def responderPeticion(producer,peticion,res,motivo):

    respuesta={"operador": peticion["operador"], 
               "ws":peticion["ws"],
               "ts" : peticion["ts"], 
               #usamos mismo ts para quedarnos luego en el operador el resultado correcto
               "resultado": res,
               "motivo": motivo}
    producer.produce(TOPIC_RESPUESTAS, key=peticion["operador"], value = json.dumps(respuesta))

def procesarPeticion(consumer, producer, msg):
    try:
        peticion=json.loads(msg.value().decode(FORMAT))
        operador = peticion["operador"]
        ws=peticion["ws"]
        peticion["ts"]#comprobamos que existe
        tiempo=int(peticion["tiempo"])
    except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, AttributeError,ValueError):
        #mensaje mal formado: lo descartamos y lo damos por leido para no atascarnos con el
        print("Formato de peticion incorrecto, se descarta")
        consumer.commit(message=msg, asynchronous=False)
        return
    
    print(f"Peticion de {operador} para {ws}")
    responderPeticion(producer,peticion,"RECIBIDA","Peticion recibida, comprobando estado")
 
    resultado,motivo=evaluarPeticion(peticion)
    print(f"Peticion de {operador} para {ws} -> {resultado}, {motivo}")        
    responderPeticion(producer,peticion, resultado,motivo)
    
    if resultado=="AUTORIZADA":
        orden={"ws": ws, "orden": "START", "tiempo": tiempo, "operador": operador,
               "ts": time.time(), "ts_peticion": peticion["ts"]}
        producer.produce(TOPIC_ORDENES, key=ws, value=json.dumps(orden))
    #enviamos las respuestas
    pendientes = producer.flush(10)
    if pendientes == 0:
        #solo damos la peticion por leida cuando ya hemos respondido
        consumer.commit(message=msg, asynchronous=False)
    else:
        print("No se ha podido responder al operador (Kafka no disponible)")

def procesarCaudal(consumer, producer,msg):
    try:
        dato=json.loads(msg.value().decode(FORMAT))
        ws=dato["ws"]
        tipo=dato["tipo"]
        operador=dato["operador"]
        ts=dato["ts_peticion"]
    except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, AttributeError):
        print("Mensaje de caudal incorrecto, se descarta")
        consumer.commit(message=msg, asynchronous=False)
        return
    
    peticion={"operador": operador, "ws":ws,"ts":ts} #para respomder al fo que lo ha pedido
    conexion=sqlite3.connect(DB_PATH)
    try:
        cursor=conexion.cursor()
        
        if tipo=="CAUDAL":
            #solo guardamos si la estación sigue en watering
            
                cursor.execute("UPDATE estaciones SET caudal=:c,volumen=:v WHERE id_estacion=:ws AND estado='WATERING'", {"c":dato['caudal'], "v":dato['volumen'],"ws":ws})
                conexion.commit()
                if cursor.rowcount==1:
                    print(f"{ws} regando: {dato['caudal']} L/m, {dato['volumen']}")
                    responderPeticion(producer, peticion, "REGANDO", f"Caudal {dato['caudal']} L/min, volumen {dato['volumen']} L")
                
        elif tipo=="FIN":
            if dato["motivo"]!="FUGA":
                cursor.execute("UPDATE estaciones SET estado='AVAILABLE' WHERE id_estacion=:ws AND estado='WATERING'", {"ws":ws})
            cursor.execute("UPDATE estaciones SET caudal=0, volumen=0, operador='-' WHERE id_estacion=:ws", {"ws": ws})
            conexion.commit()
            resumen=f"Fin por {dato['motivo']}. Duración {dato['duracion']} s. Volumen total {dato['volumen']} L"
            responderPeticion(producer, peticion, "FINALIZADO", resumen)
    finally:
        conexion.close()
    
    if producer.flush(10)==0:
        consumer.commit(message=msg,asynchronous=False)
            
def escucharPeticiones():
    while True:
        consumer = None
        try:
            consumer= Consumer({
                "bootstrap.servers":f"{IP_KAFKA}:{PORT_KAFKA}",
                "group.id":"wm-central",
                "auto.offset.reset":"earliest",
                "enable.auto.commit": False
            })
            producer=Producer({"bootstrap.servers":f"{IP_KAFKA}:{PORT_KAFKA}"})
            consumer.subscribe([TOPIC_PETICIONES])
            print(f"Escuchando el topic {TOPIC_PETICIONES}...")
            
            while True:
                msg=consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() not in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF):
                        print(f"[KAFKA] {msg.error()}")
                    continue
                
                procesarPeticion(consumer, producer, msg)
                
        except Exception as e:
            print(f"[KAFKA] Error en el hilo de peticiones: {e}. Reintentando en {ESPERA_REINTENTO_KAFKA} s")
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        time.sleep(ESPERA_REINTENTO_KAFKA)
        
def escucharCaudal():
    while True:
        consumer = None
        try:
            consumer= Consumer({
                "bootstrap.servers":f"{IP_KAFKA}:{PORT_KAFKA}",
                "group.id":"wm-central-caudal",
                "auto.offset.reset":"earliest",
                "enable.auto.commit": False
            })
            producer=Producer({"bootstrap.servers":f"{IP_KAFKA}:{PORT_KAFKA}"})
            consumer.subscribe([TOPIC_CAUDAL])
            print(f"Escuchando el topic {TOPIC_CAUDAL}...")
            
            while True:
                msg=consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() not in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF):
                        print(f"[KAFKA] {msg.error()}")
                    continue
                
                procesarCaudal(consumer, producer, msg)
                
        except Exception as e:
            print(f"[KAFKA] Error en el hilo de caudal: {e}. Reintentando en {ESPERA_REINTENTO_KAFKA} s")
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
        time.sleep(ESPERA_REINTENTO_KAFKA)    

        
def escucharLatidos():
    while True:
        ahora=time.time()
        for id_ws in list(ultimo_latido.keys()):
            if ahora-ultimo_latido[id_ws] > TIEMPO_SIN_LATIDO:
                conexion=sqlite3.connect(DB_PATH)
                cursor=conexion.cursor()
                cursor.execute("UPDATE estaciones SET estado='DISCONNECTED' WHERE id_estacion=:idbuscado", {"idbuscado": id_ws})
                conexion.commit()
                conexion.close()
                del ultimo_latido[id_ws] #ya esta marcada, dejamos de vigilarla hasta que vuelva
                print(f"Estación {id_ws} sin latido desde hace {TIEMPO_SIN_LATIDO} s -> DISCONNECTED")
        time.sleep(1)


######################### MAIN ##########################

#Encargada de recibir pediciones y gestionar las conexiones con los clientes

if len(sys.argv)== 4:
    
    #puerto de escucha que ofrece al ws_wm_m
    PORT= int(sys.argv[1])
    
    SERVER = "0.0.0.0"#Acepta conexiones de cualquier red
    ADDR = (SERVER, PORT)
    
    IP_KAFKA=sys.argv[2]
    PORT_KAFKA=int(sys.argv[3])
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #reinicia central sin esperar a que el sistema libere el puerto
    server.bind(ADDR) #asociamos el socket a la direccion y puerto del servidor

    print("[STARTING] Servidor inicializándose...")
    #cargamos las estaciones de la base de datos
    print("Estaciones Registradas")
    cargarEstaciones()

    #HILO POR CADA PETICION
    hilo_kafka = threading.Thread(target=escucharPeticiones, daemon=True)
    hilo_kafka.start()
    
    hilo_caudal = threading.Thread(target=escucharCaudal, daemon=True)
    hilo_caudal.start()

    hilo_latidos = threading.Thread(target=escucharLatidos, daemon=True)
    hilo_latidos.start()

    start()
else:
    print("Argumentos incorrectos. Prueba: python3 WM_CENTRAL.py <Puerto> <Ip_Kafka> <Puerto_Kafka>")
