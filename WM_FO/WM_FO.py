#solicita la activación el riego de una estación determinada
#enviará peticion a la central y esta autoriza o deniega
import threading
from confluent_kafka import Consumer, Producer, KafkaError
import sys
import json
import time
#ARGUMENTOS
#IP y puerto del Broker/Bootstrap-server del gestor de colas.
# ID del operador: Identificador único con que ese operador está dado de alta en CENTRAL.

FORMAT="utf-8"
TOPIC_PETICIONES="wm.peticiones"
TOPIC_RESPUESTAS="wm.respuestas"
ESPERA_RESPUESTA=4 # 4segundo es los maximo que espera sea exito o fracaso
TIMEOUT_RESPUESTA = 15 #max tiempo que se espera respuesta de la central
RESULTADOS_FINALES = {"AUTORIZADA", "DENEGADA"}

resultados = {} 
condicion = threading.Condition()
detener = threading.Event()

#va leyendo las respuestas de la central y las muestra por pantalla
def escucharRespuestas(consumer):
    try:
        while not detener.is_set():
            try:
                msg=consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() not in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF):
                        print(f"[KAFKA] {msg.error()}")
                    continue
                
                respuesta=json.loads(msg.value().decode(FORMAT))
                if respuesta.get("operador")!=ID_OPERADOR or respuesta.get("ts",0)<INICIO:
                    continue
                
                print(f"\n[CENTRAL] {respuesta['ws']}: {respuesta['resultado']} - {respuesta['motivo']}")
                with condicion:
                    resultados[respuesta["ts"]]=respuesta["resultado"]
                    condicion.notify_all()
            except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError):
                continue #mensaje mal formado lo ignoramos
            except Exception as e:
                print(f"[KAFKA] Error leyendo respuestas: {e}")
                time.sleep(2)
    finally:
        consumer.close()
 
#solicitar a central un suministro en un ws

def solicitarRiego(ws, tiempo):
    
    peticion={"operador": ID_OPERADOR, "ws":ws, "tiempo":tiempo,"ts":time.time()}
    print(f"Envio la peticion: {peticion}")

    try:
        producer.produce(TOPIC_PETICIONES,key=ID_OPERADOR, value=json.dumps(peticion))
    except BufferError:
        print("Imposible enviar la peticion a Kafka, buffer lleno")
        return None

    pendientes=producer.flush(10)
    if pendientes>0:
        print("Imposible conectar con kafka")
        producer.purge()
        producer.flush(1)
        return None
    return peticion["ts"]

#espera a que la central conteste a la peticion con ese ts
#devuelve el resultado, o None si no contesta a tiempo
def esperarResultado(ts):
    limite=time.time()+TIMEOUT_RESPUESTA
    with condicion:
        while resultados.get(ts) not in RESULTADOS_FINALES:
            restante=limite-time.time()
            if restante<=0:
                return None
            condicion.wait(restante)
        return resultados[ts]
 


#leer un fichero todos los servicios que va a solicitar
#cada linea tendrá un servicio, formato ID-WS#TIEMPO
def leerFichero(nombre):
    
    try:
        fichero=open(nombre)
    except OSError:
        print(f"No se puede abrir el fichero {nombre}")
        sys.exit(1)
    
    lineas=fichero.readlines()
    fichero.close()

    #recorro las lineas del fichero y solicito uno a uno
    for linea in lineas:
        linea=linea.strip()
        if linea=="":
            continue
        
        partes=linea.split('#')
       
        if len(partes)!=2:
            print("Linea no valida")
            continue #nos slatamos la linea
        ws=partes[0]
        try:
            tiempo=int(partes[1])
        except ValueError:
            print(f"Linea no valida")
            continue # nos la saltamos si da error
        #solicitamos servicio 
        print(f"Servicio del fichero: {ws} durante {tiempo} segundos")
        ts=solicitarRiego(ws,tiempo)
        
        #esperamos antes de leer la siguiente linea y pedir el sefivico
        if ts is not None:
            print("Esperando a terminar la peticion...")
            if esperarResultado(ts) is None:
                print("Sin respuesta de WM_Central (puede estar caida). Paso al siguiente servicio.")
        
        #esperamos antes de pedir el siguiente servicio
        time.sleep(ESPERA_RESPUESTA)


######################### MAIN ##########################

if len(sys.argv)==4 or len(sys.argv)==5:
    
    IP_K = sys.argv[1]
    PORT_K = int(sys.argv[2])
    ID_OPERADOR = sys.argv[3]
    FICHERO = sys.argv[4] if len(sys.argv)==5 else None
    INICIO=time.time()
    producer = Producer({"bootstrap.servers": f"{IP_K}:{PORT_K}"})

    #usamos un group id distinto por cada ejecucion
    #asi sabremos cual es nuestra peticion
    consumer = Consumer({
        "bootstrap.servers": f"{IP_K}:{PORT_K}",
        "group.id": f"wm-fo-{ID_OPERADOR}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_RESPUESTAS])
    hilo_respuestas=threading.Thread(target=escucharRespuestas, args=(consumer,), daemon=True)
    hilo_respuestas.start()

    if FICHERO is not None:
        leerFichero(FICHERO)

    salir=False
    
    while not salir:
        
        print("1. Solicitar riego")
        print("2. Leer servicios de un fichero")
        print("3. Salir")
        opcion = input("Opción: ")

        if opcion == "1":
            ws = input("ID de la estación: ")
            if ws == "":
                print("ID no válido")
                continue
            try:
                tiempo = int(input("Tiempo de riego (segundos): "))
            except ValueError:
                print("Tiempo no válido")
                continue
            if tiempo <= 0:
                print("Tiempo no válido")
                continue
            ts=solicitarRiego(ws, tiempo)
            
            if ts is not None:
                if  esperarResultado(ts) is None:
                    print("Sin respuesta de Central")
                    
                #Dependiendo del riego seguiremos
            
        elif opcion == "2":
            nombre = input("Nombre del fichero: ")
            #mirar si pongo caso de q se lea por argumento o no
            leerFichero(nombre)
        elif opcion == "3":
            salir = True
        else:
            print("Opción no válida")
            
    detener.set()#paro el hilo
    hilo_respuestas.join(timeout=3)

else:
    print("Necesito estos argumentos: <IP_KAFKA> <PORT_KAFKA> <ID_OPERADOR> [fichero]")
        
