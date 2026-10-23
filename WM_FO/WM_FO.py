#solicita la activación el riego de una estación determinada
#enviará peticion a la central y esta autoriza o deniega
from confluent_kafka import Consumer, Producer
import sys
import json
import time
#ARGUMENTOS
#IP y puerto del Broker/Bootstrap-server del gestor de colas.
# ID del operador: Identificador único con que ese operador está dado de alta en CENTRAL.

FORMTAT="utf-8"
TOPIC_PETICIONES="wm.peticiones"
TOPIC_RESPUESTAS="wm.respuestas"
ESPERA_RESPUESTA=4 # 4segundo es los maximo que espera sea exito o fracaso


#solicitar a central un suministro en un ws

def solicitarRiego(ws, tiempo):
    
    peticion={"operador": ID_OPERADOR, "ws":ws, "tiempo":tiempo,"ts":time.time()}
    print(f"Envio la peticion: {peticion}")
    
    producer.produce(TOPIC_PETICIONES,key=ID_OPERADOR, value=json.dumps(peticion))
    

    pendientes=producer.flush(10)
    if pendientes>0:
        print("Imposible conectar con kafka")
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
        ws=partes[0]
        if len(partes)!=2:
            print("Linea no valida")
            continue #nos slatamos la linea
        if len(partes)==2:
            try:
                tiempo=int(partes[1])
            except ValueError:
                print(f"Linea no valida")
                continue # nos la saltamos si da error
        #solicitamos servicio 
        print(f"Servicio del fichero: {ws} durante {tiempo} segundos")
        solicitarRiego(ws,tiempo)
        
        #esperamos antes de leer la siguiente linea y pedir el sefivico
        print(f"Esperando a terminar la peticion...")
        time.sleep(1)


######################### MAIN ##########################

if len(sys.argv)==4:
    
    IP_K = sys.argv[1]
    PORT_K = int(sys.argv[2])
    ID_OPERADOR = sys.argv[3]
    #preguntar si pasamos fichero como argumento o que, no entiendo
    #como leer el fichero si no se por donde lo paso
    #en la interfaz o mejor con un arg en terminal

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
            solicitarRiego(ws, tiempo)
        elif opcion == "2":
            nombre = input("Nombre del fichero: ")
            #mirar si pongo caso de q se lea por argumento o no
            leerFichero(nombre)
        elif opcion == "3":
            salir = True
        else:
            print("Opción no válida")
            
    consumer.close()

else:
    print("Necesito estos argumentos: <IP_KAFKA> <PORT_KAFKA> <ID_OPERADOR> [fichero]")
        
