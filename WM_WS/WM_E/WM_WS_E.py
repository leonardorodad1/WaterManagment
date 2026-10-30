#Módulo que recibe la información del caudalímetro y activa la electroválvula.
#Se conecta al monitor
import socket
import sys
import  threading
import time
import random
import json
from confluent_kafka import Consumer, Producer,KafkaError

#ARGUMENTOS
# IP y puerto del Broker/Bootstrap-server del gestor de colas.
# IP y puerto del Monitor del WS

HEADER=64
FORMAT = "utf-8"
estado="OK" #vairable globar para controlar el estado 
#nos permite actualizar en tiempo de ejecución el estado

ID_WS=None #nos la manda el monitor cuando nos conectamos
valvula="CERRADA" #electrovalvula: "CERRADA" O "ABIERTA" 
regando=False #si hay riego activo se pone a true

pararRiego=threading.Event() #encargado de parar riego

TOPIC_ORDENES="wm.ordenes" #central deja ordenes en este topic
TOPIC_CAUDAL="wm.caudal"
TIEMPO_CADUCIDAD=20 # 20 segundos es lo maximo que espera 
#una orden para ser usada, si no lo es en este tiempo 
#se da la orden como caducada


def send(objeto_socket,msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    objeto_socket.send(send_length)
    objeto_socket.send(message)

def receive(objeto_socket):
    msg_length=objeto_socket.recv(HEADER).decode(FORMAT)
    if msg_length=="":
        return "" # en caso de q cierre monitro, termino
    msg_length=int(msg_length)
    return objeto_socket.recv(msg_length).decode(FORMAT)

    
def leerTeclado():
    global estado
    print("Pulsa Enter para alternar OK -> KO")
    print("Pulsa f y Enter para finalizar riego")
    while True:
        
        #si usamos
        #input("Pulsa Enter para alternar OK->KO")
        # se queda pillado en caso de que hagamos control+c en la terminal del monitor
        linea=sys.stdin.readline()
        if linea=="":
            #no hay reclado, si no el \n se lee como 
            return
        linea=linea.strip().lower()   #quitamos el salto de linea
        if linea=="":           
            if estado=="KO":
                estado="OK"
            else:
                estado="KO"
                pararRiego.set()# se corta si estaba regando
            print(f"Estado: {estado}")
        elif linea=="f":
            if regando:
                print("Finalizando el riego...")
            pararRiego.set()
        else:
            print("Opción no válida")
############################
#Caudalimetro
############################

#Mido caudal solo si la válvula esta abierta
def medirCaudal():
    
    if valvula!="ABIERTA":
        return 0.0
    return round(random.uniform(5,20),1)
    #simulamos una medida 

############################
#Conexion con la central, envio de la info por kafka
############################

def publicar(dato):
    try:
        producer.produce(TOPIC_CAUDAL, key=ID_WS, value=json.dumps(dato))
        producer.poll(0)
    except BufferError:
        print("Kafka: buffer lleno, no se ha podido enviar")
 
def enviarCaudal(caudal,volumen,operador,ts):
    print(f"Caudal: {caudal} L/min Volumen acumulado: {volumen} L")
    publicar({"ws":ID_WS,"tipo":"CAUDAL","caudal":caudal,"volumen":round(volumen,2),"operador":operador,"ts_peticion":ts})

def enviarFinRiego(resumen,operador,ts):
    print("RESUMEN RIEGO")
    print(f"Fin de riego por {resumen['motivo']}")
    print(f"Duración: {resumen['duracion']} segundos")
    print(f"Volumen total: {resumen['volumen']} L")
    
    publicar({"ws":ID_WS,"tipo":"FIN","motivo":resumen["motivo"],"duracion":resumen["duracion"],"volumen":round(resumen["volumen"],2),"operador":operador,"ts_peticion":ts})
    producer.flush(5) #esperamos a que salga y asi no perdemos el riego 
    
######################
#Riego
######################

#lo ejecutamos en su hilo para que 
# siga funcionando el bucle de salud de monitor-engine 

def regar(tiempo,operador,ts):
    
    global regando,valvula
    
    pararRiego.clear() #quitamos peticion anterior    
    volumen=0.0
    inicio=time.time()
    motivo="TIEMPO" #si riega los x segundos, 
    #se acaba por tiempo, si pasa algo cambiamos motivo
    
    #abrimos valvula
    valvula="ABIERTA"
    print("Electroválvula abierta")
    
    for segundo in range(tiempo):
        
        if estado=="KO":#fuga
            motivo="FUGA"
            break
        
        caudal=medirCaudal()
        volumen+=caudal/60 # L/min aqui es caudal/60 segundos
        enviarCaudal(caudal,volumen,operador,ts)
        if pararRiego.wait(1):# si alguien activa parar riego en ese sec
            if estado=="KO":
                motivo="FUGA"
            else:
                motivo="PARADAMANUAL"#operador ha pulsado f en el menu
            break
    valvula="CERRADA"
    print("Electroválvula cerrada")

    resumen={"motivo":motivo, "duracion":round(time.time()-inicio), "volumen":volumen}
    enviarFinRiego(resumen,operador,ts)
    
    regando=False
        
def iniciarRiego(tiempo,operador, ts):
    
    global regando
    if regando:
        print("Ya hay un riego en curso, ignoro la orden")
        return
    regando=True
    #lanzamos regar en un hilo propio
    threading.Thread(target=regar,args=(tiempo,operador,ts),daemon=True).start()

######################
#Kafka
######################

#Hilo que lee topic y se queda con las ordenes 
# de la estacion asignada

def escucharOrdenes(consumer):
    
    while True:
        try:
            msg=consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                #el topic no existe hasta la primera orden, no es un error real
                if msg.error().code() not in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._PARTITION_EOF):
                    print(f"{msg.error()}")
                continue
 
            orden=json.loads(msg.value().decode(FORMAT))

            if orden['ws']!=ID_WS:        
                continue
                #es para otra estacion
            if time.time()-orden['ts']>TIEMPO_CADUCIDAD:
                continue
                #orden vieja
            print(f"Orden recibida de la Central: {orden}")
            #iniciar riego
            if orden["orden"]=="START":
                iniciarRiego(int(orden["tiempo"]),orden["operador"],orden["ts_peticion"])
        except(json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue#ignoramos mensaje no valido
                            
        except Exception as e:

            print(f"Error leyendo ordenes {e}")
            time.sleep(1)


#MAIN
if  (len(sys.argv) == 5):
    IP_K = sys.argv[1]
    PORT_K = int(sys.argv[2])

    IP_M=sys.argv[3]
    PORT_M =int(sys.argv[4])
        
    #Conectamos al monitor
    monitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Estableciendo conexión con el monitor")
    ADDR = (IP_M, PORT_M)
    try:
        monitor.connect(ADDR)

    except Exception as e:
        print("Erorr al intentar conectarse al Monitor")
        sys.exit(1)

    print (f"Establecida conexión en [{ADDR}]")
    
    #asigno id del monitor al engine
    asignacion=receive(monitor)
    partes=asignacion.split('#')
    
    if partes[0]=="ID":
        ID_WS=partes[1]
        print(f"Engine asignado a {ID_WS}")  
    
    #conectamos con Kafka: productor (caudal) y consumidor (órdenes)
    producer=Producer({"bootstrap.servers": f"{IP_K}:{PORT_K}"})
    consumer=Consumer({
        "bootstrap.servers": f"{IP_K}:{PORT_K}",
        "group.id": f"wm-engine-{ID_WS}-{int(time.time())}", 
        #Cada engine recibe todas las órdenes y filtra las suyas
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ORDENES])
    hilo_ordenes=threading.Thread(target=escucharOrdenes, args=(consumer,), daemon=True)
    hilo_ordenes.start()
    
    print(f"Escuchando órdenes de la Central en {TOPIC_ORDENES}...")    
    
    respuesta_teclado=threading.Thread(target=leerTeclado, daemon=True)
    #si el programa principal termina lo mato con daemon=true
    respuesta_teclado.start()
    while True:
        try:
            msg=receive(monitor)
        except OSError:
            msg=""
        if msg=="":
            print("El monitor ha cerrado conexión")
            break               
        if msg=="CONEXION":
            send(monitor,estado)
    
    monitor.close()
    sys.exit(1)
    
else:
    print ("Parece que algo falló. Necesito estos argumentos: <IP_KAFKA> <PORT_KAFKA> <IP_MONITOR> <PORT_MONITOR>")