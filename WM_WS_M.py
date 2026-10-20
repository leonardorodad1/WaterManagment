import socket
import sys
import time#para poder preguntar al engine su estado

#ARGUMENTOS 
# Puerto del servidor de sockets que ofrece a WM_WS_E
# IP y puerto del WM_Central
# ID de la WS: Identificador único con que esa WS está dada de alta en la red.

HEADER = 64
FORMAT = 'utf-8'
TIMEOUT_CENTRAL = 3  # segundos máximos esperando a la central

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
        return "" #si engine cierra
    msg_length=int(msg_length)
    return objeto_socket.recv(msg_length).decode(FORMAT)


def enviar_a_central(addr, msg):
    """Abre una conexión con la central, envía msg y devuelve la respuesta.
    Devuelve None si no se ha podido hablar con la central (caída, red, timeout...).
    Nunca lanza excepción de red: el que llama decide qué hacer."""
    central = None
    try:
        central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        central.settimeout(TIMEOUT_CENTRAL)
        central.connect(addr)
        send(central, msg)
        respuesta = central.recv(2048).decode(FORMAT)
        if respuesta == "":
            return None  # la central cerró sin contestar
        return respuesta
    except (OSError, ValueError):
        # OSError cubre ConnectionRefusedError, ConnectionResetError, timeout, etc.
        return None
    finally:
        if central is not None:
            central.close()
 

########## MAIN ##########

#Encargado de leer los argumentos y pasar el mensaje a la central

if  (len(sys.argv) == 5):
    #puerto que ofrece a engine
    PORT_M=int(sys.argv[1])
    #Ip y puerto de la central
    IP_C = sys.argv[2]
    PORT_C = int(sys.argv[3])
    ADDR = (IP_C, PORT_C)
    ID_WS=sys.argv[4]
    
    msg = f"AUT#{ID_WS}"
    print("Envio al servidor: ", msg)
    respuesta = enviar_a_central(ADDR, msg)
    if respuesta is None:
        print("Error: Imposible conectar con WM_Central")
        sys.exit(1)
    print("Recibo del Servidor: ", respuesta)
        
    partes=respuesta.split('#')
    #TODO Hacer comprobacion de que ID_WS tiene formato WS-Numero
    #por ahora asumo que el cliente lo mete bien
    
    #si la central nos pasa STATUS#ERROR, significa que no está registrada, asi que pedimos si quiere registrarla
    if len(partes)==3 and partes[0] == "STATUS" and partes[1]=="ERROR":
        opcion=""
        while opcion.upper()!="Y" and opcion.upper()!="N":
            opcion=input(f"La estacion {ID_WS} no está registrada. ¿Quieres registrarla? (Y/N): ")
        
        if opcion.upper()=="N":
            sys.exit(0)# se ha salido con exito
        
        ubicacion=input("Ubicación: ")
        if ubicacion == "":
            print("Ubicación no válida")
            sys.exit(1)#salida por fallo
        for i in ubicacion:
            if not (i.isalpha() or i==" "):
                print("Ubicación no válida")
                sys.exit(1)#salida por fallo
        #preguntar a victorai si le parece mejor q termine programa o hacer la validacion
        # en un while y asi q la ubicacion sea valida
                    
        msg = f"REGISTRO#{ID_WS}#{ubicacion}"
        print("Envio al servidor: ", msg)
        respuesta = enviar_a_central(ADDR, msg)
        if respuesta is None:
            print("Error: Imposible conectar con WM_Central")
            sys.exit(1)
        print("Recibo del servidor: ", respuesta)
            
    #Si estaba registrada nos dará mensaje de STATUS#ok
    #SI O SI se va autenticar porque controlamos con msg el formato esperado
    #En wm_central se hacen las validaciones y aqui controlamos que la ubicaicon sea valida
    #así el registro siempre va a ser valido
    
    #de todas formas comprobamos la respuesta por si acaso
    partes=respuesta.split('#')
    if not (len(partes)==3 and partes[0]=="STATUS" and partes[1]=="OK"):
        print("La central no ha aceptado la estación. Cierro el monitor.")
        sys.exit(1)
        
    #TODO PARTE DEL ENGINE
    #servidor para el engine
    servidor=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ADDR_ENGINE=("0.0.0.0",PORT_M)
    servidor.bind(ADDR_ENGINE) # con esta ip podrá escuchar desde otros pc y docker
    servidor.listen()
    print(f"Esperando al Engine en el puerto {PORT_M}")            
    
    conn_engine, addr_engine= servidor.accept()
    print(f"Engine conectado desde {addr_engine}")
    
    #enviamos un mensaje cada sec para comprobar la salud
    #si no recibo respuesta enviamos a central mensaje de averia
    
    estado_real = "OK"        # lo que dice el engine ahora mismo
    estado_notificado = "OK"  # lo último que la central sabe con seguridad
    central_caida = False     # True si falló la última comunicación con la central
    conn_engine.settimeout(1)
    while True:
        try:
            send(conn_engine,"CONEXION")
            salud=receive(conn_engine)
        except (OSError, ValueError):# ver excepciones de conexion y ver cual usar
            #opcion IA, mirar si es la correcta
            salud="KO"

        if salud=="":#Por si el engine ha cerrado conex
            salud="KO"

        print(f"Engine responde: {salud}")
        #si se recibe KO enviamos mensaje de averia 
        estado_real = "OK" if salud == "OK" else "KO"

        # Si la central estaba caída, intentamos reautenticarnos
        if central_caida:
            resp = enviar_a_central(ADDR, f"AUT#{ID_WS}")
            if resp is None:
                print("Imposible conectar con WM_Central. Reintentando...")
            else:
                print("Recibo del Servidor: ", resp)
                print("Conexión con WM_Central restablecida")
                central_caida = False
                estado_notificado = "OK"   # tras el AUT la central pone AVAILABLE

        # Si hay un cambio pendiente de notificar, lo intentamos
        if not central_caida and estado_real != estado_notificado:
            if estado_real == "KO":
                print(f"Averia en {ID_WS}. Avisando a la Central...")
                msg = f"AVERIA#{ID_WS}#LEAK"
            else:
                print(f"Avería resuelta en {ID_WS}. Pasando de LEAK -> AVAILABLE")
                msg = f"AVERIA#{ID_WS}#AVAILABLE"

            print("Envio al servidor: ", msg)
            resp = enviar_a_central(ADDR, msg)
            if resp is None:
                central_caida = True   # NO tocamos estado_notificado: se reintenta
                print("Imposible conectar con WM_Central. Se reintentará el aviso.")
            else:
                estado_notificado = estado_real
                print("Recibo del Servidor: ", resp) 
            
        time.sleep(1)
        
    servidor.close()#moverlo luego de hacer el aviso a la central
else:
    print ("Necesito estos argumentos: <Puerto para WM_WS_E> <Ip central> <Puerto central> <ID_WS>")