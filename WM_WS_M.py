import socket
import sys
import time#para poder preguntar al engine su estado

#ARGUMENTOS 
# Puerto del servidor de sockets que ofrece a WM_WS_E
# IP y puerto del WM_Central
# ID de la WS: Identificador único con que esa WS está dada de alta en la red.

HEADER = 64
FORMAT = 'utf-8'

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
    
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")
    except Exception as e:
        #TODO revisar las excepciones, y ver cual es la correcta aqui
        print("Error: No se puede conectar con la central")
        sys.exit(1) #salida  por fallo
    msg=f"AUT#{ID_WS}"
    print("Envio al servidor: ", msg)
    send(client,msg)
    respuesta=client.recv(2048).decode(FORMAT)
    print("Recibo del Servidor: ", respuesta)
    client.close()
    
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
                    
        client=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        #Pasamos el registro como mensaje
        msg=f"REGISTRO#{ID_WS}#{ubicacion}"        
        print("Envio al servidor: ",msg)
        send(client, msg)
        respuesta=client.recv(2048).decode(FORMAT)  
        print("Recibo del servidor: ",respuesta)
        client.close()
        
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
    
    conn_engine.settimeout(1)
    while True:
        try:
            send(conn_engine,"CONEXION")
            salud=receive(conn_engine)
        except OSError as e:# ver excepciones de conexion y ver cual usar
            #opcion IA, mirar si es la correcta
            salud="KO"

        if salud=="":#Por si el engine ha cerrado conex
            salud="KO"

        print(f"Engine responde: {salud}")
        #si se recibe KO enviamos mensaje de averia 
        if salud!="OK":
            print(f"Averia en {ID_WS}")
            #avisamos a la central
        
        time.sleep(1)
        
    servidor.close()#moverlo luego de hacer el aviso a la central
else:
    print ("Necesito estos argumentos: <Puerto para WM_WS_E> <Ip central> <Puerto central> <ID_WS>")