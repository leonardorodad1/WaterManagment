import socket
import sys

#ARGUMENTOS 
# Puerto del servidor de sockets que ofrece a WM_WS_E
# IP y puerto del WM_Central
# ID de la WS: Identificador único con que esa WS está dada de alta en la red.

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
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
         
    msg=f"AUT#{ID_WS}"
    print("Envio al servidor: ", msg)
    send(msg)
    respuesta=client.recv(2048).decode(FORMAT)
    print("Recibo del Servidor: ", respuesta)

    partes=respuesta.split('#')
    #TODO Hacer comprobacion de que ID_WS tiene formato WS-Numero
    #por ahora asumo que el cliente lo mete bien
    
    #si la central nos pasa STATUS#ERROR, significa que no está registrada, asi que pedimos si quiere registrarla
    if len(partes)==3 and partes[0] == "STATUS" and partes[1]=="ERROR":
        opcion=""
        while opcion.upper()!="S" and opcion.upper()!="N":
            opcion=input(f"La estacion {ID_WS} no está registrada. ¿?")
        
        ubicacion=input("Ubicación: ")
        if ubicacion == "":
            print("Ubicación no válida")
            sys.exit()
            
        client=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        #Pasamos el registro como mensaje
        msg=f"REGISTRO#{ID_WS}#{ubicacion}"        
        print("Envio al servidor: ",msg)
        send(msg)
        respuesta=client.recv(2048).decode(FORMAT)  
        print("Recibo del servidor: ",respuesta)

    client.close()
    #Si estaba registrada nos dará mensaje de STATUS#ok
    #SI O SI se va autenticar porque controlamos con msg el formato esperado
    #En wm_central se haces las validaciones

                
    #TODO PARTE DEL ENGINE

else:
    print ("Necesito estos argumentos: <Puerto para WM_WS_E> <Ip central> <Puerto central> <ID_WS>")