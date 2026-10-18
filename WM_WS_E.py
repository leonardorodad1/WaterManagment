#Módulo que recibe la información del caudalímetro y activa la electroválvula.
#Se conecta al monitor
import socket
import sys
import threading


#ARGUMENTOS
#Puerto del servidor de sockets que ofrece a WM_WS_E
#IP y puerto del WM_Central
#ID de la WS: Identificador único con que esa WS está dada de alta en la red.

HEADER=64
FORMAT = "utf-8"


def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    

if  (len(sys.argv) == 5):
    IP_K = sys.argv[1]
    PORT_K = int(sys.argv[2])

    IP_M=sys.argv[3]
    PORT_M =int(sys.argv[4])
        
    #Conectamos al monitor
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Estableciendo conexión con el monitor")
    ADDR = (IP_K, PORT_K)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

        
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR) #asociamos el socket a la direccion y puerto del servidor

    client.close()
else:
    print ("Parece que algo falló. Necesito estos argumentos: <IP_KAFKA> <PORT_KAFKA> <IP_MONITOR> <PORT_MONITOR>")