#Módulo que recibe la información del caudalímetro y activa la electroválvula.
import socket
import sys

#Puerto del servidor de sockets que ofrece a WM_WS_E
#IP y puerto del WM_Central
#ID de la WS: Identificador único con que esa WS está dada de alta en la red.

if  (len(sys.argv) == 5):
    IP_K = sys.argv[1]
    PORT_K = int(sys.argv[2])
    ADDR = (IP_K, PORT_K)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    IP_M=sys.argv[3]
    PORT_M = sys.argv[4]
        
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR) #asociamos el socket a la direccion y puerto del servidor

    
    
    
    client.close()
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <IP_KAFKA> <PORT_KAFKA> <IP_MONITOR> <PORT_MONITOR>")