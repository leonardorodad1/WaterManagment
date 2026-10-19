#Módulo que recibe la información del caudalímetro y activa la electroválvula.
#Se conecta al monitor
import socket
import sys
import  threading

#ARGUMENTOS
# IP y puerto del Broker/Bootstrap-server del gestor de colas.
# IP y puerto del Monitor del WS

HEADER=64
FORMAT = "utf-8"
estado="OK" #vairable globar para controlar el estado 
#nos permite actualizar en tiempo de ejecución el estado

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
    while True:
        print("Pulsa Enter para alternar OK -> KO")
        #si usamos
        #input("Pulsa Enter para alternar OK->KO")
        # se queda pillado en caso de que hagamos control+c en la terminal del monitor
        sys.stdin.readline()

        if estado=="KO":
            estado="OK"
        else:
            estado="KO"
        print(f"Estado: {estado}")

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

    #TODO permitir que en tiempo de ejecucion se pulse una tecla 
    # para reportar el KO al monitor y q este envie el msg averia a central
    
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