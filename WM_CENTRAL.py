import socket 
import threading
import sys
import sqlite3

#Const de configuración
HEADER = 64
FORMAT = 'utf-8'

def handle_client(conn, addr):
    
    print(f"[NUEVA CONEXION] {addr} connected.")

    msg_length = conn.recv(HEADER).decode(FORMAT)#leo los datos recibidos del cliente y los decodifico a texto
    
    if msg_length:
        
        msg_length = int(msg_length)
        msg = conn.recv(msg_length).decode(FORMAT)
        
        partes = msg.split("#")
        
        #caso de registar una 
        if len(partes) ==3:
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
                    
                    conexion = sqlite3.connect("watermanagment.db")
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
                        #TODO VER SI RENTA MAS USARLO CON UN DICCIONARIO Y ASI NO USAR ? ? ?            
                        conn.send(f"STATUS#OK#Estacion registrada correctamente".encode(FORMAT))
                    
                    conexion.close()
                    
        elif partes[0]=="AUT" and len(partes)==2:
            id_ws=partes[1]
            conexion=sqlite3.connect("watermanagment.db")
            
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
                conn.send(f"STATUS#OK#Autenticada en {ubicacion}".encode(FORMAT))
            else:
                conn.send("STATUS#ERROR#Estacion no registrada".encode(FORMAT))
            
            conexion.close()     
        else:
            print("Error intentando leer el mensaje")
            conn.send("El mensaje pasado no es correcto, formato registro: 'REGISTRO#ID_ESTACION#UBICACION', formato autenticar: 'AUT#ID_ESTACION'".encode(FORMAT))
            
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    
    while True:
        conn, addr = server.accept() # aceptamos la conexion de un cliente y devuelve un socket para comunicarse con el y la direccion del cliente
        
        thread = threading.Thread(target=handle_client,args=(conn, addr))   
        
        thread.start()
        
        CONEX_ACTIVAS = threading.active_count() # esto crea un hilo para cada cliente que se conecta y lo maneja de forma independiente 

def cargarEstaciones():
    conexion = sqlite3.connect("watermanagment.db")
    cursor= conexion.cursor()
    #las dejamos en desconectadas al iniciarlas
    cursor.execute("UPDATE estaciones SET estado=:nuevoestado", {"nuevoestado" : "DISCONNECTED"})
    
    conexion.commit()
    
    cursor.execute("SELECT * FROM estaciones")                    
    listaestaciones =cursor.fetchall()#obtengo todas las filas
    #esto devuelve [ (idestacion1,estacion1, estado), (idestacion2,estacion2, estado)]

    for estacion in listaestaciones:
        
        print(f"Id: {estacion[0]} Ubicacion: {estacion[1]} Estado: {estacion[2]}")    

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
    server.bind(ADDR) #asociamos el socket a la direccion y puerto del servidor

    print("[STARTING] Servidor inicializándose...")
    #cargamos las estaciones de la base de datos
    print("Estaciones Registradas")
    cargarEstaciones()

    start()
else:
    print("Argumentos incorrectos. Prueba: python3 WM_CENTRAL.py <Puerto> <Ip_Kafka> <Puerto_Kafka>")
