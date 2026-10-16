import sqlite3
#abrimos base de datos
conexion=sqlite3.connect("watermanagment.db")
#creamos curson para ejecutrar las consultas
cursor=conexion.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS estaciones (id_estacion TEXT PRIMARY KEY,ubicacion TEXT NOT NULL,estado TEXT NOT NULL)")

print("Base de datos abierta")
conexion.close()