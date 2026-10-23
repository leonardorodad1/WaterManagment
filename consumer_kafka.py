from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import json
import sqlite3

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',   # Kafka broker
    'group.id': 'my-group',                   # Consumer group id
    'auto.offset.reset': 'earliest',          # Start reading at the earliest offset
    'enable.auto.commit': True                # Enable auto-commit of offsets
}

TOPIC = "wm.peticiones"
DB = "WM_Central/watermanagment.db"
 
 
def evaluar(peticion):
    """Devuelve (resultado, motivo) consultando la BD de la central."""
    conexion = sqlite3.connect(DB)
    try:
        cursor = conexion.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS estaciones (id_estacion TEXT PRIMARY KEY,ubicacion TEXT NOT NULL,estado TEXT NOT NULL)")
        cursor.execute("SELECT estado FROM estaciones WHERE id_estacion=?", (peticion["ws"],))
        fila = cursor.fetchone()
    finally:
        conexion.close()
 
    if fila is None:
        return "DENEGADA", "estación no registrada"
    if fila[0] != "AVAILABLE":
        return "DENEGADA", f"estación en estado {fila[0]}"
    return "AUTORIZADA", "estación disponible"
 
 
if len(sys.argv) != 3:
    print("Uso: python consumer_kafka.py <ip_kafka> <puerto_kafka>")
    sys.exit(1)
 
consumer = Consumer({
    "bootstrap.servers": f"{sys.argv[1]}:{sys.argv[2]}",
    "group.id": "wm-central",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([TOPIC])
print(f"Escuchando el topic {TOPIC}...")
 
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
 
        if msg.error():
            codigo = msg.error().code()
            if codigo == KafkaError._PARTITION_EOF:
                continue
            if codigo == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print("El topic aún no existe (nadie ha producido todavía). Esperando...")
                continue
            raise KafkaException(msg.error())
 
        try:
            peticion = json.loads(msg.value().decode("utf-8"))
            resultado, motivo = evaluar(peticion)
            print(f"Petición de {peticion['operador']} para {peticion['ws']}: {resultado} ({motivo})")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Mensaje mal formado, se descarta: {e}")
 
        consumer.commit(message=msg, asynchronous=False)
 
except KeyboardInterrupt:
    pass
finally:
    consumer.close()