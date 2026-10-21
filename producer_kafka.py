
from confluent_kafka import Producer
import time
import json
import sys

TOPIC = "wm.peticiones"
# Callback function called on delivery success or failure
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if len(sys.argv) != 5:
    print("Uso: python producer_kafka.py <ip_kafka> <puerto_kafka> <id_operador> <id_ws>")
    sys.exit(1)
 
IP_K, PORT_K, OPERADOR, WS = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

producer = Producer({
    "bootstrap.servers": f"{IP_K}:{PORT_K}"})
 
peticion = {"operador": OPERADOR, "ws": WS, "tiempo": 30, "ts": time.time()}
print(f"Envío petición: {peticion}")
 
producer.produce(TOPIC, key=OPERADOR, value=json.dumps(peticion), callback=delivery_report)
 
pendientes = producer.flush(10)
if pendientes > 0:
    print("Imposible conectar con Kafka: la petición no se ha enviado.")
    sys.exit(1)