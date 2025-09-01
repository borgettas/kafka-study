import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Conecta-se ao Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Iniciando o Producer...")

for i in range(10):
    log_data = {
        'id': i,
        'timestamp': str(datetime.now()),
        'level': 'INFO',
        'message': f'Log de teste #{i}'
    }
    
    # Envia o log para o tópico `logs-de-aplicacao`
    producer.send('logs-de-aplicacao', value=log_data)
    print(f"Enviado: {log_data}")
    time.sleep(1)

producer.flush()
print("Producer finalizado.")