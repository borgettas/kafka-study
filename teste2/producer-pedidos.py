import json
import time
from kafka import KafkaProducer
import random

# Conecta-se ao Kafka broker
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print("Iniciando o Producer de Pedidos...")

for i in range(20):
    order_id = random.randint(100, 105) # Simula pedidos de 6 clientes diferentes
    order_data = {
        'order_id': order_id,
        'item': f'item-{i}',
        'quantity': random.randint(1, 10)
    }
    
    # Envia o pedido para o tópico 'pedidos', usando order_id como a chave
    producer.send('pedidos', key=order_id, value=order_data)
    print(f"Enviado para o pedido {order_id}: {order_data}")
    time.sleep(0.5)

producer.flush()
print("Producer finalizado.")