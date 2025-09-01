import json
import time
from kafka import KafkaProducer
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

print("Iniciando o Producer de Pedidos...")

order_ids = ['100', '101', '102', '103', '104', '105']

for i in range(20):
    order_id = random.choice(order_ids)
    order_data = {
        'order_id': order_id,
        'item': f'item-{i}',
        'quantity': random.randint(1, 10),
        'timestamp': time.time()
    }
    
    producer.send('pedidos', key=order_id, value=order_data)
    print(f"Enviado para o pedido {order_id}: {order_data}")
    time.sleep(0.5)

producer.flush()
print("Producer finalizado.")