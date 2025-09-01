import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='grupo-processamento-paralelo',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

group_id = 'grupo-processamento-paralelo'
print(f"[{group_id}] Iniciando o Consumer...")

for message in consumer:
    print(f"[{group_id}] Recebido na Partição {message.partition}:")
    print(f"  Chave (Order ID): {message.key.decode('utf-8')}")
    print(f"  Mensagem: {message.value}")
    print("-" * 20)