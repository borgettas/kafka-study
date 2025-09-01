import json
from kafka import KafkaConsumer

# Conecta-se ao Kafka e assina o tópico
consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='grupo-de-processamento-de-pedidos',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# O grupo de consumidores
group_id = 'grupo-de-processamento-de-pedidos'

print(f"[{group_id}] Iniciando o Consumer de Pedidos...")

for message in consumer:
    # A variável `message.partition` nos diz de qual partição a mensagem veio
    print(f"[{group_id}] Recebido da Partição {message.partition}:")
    print(f"  Chave (Order ID): {message.key.decode('utf-8')}")
    print(f"  Mensagem: {message.value}")
    print("-" * 20)