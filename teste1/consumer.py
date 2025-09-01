import json
from kafka import KafkaConsumer

# Conecta-se ao Kafka e assina o tópico
consumer = KafkaConsumer(
    'logs-de-aplicacao',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='meu-grupo-de-processamento',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Iniciando o Consumer...")

for message in consumer:
    log_data = message.value
    print("Novo log recebido:")
    print(f"  ID: {log_data['id']}")
    print(f"  Timestamp: {log_data['timestamp']}")
    print(f"  Mensagem: {log_data['message']}")
    print("-" * 20)