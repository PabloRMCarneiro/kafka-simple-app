import argparse 
from kafka import KafkaConsumer, TopicPartition 
from collections import deque

# Configuração do servidor de inicialização do Kafka - é o endereço do(s) corretor(es) Kafka
BOOTSTRAP_SERVERS = 'localhost:9092'
# O tópico Kafka de onde consumir as mensagens
TOPIC = 'sensor_data'

# Códigos de cores do terminal para impressão bonita no console.
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ENDC = '\033[0m'  

def process_message(message, sensor_id):
    message_text = message.value.decode('utf-8')
    if f"{sensor_id} |" in message_text:
        parts = message_text.split(' | ')
        if len(parts) == 3:
            sensor, timestamp, value = parts
            print(f"{BLUE}{sensor}{ENDC} | {GREEN}{timestamp}{ENDC} | {RED}{value}{ENDC}")


# Função para consumir todas as mensagens disponíveis no tópico para o sensor_id fornecido
def consume_all_messages(sensor_id):
    # Inicializando o consumidor Kafka para o tópico especificado
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',  # Começa a consumir desde a primeira mensagem
        enable_auto_commit=False  # Desabilita o commit automático dos offsets
    )
    try:
        # Processando continuamente as mensagens recebidas
        for message in consumer:
            process_message(message, sensor_id)
    finally:
        # Fechando o consumidor ao terminar de processar
        consumer.close()

# Função para consumir as últimas 'n' mensagens do tópico para o sensor_id fornecido
def consume_last_n_messages(sensor_id, last_n):
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    
    # Buscando o último offset
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    offset = max(0, last_offset - last_n)
    consumer.seek(tp, offset)
    
    try:
        for _ in range(last_n):
            message = next(consumer)
            process_message(message, sensor_id)
    finally:
        consumer.close()

# Função para consumir mensagens em tempo real do tópico para o sensor_id fornecido
def consume_real_time(sensor_id):
    # Inicializando o consumidor Kafka para o tópico especificado
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',  # Começa a consumir a partir da última mensagem
        group_id=None  # Não usa um group_id, consumindo independentemente
    )
    # Informando ao usuário que o consumo em tempo real está ativo
    print("Consumindo mensagens em tempo real...")
    # Processando continuamente as mensagens recebidas
    for message in consumer:
        process_message(message, sensor_id)

# Função principal que configura o argparse e inicia o processo de consumo
def main():
    # Configuração do argparse para aceitar argumentos da linha de comando
    parser = argparse.ArgumentParser(description='Consumidor Kafka para dados de sensor.')
    # Adicionando argumentos esperados
    parser.add_argument('sensor_id', type=str, help='O ID do sensor para filtrar as mensagens.')
    parser.add_argument('function', type=str, choices=['all', 'last', 'realtime'],
                        help='Função a ser executada: all = consumir todas as mensagens, '
                             'last = consumir as últimas N mensagens, realtime = consumir em tempo real.')
    parser.add_argument('last_n', nargs='?', type=int, default=10,
                        help='Número das últimas mensagens a serem consumidas (somente para a função last).')

    # Analisando os argumentos fornecidos
    args = parser.parse_args()

    # Direcionando para a função apropriada baseada no argumento 'function'
    if args.function == 'all':
        consume_all_messages(args.sensor_id)
    elif args.function == 'last':
        consume_last_n_messages(args.sensor_id, args.last_n)
    elif args.function == 'realtime':
        consume_real_time(args.sensor_id)

if __name__ == "__main__":
    main()
