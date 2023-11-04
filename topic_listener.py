# Importando as bibliotecas necessárias
from kafka import KafkaConsumer

# Definição das constantes para conexão com o Kafka
BOOTSTRAP_SERVERS = 'localhost:9092'  # Endereço dos servidores Kafka.
TOPIC = 'sensor_data'  # Nome do tópico de onde as mensagens serão consumidas.

# Definição de códigos de cores ANSI para formatar a saída com cores no terminal
BLUE = '\033[94m'
GREEN = '\033[92m'
RED = '\033[91m'
ENDC = '\033[0m'  # Código para resetar a cor

# Inicializando o consumidor Kafka
consumer = KafkaConsumer(
    TOPIC,  # Nome do tópico.
    bootstrap_servers=BOOTSTRAP_SERVERS,  # Lista de servidores Kafka aos quais conectar.
    auto_offset_reset='latest',  # Começa a consumir desde a mensagem mais antiga. Outras opções: 'earliest' -> mais antiga
    group_id=None  # Sem grupo, para que todas as mensagens sejam lidas individualmente.
)

print("Iniciando o consumo de mensagens do tópico 'sensor_data'...")

# Função auxiliar para imprimir mensagens com cores
def print_colored_message(sensor_id, timestamp, value):
    print(f"{BLUE}{sensor_id}{ENDC} | {GREEN}{timestamp}{ENDC} | {RED}{value}{ENDC}")

# Loop para consumir mensagens continuamente
for message in consumer:
    # Decodifica a mensagem de bytes para string
    decoded_message = message.value.decode('utf-8')
    # Divide a mensagem em partes baseando-se no delimitador ' | '
    parts = decoded_message.split(' | ')
    # Checa se a mensagem está no formato esperado antes de imprimir
    if len(parts) == 3:
        sensor_id, timestamp, value = parts
        print_colored_message(sensor_id, timestamp, value)
    else:
        print("Mensagem recebida em formato desconhecido:", decoded_message)

# A execução do script pode ser parada com Ctrl+C.
