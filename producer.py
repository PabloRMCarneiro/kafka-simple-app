
import argparse 
import random 
from datetime import datetime 
import string 
from kafka import KafkaProducer 
import time 

# Definindo o endereço do servidor Kafka e o tópico a ser utilizado
BOOTSTRAP_SERVERS = 'localhost:9092'  # O servidor Kafka a ser conectado.
TOPIC = 'sensor_data'  # O tópico Kafka onde as mensagens serão publicadas.

# Função para gerar uma string aleatória de um determinado comprimento
def random_string(length):
    letters = string.ascii_lowercase  # Define o alfabeto como sendo todas as letras minúsculas.
    # Gera e retorna uma string composta de 'length' letras aleatórias.
    return ''.join(random.choice(letters) for i in range(length))

# A função principal que será chamada quando o script for executado
def main(sensor_id, frequency):
    # Inicializa um produtor Kafka com o servidor especificado
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        # Loop infinito para continuar enviando dados até que o programa seja interrompido
        while True:
            # Gera um valor aleatório entre -100 e 100 e arredonda para 3 casas decimais
            value = round(random.uniform(-100, 100), 3)
            # Obtém o carimbo de tempo atual em formato ISO 8601
            timestamp = datetime.now().isoformat()
            # Monta a mensagem a ser enviada para o Kafka
            message = f"{sensor_id} | {timestamp} | {value}"
            # Envia a mensagem para o tópico especificado, codificada em UTF-8
            producer.send(TOPIC, message.encode('utf-8'))
            # Garante que todas as mensagens pendentes sejam enviadas
            producer.flush()
            # Aguarda um tempo definido pela frequência antes de enviar a próxima mensagem
            time.sleep(frequency / 1000.0) 
    except KeyboardInterrupt:
        # Captura o evento de interrupção do teclado (Ctrl+C)
        pass
    finally:
        # Garante que o produtor seja fechado e limpe todos os recursos ao sair
        producer.close()

if __name__ == "__main__":
    # Configura o parser de argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Simulador de sensor para Kafka.')
    # Adiciona a opção de linha de comando para especificar o ID do sensor
    parser.add_argument('--sensor_id', type=str, default=random_string(10),
                        help='O ID do sensor.')
    # Adiciona a opção de linha de comando para especificar a frequência das leituras do sensor
    parser.add_argument('--frequency', type=int, default=1000,
                        help='A frequência em milissegundos das leituras do sensor.')

    # Analisa os argumentos da linha de comando
    args = parser.parse_args()
    # Chama a função principal com os argumentos fornecidos
    main(args.sensor_id, args.frequency)
