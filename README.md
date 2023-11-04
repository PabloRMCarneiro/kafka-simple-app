## Baixar o Kafka

`wget https://dlcdn.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz`

## Descompactar o Kafka

`tar -xzf kafka_2.13-3.6.0.tgz`

## Acessar o diretório do Kafka

`cd kafka_2.13-3.6.0`

## Iniciar o Zookeeper

`bin/zookeeper-server-start.sh config/zookeeper.properties`

## Iniciar o Kafka

`bin/kafka-server-start.sh config/server.properties`

## Criar o tópico

`bin/kafka-topics.sh --create --topic TOPIC_NAME --bootstrap-server localhost:9092`

Para o código foi utilizado o tópico `sensor_data`

# Executar o Produtor

`python3 producer.py --sensor_id SENSOR_ID --frequency FREQUENCY`

`SENSOR_ID` é o id do sensor que será utilizado para enviar os dados
`FREQUENCY` é a frequência em mili segundos que os dados serão enviados

# Executar o Consumidor

### Realtime

Fica escutando o tópico e imprime os dados em tempo real para o sensor especificado

`python3 consumer.py SENSOR_ID realtime`

`SENSOR_ID` é o id do sensor que será utilizado para receber os dados

### Last N

Imprime os últimos N dados do tópico para o sensor especificado

`python3 consumer.py SENSOR_ID last N`

`N` é a quantidade de dados que serão impressos

### All

Imprime todos os dados do tópico para o sensor especificado

`python3 consumer.py SENSOR_ID all`