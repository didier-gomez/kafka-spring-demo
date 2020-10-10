# kafka-spring-demo
Demo de curso de apache kafka con spring boot
(
# Prerequisitos
1. Tener instalado java 8+
2. Instalar [kafka](https://kafka.apache.org/downloads)

# Instrucciones de uso
1. `cd <path_to_kafka_dir>`
2. Correr zookeeper y kafka:
  * windows 
  ```
    .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    .\bin\windows\kafka-server-start.bat .\config\server.properties
  ```
  * linux/mac 
  ```
  bin/zookeeper-server-start.sh config/zookeeper.properties
  bin/kafka-server-start.sh config/server.properties
  ```
3. cd <path/to/kafka-spring-demo>
4. Ejecutar `mvnw spring-boot:run`
