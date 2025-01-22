# Banco-Base 🏦
Ejercicio práctico de ABC de pagos con integración a **Kafka** utilizando **Redpanda**.

## Pasos para la ejecución 🚀

### 1️⃣ Iniciar el contenedor de Redpanda:
```bash
docker run -d --name redpanda vectorized/redpanda:v22.2.6
```
### 2️⃣ Levantar los servicios con Docker Compose:
En la raíz del proyecto, ejecuta:
```
docker compose up -d
```
Este comando iniciará los servicios:
* Generación de la base de datos.
* Aplicación Java.
* Kafka.

### 3️⃣ Documentación de servicios:
Para consultar la documentación de los servicios, se integró Swagger (reemplazando las colecciones de Postman).
Accede en tu navegador a:
👉 http://localhost:9750/base/services/swagger-ui.html#/

### 4️⃣ Ver los logs de los servicios:
Para monitorear los logs, utiliza:
```
docker logs -f base-app-1
```
Esto incluye los logs de consumer y producer al realizar actualizaciones de estado.

5️⃣ Scripts SQL:
En la raíz del proyecto encontrarás el **_archivo init.sql_**, que contiene los scripts necesarios en caso de que desees ejecutar la base de datos localmente.

## Definición de Producer y Consumer 📡
### Configuración Kafka
```
@Configuration
public class KafkaStringConfig {

    public ProducerFactory<String, PagosDTO> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://redpanda:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://redpanda:9092");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean(name = "kafkaStringTemplate")
    public KafkaTemplate<String, PagosDTO> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```
### Consumer
```
@Component
@Slf4j
public class KafkaStringConsumer {

    @KafkaListener(topics = "TOPIC-DEMO", groupId = "group_id")
    public void consume(String message) {
        log.info("Consuming Message {}", message);
    }
}
```
### Producer
```
@Component
@Slf4j
public class KafkaStringProducer {

    private final KafkaTemplate<String, PagosDTO> kafkaTemplate;

    public KafkaStringProducer(@Qualifier("kafkaStringTemplate") KafkaTemplate<String, PagosDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(PagosDTO message, String key) {
        log.info("Producing message {}", message);
        this.kafkaTemplate.send("TOPIC-DEMO", key, message);
    }
}
```


