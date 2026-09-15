# CDC Target Sync Service (Java & MS SQL Server)

Микросервис на Spring Boot для высокопроизводительной, тип-безопасной синхронизации данных Change Data Capture (CDC) из Kafka (Avro) в MS SQL Server в режиме реального времени.

---

## 🛠 Технологический стек

* **Java 17**
* **Spring Boot 2.6.3**
* **Spring Kafka** & **Apache Avro**
* **Spring JDBC (`JdbcTemplate`)**
* **MS SQL Server 2019+**

---

## 🏛 Архитектура и зоны ответственности

Микросервис принимает входящий поток CDC-событий в формате Apache Avro, выполняет метаданные-ориентированное приведение типов и выполняет динамические транзакционные операции в MS SQL Server.
```
[Kafka Topic: cdc.events]
│
▼ (ConsumerRecord<GenericRecord, GenericRecord>)
┌────────────────────────────────────────────────────────┐
│ GenericCdcConsumer                                     │
│  ├── Извлечение метаданных (__schema, __table, __op)   │
│  └── Извлечение Primary Keys из Kafka Key              │
└────────────────────────────────────────────────────────┘
│
▼
┌────────────────────────────────────────────────────────┐
│ AvroTypeResolver                                       │
│  └── Конвертация типов Avro (LogicalTypes, UUID, Date) │
└────────────────────────────────────────────────────────┘
│
▼ (Map<String, Object>)
┌────────────────────────────────────────────────────────┐
│ GenericSyncService                                     │
│  ├── Генерация MERGE (UPSERT) / DELETE                 │
│  └── Управление SET IDENTITY_INSERT                    │
└────────────────────────────────────────────────────────┘
│
▼ (JdbcTemplate)
[MS SQL Server Target]
```
---

## ⚡ Ключевые инженерные решения

* **Метаданные-ориентированное приведение типов (`AvroTypeResolver`)**: Отказ от хардкода по именам колонок. Все Avro-типы (`LogicalTypes`, `connect.name`) динамически приводятся к соответствующим типам Java/JDBC (`DATETIMEOFFSET`, `UUID`, `DECIMAL`, `BIGINT`).
* **Динамический UPSERT (`MERGE`)**: Построение атомарных `MERGE`-запросов.
* **Управление `IDENTITY_INSERT`**: Автоматическое включение и выключение `SET IDENTITY_INSERT [schema].[table] ON/OFF` в рамках транзакции `MERGE`, что позволяет сохранять оригинальные идентификаторы записей без нарушения автоинкремента MS SQL.
* **Поддержка составных ключей и таблиц-связок**: Динамическое извлечение первичных ключей из структуры Kafka Key и корректная обработка таблиц без колонок обновления (пропуск секции `WHEN MATCHED THEN UPDATE SET`).

---

## 📄 Структура пакетов
```
src/main/java/ru/ldv/debezium/
├── configurations/
│   └── KafkaConsumerConfiguration.java     # Настройка приёмника kafka
├── resolvers/
│   └── AvroTypeResolver.java               # Приведение Avro-типов в Java-объекты
├── kafka/
│   ├── DltKafkaListener.java               # Kafka Listener dlt топика 
│   └── GenericCdcConsumer.java             # Kafka Listener, фильтрация и маршрутизация
├── services/
│   └── GenericSyncService.java             # Генератор SQL-запросов и работа с JdbcTemplate
└── DebeziumSyncApplication.java
```
---

## ⚙ Конфигурация Spring Boot (`application.yml`)

```yaml
spring:
  # Настройки подключения к базе-приёмнику MS SQL Server
  datasource:
    url: jdbc:sqlserver://localhost:1433;databaseName=target_db;encrypt=false;trustServerCertificate=true
    username: sa
    password: YourStrongPassword123
    driver-class-name: com.microsoft.sqlserver.jdbc.SQLServerDriver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5

  # Настройки интеграции с Apache Kafka и Confluent Schema Registry
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: mssql-cdc-sync-group
      auto-offset-reset: earliest
      enable-auto-commit: false  # Используется ручной Acknowledgment в коде

      # Использование KafkaAvroDeserializer для десериализации ключей и значений
      key-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer

      # Дополнительные свойства Confluent Avro и Schema Registry
      properties:
        schema.registry.url: http://localhost:8081
        specific.avro.reader: false

    listener:
      ack-mode: manual_immediate # Моментальное подтверждение смещения после ack.acknowledge()

# Пользовательские настройки CDC-топика
cdc:
  topic: cdc.events
  dlt-topic: cdc.events-dlt
  retry:
    max-attempts: 3
    backoff-interval-ms: 2000
```

## 📝 Формируемый SQL-шаблон

Для входящего CDC-события GenericSyncService генерирует следующий SQL-запрос для JdbcTemplate (пример):
```amplicode sql
SET IDENTITY_INSERT [catalog].[products] ON;

MERGE INTO [catalog].[products] AS target
USING (
    SELECT 
        ? AS [id], 
        ? AS [sku], 
        ? AS [name], 
        ? AS [supplier_id], 
        ? AS [price], 
        ? AS [updated_at]
) AS source ON target.[id] = source.[id]
WHEN MATCHED THEN UPDATE SET 
    target.[sku] = source.[sku], 
    target.[name] = source.[name], 
    target.[supplier_id] = source.[supplier_id], 
    target.[price] = source.[price], 
    target.[updated_at] = source.[updated_at]
WHEN NOT MATCHED THEN INSERT ([id], [sku], [name], [supplier_id], [price], [updated_at]) 
VALUES (source.[id], source.[sku], source.[name], source.[supplier_id], source.[price], source.[updated_at]);

SET IDENTITY_INSERT [catalog].[products] OFF;
```