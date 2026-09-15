# CDC-стенд: PostgreSQL → Debezium → Kafka

Стенд для отладки CDC: кластер PostgreSQL (мастер и две реплики), Debezium в
Kafka Connect и один топик в Kafka, куда складываются все изменения. Ничего не
автоматизировано: поднимаете compose, ставите коннектор одним curl, дальше сами
пишете SQL и смотрите, что приехало в Kafka.

```
        ┌──────────────┐   физическая репликация   ┌───────────────┐
        │  pg-master   │ ────────────────────────► │ pg-replica-1  │
        │  (источник)  │ ────────────────────────► │ pg-replica-2  │
        └──────┬───────┘                           └───────────────┘
               │ логический слот dbz_cdc_slot (pgoutput)
               ▼
        ┌──────────────┐        ┌───────────┐      ┌─────────────────┐
        │   connect    │ ─────► │   kafka   │      │ schema-registry │
        │  (Debezium)  │        │ cdc.events│ ◄──► │   Avro-схемы    │
        └──────────────┘        │ 1 партиция│      └─────────────────┘
                                └───────────┘
```

Изменения всех таблиц всех схем идут в один топик `cdc.events` с одной
партицией, в том же порядке, в котором они попали в WAL. На этом порядке всё и
держится: приёмник применяет события подряд и не разбирается с foreign key.

## Что нужно на машине

Docker с плагином compose (`docker compose version`), ~4 ГБ памяти для Docker
и свободные порты: 55432–55434 (PostgreSQL), 29092 (Kafka), 8081 (Schema
Registry), 8083 (Connect), 8088 (Kafka UI). Порты меняются в `.env`.

Ещё понадобится `python3`, но только для выгрузки контрактов из Schema
Registry (см. «Своя таблица и её контракт»). Сам стенд без него работает.

## Порядок действий

```
docker compose up -d --build     поднять
docker compose ps                убедиться, что всё встало
psql: pg_stat_replication        реплики стримятся
psql: pg_replication_slots       слоты на месте
curl POST /connectors            поставить коннектор
kafka-avro-console-consumer      открыть просмотр топика
psql: INSERT ...                 внести изменения и увидеть события
```

Ниже то же самое, но подробно.

## 1. Поднять

```bash
docker compose up -d --build
```

`--build` нужен только в первый раз: образ Connect собирается из
`Dockerfile.connect`, который тянет плагин Debezium с Maven Central.

```bash
docker compose ps
```

Должно быть 7 контейнеров в состоянии `Up`. Пометку `healthy` имеют четыре из
них — `pg-master`, `kafka`, `schema-registry`, `connect`. У реплик и Kafka UI
healthcheck'а нет, у них просто `Up`.

На первом старте, пока том мастера пуст, `pg-master` прогоняет
[init/01-init.sql](init/01-init.sql): роли, три схемы, семь таблиц, публикацию
и слоты репликации. Второй раз этот файл не запускается — Postgres выполняет
`/docker-entrypoint-initdb.d` только на пустом каталоге данных.

## 2. Проверить, что кластер живой

Реплики должны стримиться с мастера:

```bash
docker compose exec pg-master psql -U postgres -d cdc -c "SELECT application_name, state, sync_state FROM pg_stat_replication;"
```

Ждём две строки, `pg-replica-1` и `pg-replica-2`, обе в `streaming`.

Слоты на мастере:

```bash
docker compose exec pg-master psql -U postgres -d cdc -c "SELECT slot_name, slot_type, active, failover FROM pg_replication_slots;"
```

Два физических (`replica_1_slot`, `replica_2_slot`) и логический
`dbz_cdc_slot` с `failover = t`. То, что логический пока `active = f`, —
нормально: коннектора ещё нет.

Копия логического слота на реплике появляется через несколько секунд после
старта:

```bash
docker compose exec pg-replica-1 psql -U postgres -d cdc -c "SELECT slot_name, synced, failover FROM pg_replication_slots;"
```

## 3. Поставить коннектор

Конфиг лежит в [connector/pg-cdc.json](connector/pg-cdc.json). Это обычный
JSON, никаких подстановок, ставится одним запросом:

```bash
curl -sS -X POST -H "Content-Type: application/json" --data @connector/pg-cdc.json http://localhost:8083/connectors
```

```bash
curl -sS http://localhost:8083/connectors/pg-cdc/status
```

Нужен `"state":"RUNNING"` у коннектора и у единственной задачи. После этого
слот занят коннектором:

```bash
docker compose exec pg-master psql -U postgres -d cdc -c "SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'dbz_cdc_slot';"
```

## 4. Открыть просмотр топика

Команда блокирующая — запускайте в отдельном терминале и оставляйте висеть,
она печатает события по мере появления (выход по Ctrl+C):

```bash
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:9092 --property schema.registry.url=http://schema-registry:8081 --topic cdc.events --from-beginning
```

Сначала она выдаст несколько десятков строк своего лога — это не ошибки, у
консольного консьюмера log4j пишет в stdout. Потом замолчит и будет ждать: пока
событий нет, печатать нечего. Перезапускать её не надо, всё появится в этом же
окне.

Топика `cdc.events` изначально нет, его создаёт первый подключившийся клиент —
в том числе сам консьюмер. Так что пустой топик до первого события — это норма,
а не поломка.

### `cleanup.policy` у `cdc.events` обязан быть `delete`

Топик создаётся автосозданием и наследует настройки брокера. Значение по
умолчанию — `delete`, то есть «хранить всё подряд, удалять старое по времени и
размеру». Это единственный допустимый режим для `cdc.events`: приёмнику нужна
вся цепочка событий по строке, а не только её последнее состояние.

Проверять всё равно стоит — вместе с числом партиций это два параметра, которые
портятся необратимо:

```bash
docker compose exec kafka kafka-configs --bootstrap-server kafka:9092 --entity-type topics --entity-name cdc.events --describe --all | grep cleanup.policy
```

Ожидается `cleanup.policy=delete`. Если топик почему-то создан с `compact`,
политику можно переключить на лету:

```bash
docker compose exec kafka kafka-configs --bootstrap-server kafka:9092 --entity-type topics --entity-name cdc.events --alter --add-config cleanup.policy=delete
```

Но уже уплотнённые сегменты не восстановятся: события, которые compaction успел
выбросить, потеряны. Если он поработал по топику — честный путь один: `down -v`
и заново (шаг 8).

Топик можно создать и явно, до первого клиента, — тогда обе настройки задаются
сразу и автосоздание не участвует:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic cdc.events --partitions 1 --replication-factor 1 --config cleanup.policy=delete
```

## 5. Внести изменения и увидеть их

```bash
docker compose exec pg-master psql -U postgres -d cdc
```

Вставляем данные сверху вниз по цепочке FK:

```sql
INSERT INTO ref.currencies (code, name) VALUES ('USD', 'US Dollar');
INSERT INTO ref.regions (id, code, name, currency_code) VALUES (1, 'EU', 'Europe', 'USD');
INSERT INTO catalog.suppliers (name, region_id) VALUES ('ACME', 1);
INSERT INTO catalog.products (sku, name, supplier_id, currency_code, price)
       VALUES ('SKU-1', 'Widget', 1, 'USD', 19.99);
INSERT INTO sales.customers (email, name, region_id) VALUES ('a@b.c', 'Alice', 1);
INSERT INTO sales.orders (customer_id, currency_code, total) VALUES (1, 'USD', 19.99);
INSERT INTO sales.order_items (order_id, product_id, qty, price) VALUES (1, 1, 1, 19.99);
```

В консьюмере появятся семь сообщений в том же порядке. Формат плоский: поля
строки плюс служебные с префиксом `__`. Значения в фигурных скобках вида
`{"string": ...}` — это то, как Avro-консьюмер печатает nullable-поля:

```json
{"id":1,"sku":"SKU-1","name":"Widget","supplier_id":1,"currency_code":"USD",
 "price":"<сырые байты>","description":null,"updated_at":"2026-09-09T13:53:23.209285Z",
 "__deleted":{"string":"false"},"__op":{"string":"c"},
 "__table":{"string":"products"},"__schema":{"string":"catalog"},
 "__lsn":{"long":67110408},"__txId":{"long":760},"__ts_ms":{"long":1788962003693}}
```

Вместо цены в выводе будет мусор из непечатаемых символов, и так задумано:
`numeric(18,4)` едет как `bytes` с `logicalType=decimal`, консьюмер печатает
сырые байты. Приёмник соберёт из них точное значение. Читаемое число дал бы
`decimal.handling.mode=double`, но он молча портит копейки, а это банковские
данные.

Дальше можно пробовать что угодно:

```sql
UPDATE catalog.products SET price = 24.50 WHERE sku = 'SKU-1';   -- __op = u
DELETE FROM sales.order_items WHERE id = 1;                      -- __op = d, __deleted = true
```

У события удаления заполнен только первичный ключ, остальные поля нулевые
(`"order_id":0`). Так и должно быть: при `REPLICA IDENTITY DEFAULT` в WAL
попадает только PK, а для `DELETE ... WHERE id = ?` большего и не нужно.

Транзакция видна целиком — у событий один `__txId`:

```sql
BEGIN;
INSERT INTO sales.orders (customer_id, currency_code, total) VALUES (1, 'USD', 5);
INSERT INTO sales.order_items (order_id, product_id, qty, price) VALUES (2, 1, 1, 5);
COMMIT;
```

## 6. То же самое в Kafka UI

http://localhost:8088 — там видны топики, сообщения, схемы и состояние
коннектора.

Чтобы убедиться, что сообщение приехало:

1. **Topics** → `cdc.events`. В списке смотрим на счётчик сообщений и число
   партиций: партиция должна быть одна.
2. Вкладка **Messages**. Сообщения сначала показываются как строки с мусором —
   UI по умолчанию берёт сериализатор String.
3. Переключите **Key Serde** и **Value Serde** на `SchemaRegistry` (кнопка с
   настройками рядом с фильтром) и обновите список. Теперь каждое сообщение
   читается как JSON: поля строки, `__op`, `__schema`, `__table`, `__lsn`.

Почему не сразу: UI ищет схему в subject'е `cdc.events-value`, а у нас включена
`TopicRecordNameStrategy`, и схемы лежат по таблицам —
`cdc.events-dbz.catalog.products.Value` и так далее. Сам поиск по id из
сообщения работает, надо только выбрать нужный сериализатор.

Ещё полезное в UI: **Schema Registry** — список схем по таблицам, **Kafka
Connect** — состояние коннектора и его задачи, там же его можно
перезапустить.

## 7. Своя таблица и её контракт

В [contracts/](contracts/) лежат Avro-схемы событий, по паре файлов на таблицу:
`<схема>.<таблица>.Key.avsc` и `<схема>.<таблица>.Value.avsc`. Руками их никто
не пишет — их отдаёт Schema Registry после того, как коннектор зарегистрировал
схему. В репозитории они нужны для того, чтобы изменение структуры таблицы
было видно в `git diff`, а не выяснялось из упавшего приёмника.

Разберём на таблице `sales.invoices`.

**Создаём таблицу.** В публикацию она попадёт сама: публикация сделана как
`FOR TABLES IN SCHEMA ref, catalog, sales`. Коннектор перезапускать не нужно.

```bash
docker compose exec pg-master psql -U postgres -d cdc -c "CREATE TABLE sales.invoices (id bigserial PRIMARY KEY, order_id bigint NOT NULL REFERENCES sales.orders(id), amount numeric(18,4) NOT NULL);"
```

Первичный ключ обязателен. При `REPLICA IDENTITY DEFAULT` таблица без PK не
даёт ключа сообщения, а UPDATE и DELETE по ней вообще не попадут в поток.

**Делаем одно изменение.** До первого изменения схемы этой таблицы не
существует, регистрировать нечего:

```bash
docker compose exec pg-master psql -U postgres -d cdc -c "INSERT INTO sales.invoices (order_id, amount) VALUES (1, 42.00);"
```

Событие сразу появится в консьюмере и в UI.

**Проверяем, что схемы зарегистрированы:**

```bash
curl -sS http://localhost:8081/subjects
```

В списке должны быть `cdc.events-dbz.sales.invoices.Key` и
`cdc.events-dbz.sales.invoices.Value`.

**Выгружаем контракт.** В команде меняется только `T` — это `схема.таблица`:

```bash
T=sales.invoices; for k in Key Value; do curl -sS "http://localhost:8081/subjects/cdc.events-dbz.$T.$k/versions/latest" | python3 -c "import json,sys;print(json.dumps(json.loads(json.load(sys.stdin)['schema']),indent=2,sort_keys=True,ensure_ascii=False))" > "contracts/$T.$k.avsc"; done
```

Получатся `contracts/sales.invoices.Key.avsc` и `.Value.avsc` в том же формате,
что остальные файлы. Остаётся посмотреть `git status --short contracts/` и
закоммитить.

Откуда что берётся в этих именах:

```
subject:  cdc.events-dbz.sales.invoices.Value
          └────┬────┘ └┬┘ └──────┬─────┘ └─┬─┘
             топик  префикс  схема.таблица  Key или Value
                       │
                       └── topic.prefix из connector/pg-cdc.json

файл:     contracts/sales.invoices.Value.avsc
                    └──────┬─────┘ └─┬─┘
                     схема.таблица   то же Key/Value, префиксы отброшены
```

Реестр отдаёт схему строкой внутри JSON-ответа, поэтому в команде есть
`json.loads(...['schema'])`. `indent=2, sort_keys=True` — формат остальных
файлов; без него каждая выгрузка давала бы бессмысленный diff.

**Когда контракт надо обновить.** DDL-событий Debezium для PostgreSQL не
отдаёт, поэтому `ALTER TABLE ... ADD COLUMN` проявится как новая версия схемы
в реестре — на первом изменении данных в этой таблице. Выгрузите контракт
заново, и `git diff` покажет, что поменялось. Пройтись сразу по всем таблицам
(пустой `git status contracts/` значит, что контракты совпадают с реестром):

```bash
for f in contracts/*.avsc; do n=$(basename "$f" .avsc); curl -sS "http://localhost:8081/subjects/cdc.events-dbz.$n/versions/latest" | python3 -c "import json,sys;print(json.dumps(json.loads(json.load(sys.stdin)['schema']),indent=2,sort_keys=True,ensure_ascii=False))" > "$f"; done
```

Чего делать не стоит: удалять subject'ы из Schema Registry, пока в топике есть
сообщения с их schema id. Повторная регистрация даёт новый id, а в старых
сообщениях записан прежний — топик станет нечитаемым (`Schema N not found`), и
поможет только пересоздание топика.

### Новая схема

Здесь правок больше — публикация ведётся по схемам, и коннектор фильтрует тоже
по схемам:

```sql
CREATE SCHEMA billing;
GRANT USAGE ON SCHEMA billing TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA billing GRANT SELECT ON TABLES TO debezium;
ALTER PUBLICATION dbz_cdc_pub ADD TABLES IN SCHEMA billing;
```

`ALTER DEFAULT PRIVILEGES` здесь полезнее обычного `GRANT SELECT ON ALL
TABLES`: в только что созданной схеме таблиц ещё нет, так что грант «на всё»
ничего не даёт, а на будущие таблицы не распространяется.

Плюс дописать `billing` в `schema.include.list` в `connector/pg-cdc.json`.
Чтобы коннектор подхватил правку, проще всего снять его и поставить заново.
Позиция не потеряется — она хранится в слоте на стороне PostgreSQL, и события
за время переустановки приедут после старта:

```bash
curl -sS -X DELETE http://localhost:8083/connectors/pg-cdc
```

и снова POST из шага 3.

Если схема есть в публикации, но её нет в `schema.include.list`, получится
самый неприятный расклад: WAL по таблице идёт, а событий в топике нет.

## 8. Голый стенд с нуля

Всё состояние живёт в именованных томах Docker, поэтому «с нуля» — это снести
тома:

```bash
docker compose down -v
```

```bash
docker compose up -d
```

Дальше заново, по порядку: проверить кластер (шаг 2), поставить коннектор
(шаг 3), открыть консьюмер (шаг 4), вставить данные (шаг 5). Коннектор после
`down -v` обязательно ставить заново — его конфиг Connect держал в топике
`connect-configs`, а он был в томе Kafka.

Если тома не трогать, состояние переживает перезапуск:

| команда | что происходит |
|---|---|
| `docker compose stop` | контейнеры остановлены, всё на месте |
| `docker compose down` | контейнеры снесены, тома целы, коннектор после `up` продолжит с прежнего LSN |
| `docker compose down -v` | сносятся и тома: `init/01-init.sql` выполнится заново, коннектора нет |

Сбрасывать PostgreSQL и Kafka по отдельности нельзя. Подтверждённый LSN в
слоте и offset'ы коннектора в `connect-offsets` описывают одну и ту же точку:
снесёте Kafka — коннектор поедет от позиции слота без своих offset'ов, снесёте
PostgreSQL — offset'ы будут ссылаться на LSN, которого уже нет. Поэтому сброс
только целиком.

`init/01-init.sql` работает только на пустом томе. Поправили его — значит
`docker compose down -v && docker compose up -d`.

## 9. Что здесь легко сломать

* **Партиции у `cdc.events`.** Порядок Kafka гарантирует только внутри
  партиции. Добавили партиций — сломали порядок, и обратно уже не уменьшить.
* **Compaction.** У `cdc.events` обязателен `cleanup.policy=delete` — это
  дефолт брокера, но его надо проверить (шаг 4). `compact` оставит по ключу
  только последнюю версию строки, а приёмнику нужна история изменений; уже
  уплотнённое обратно не собрать.
* **`tasks.max`.** Держим 1. Не путать с `max.in.flight` ниже — это разные
  настройки, у которых просто рядом стоят похожие числа. `tasks.max` — это
  параллелизм Kafka Connect, и у PostgreSQL-коннектора Debezium он ни на что
  не влияет: логический слот читается одним подписчиком, поэтому задача всегда
  одна. Проверяется в одну команду — поставьте `tasks.max: 5` и посмотрите
  `curl /connectors/pg-cdc/tasks`: в ответе будет один `task: 0`. Смысл
  единицы в том, чтобы никто не решил, что параллелизм здесь есть.
* **`decimal.handling.mode`.** Только `precise`. `double` уменьшит сообщения и
  испортит денежные значения без единой ошибки в логах.
* **`producer.override.*`.** Работают, только когда у воркера Connect стоит
  `connector.client.config.override.policy=All` (задано в
  `docker-compose.yml`). Без этой политики Connect молча берёт настройки
  воркера: конфиг коннектора выглядит правильным, а гарантий нет.
* **`enable.idempotence`, `acks` и `max.in.flight`.** Работают только втроём.
  Это уже про producer: `max.in.flight.requests.per.connection=5` разрешает
  пять неподтверждённых батчей одновременно. С `enable.idempotence=true`
  producer нумерует батчи по партиции, а брокер отвергает пришедший не по
  порядку, так что пятёрка безопасна и на порядок не влияет — только на
  пропускную способность. Само число не случайное: буфер брокера рассчитан
  ровно на пять батчей на партицию, и при значении больше 5 вместе с
  идемпотентностью producer просто не стартует. А вот если убрать
  идемпотентность и оставить пятёрку, ретрай упавшего батча ляжет после
  успевшего — порядок сломается тихо, до первого упёршегося в foreign key
  приёмника.
* **`snapshot.mode=no_data`.** Коннектор читает только структуру таблиц.
  Данные, которые были до создания слота, в топик не поедут: считается, что
  базы синхронизированы на момент T, а слот создан в этот момент.
* **Регексп роутера.** `^dbz\.[^.]+\.[^.]+$` — ровно три сегмента, иначе в
  `cdc.events` уедет топик heartbeat'а.
* **`TopicRecordNameStrategy`.** Без неё схемы всех таблиц попадут в один
  subject `cdc.events-value`, и Schema Registry зарубит их по совместимости на
  второй же таблице.

## 10. Если что-то не работает

```bash
docker compose logs --tail=100 connect
```

| симптом | что смотреть |
|---|---|
| коннектор `FAILED`, `replication slot "dbz_cdc_slot" does not exist` | том мастера пересоздан без `init/01-init.sql`: `down -v` и заново |
| коннектор `FAILED`, `publication "dbz_cdc_pub" does not exist` | то же самое; `publication.autocreate.mode=disabled`, сам он публикацию не создаст |
| после `down -v` события не идут | коннектора больше нет, поставьте заново (шаг 3) |
| `RUNNING`, а событий нет | пишете не в те схемы (`schema.include.list`) или не в мастер, а в реплику |
| топик есть, но пустой | нормально: топик создаёт первый подключившийся клиент, а не первое событие |
| в Kafka UI вместо полей мусор | Key/Value Serde переключить на `SchemaRegistry` (шаг 6) |
| Kafka UI отдаёт 404 на все страницы | порт 8088 занят чужим процессом, и отвечает он. Проверить: `lsof -nP -iTCP:8088 -sTCP:LISTEN` |
| `no pg_hba.conf entry for replication connection` | правили `pg/*/pg_hba.conf`: строка `host all all` репликацию не покрывает, нужна отдельная |
| реплика не поднялась | `docker compose logs pg-replica-1`; обычно `REPL_USER`/`REPL_PASSWORD` в `.env` разошлись с ролью из `init/01-init.sql` |
| в событиях нет нужной колонки | таблица изменилась: обновите контракт (шаг 7) и посмотрите новую версию схемы |

Адреса: Kafka UI — http://localhost:8088, Connect REST —
http://localhost:8083/connectors, Schema Registry —
http://localhost:8081/subjects.

## 11. Версии

| компонент | стоит | комментарий |
|---|---|---|
| PostgreSQL | 17.9 | Debezium 2.6 официально тестировался до PG 16, через `pgoutput` работает. Понижать нельзя: `failover`-слоты появились в PG 17 |
| Debezium PostgreSQL Connector | 2.6.1.Final | согласованная версия. В 2.6.2 починен race condition при флаше offset (подтверждённый LSN расходился с состоянием топиков, при рестарте терялись события) — апгрейд стоит согласовать отдельно |
| Kafka / Connect / Schema Registry | Confluent Platform 7.7.1 (Kafka 3.7) | Debezium 2.6 собран против Kafka Connect 3.7.0 |
| `kafka-connect-avro-converter` | 7.7.1, из образа Connect | 6.x — линейка Kafka 2.6/2.7, с Kafka 3.7 несовместима |
| Kafka UI | `kafbat/kafka-ui:v1.5.0` | provectus-версия заархивирована и из Docker Hub удалена |

## 12. Микросервис "CDC Target Sync Service (Java & MS SQL Server)"

Микросервис читает `cdc.events` и применяет изменения в MS SQL. Расположен в папке service с подробным описанием.
