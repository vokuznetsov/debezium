# Avro-контракты потока `cdc.events`

Файлы здесь описывают, что consumer части 2 получает из Kafka.

```
contracts/
  examples/     эталонные схемы, выверенные руками: 3 таблицы x 2 профиля.
                Читать начинать отсюда.
  generated/    полная выгрузка из Schema Registry (scripts/export-contracts.sh).
                Генерируется, коммитится, дифается. Руками не править.
  README.md     этот файл: значимые поля, размеры, что убрано в slim.
```

Выгрузка: `make contracts`. Результат нужно **коммитить**: тогда изменение
структуры таблицы в (А) видно в `git diff`, а не обнаруживается по падению
consumer'а.

Две оговорки к выгрузке:

- **`id` схемы локален для конкретного Schema Registry.** В `INDEX.md` он
  приведён как факт этого прогона; после `make clean` или на другом контуре
  идентификаторы будут другими. Сравнивать между контурами надо содержимое
  схем, а не id.
- В выгрузку попадают и служебные схемы (`__debezium-heartbeat.*`,
  `*.transaction`) — они не относятся к таблицам, но полезны как контракт
  топиков метаданных.

---

## Топик ОДИН, а схем МНОГО. Это не противоречие

Самая частая путаница, поэтому подробно.

Топик действительно один: **`cdc.events`** (имя задано в `.env`, переменная
`CDC_TOPIC`). Одна партиция. Туда едут изменения всех таблиц всех схем.

Но **«схема топика» в Kafka не существует**. Схема — свойство *сообщения*, а
не топика. В Avro-сообщении первый байт — маркер `0`, следующие четыре —
**идентификатор схемы** в Schema Registry, и только потом данные:

```
[0x00][4 байта: id схемы][бинарные данные полей]
```

Поэтому в одном топике совершенно легально едут сообщения разных структур:
каждое несёт свой id, и десериализатор по этому id забирает нужную схему.

Вот как это выглядит на живом стенде — подряд идущие сообщения одного топика,
у каждого свой id схемы:

| offset | id схемы | какой subject соответствует id |
|---:|---:|---|
| 34 | 8 | `cdc.events-dbz.catalog.suppliers.Value` |
| 35 | 10 | `cdc.events-dbz.catalog.products.Value` |
| 36 | 12 | `cdc.events-dbz.sales.customers.Value` |
| 37 | 14 | `cdc.events-dbz.sales.orders.Value` |
| 38 | 16 | `cdc.events-dbz.sales.order_items.Value` |

Проверить самому:

```bash
# id схемы последнего сообщения = байты 2..5 значения
docker compose --profile tools run --rm -T kcat -b kafka:9092 -t cdc.events   -p 0 -o -1 -c 1 -C -e -f '%s' 2>/dev/null | od -An -tu1 -N5

# какому subject он принадлежит
curl -s http://localhost:8081/schemas/ids/16/subjects | jq
```

### Сколько схем у одного топика

По две на каждую таблицу — на ключ и на значение. Восемь таблиц дают
шестнадцать subject'ов:

```
cdc.events-dbz.ref.currencies.Key        cdc.events-dbz.ref.currencies.Value
cdc.events-dbz.ref.regions.Key           cdc.events-dbz.ref.regions.Value
cdc.events-dbz.catalog.suppliers.Key     cdc.events-dbz.catalog.suppliers.Value
cdc.events-dbz.catalog.products.Key      cdc.events-dbz.catalog.products.Value
cdc.events-dbz.sales.customers.Key       cdc.events-dbz.sales.customers.Value
cdc.events-dbz.sales.orders.Key          cdc.events-dbz.sales.orders.Value
cdc.events-dbz.sales.order_items.Key     cdc.events-dbz.sales.order_items.Value
cdc.events-dbz.sales.order_items_archive... (исключена, схемы нет)
```

На 500 таблицах их будет около тысячи — отсюда и поднятый лимит кэша схем
(`max.schemas.per.subject = 5000`), и требование `TopicRecordNameStrategy`.

**Именно поэтому в `contracts/` по файлу на таблицу, а не один файл на топик.**
Файл на топик был бы физически невозможен: у топика нет одной схемы.

### Что тогда общего у всех сообщений топика

Вот это и есть «контракт топика» — та часть, которая одинакова всегда,
независимо от таблицы:

| поле | тип | в каждом сообщении |
|---|---|---|
| `__op` | string | да |
| `__table` | string | да |
| `__schema` | string | да |
| `__lsn` | long | да |
| `__txId` | long | да |
| `__ts_ms` | long | да |
| `__deleted` | string | да (`"true"` только у удалений) |
| колонки таблицы | зависят от таблицы | **меняются от сообщения к сообщению** |

Consumer части 2 пишется именно против этого: служебные поля он знает заранее,
а набор колонок узнаёт из схемы, полученной по id. Никаких сгенерированных
классов и никакого предварительного знания о 500 таблицах.

---

## Почему контракты не переписаны на AVDL

Вопрос законный: AVDL (Avro IDL) читается несравнимо лучше, чем JSON-схема.
Сравните — одно и то же:

```avdl
record Value {
  long id;
  long order_id;
  long product_id;
  int qty;
  decimal(18, 4) price;
}
```

против двух десятков строк JSON с `connect.parameters` и `logicalType`.

**И всё же на AVDL перейти нельзя. Две причины, обе проверены.**

### Причина 1: AVDL не умеет имена полей на подчёркивание

Все служебные поля профиля `slim` начинаются с `__` (это префикс Debezium по
умолчанию для `add.fields`). Лексер Avro IDL такие имена **не принимает и
молча срезает подчёркивания**. Проверено компиляцией `avro-tools 1.12.0`:

```avdl
record R {
  union { null, string } _one   = null;
  union { null, string } __two  = null;
  union { null, string } `__three` = null;   // обратные кавычки не помогают
}
```

на выходе:

```json
{"fields":[{"name":"one",...},{"name":"two",...},{"name":"three",...}]}
```

`__op` превратился бы в `op`, `__lsn` в `lsn`. Документация, которая называет
поля не своими именами, хуже отсутствующей: по ней напишут consumer, который
не найдёт ни одного служебного поля.

### Причина 2: направление истины

Схемы здесь **не пишутся руками** — их создаёт Debezium из структуры таблиц и
регистрирует в Schema Registry автоматически. `contracts/generated/` — это
*выгрузка* оттуда, нужная для того, чтобы изменение структуры таблицы было
видно в `git diff`.

Если писать AVDL руками и компилировать в `.avsc`, появляется второй источник
истины, который разъедется с реальностью. Причём его компиляция **не даст
побайтово тот же `.avsc`**: Debezium кладёт в схему `connect.name`,
`connect.version`, `connect.parameters` и значения `default`, которые AVDL
либо не выражает, либо выражает иначе. Результат — постоянные ложные диффы, а
в контуре с `auto.register.schemas=false` ещё и риск зарегистрировать схему,
которая отличается от фактической.

### Что сделано вместо этого

1. **`contracts/generated/FIELDS.md`** — читаемая проекция всех схем:
   таблица полей с типами, nullability и точностью денежных полей.
   Генерируется тем же `make contracts`, поэтому разъехаться не может.
   Именно здесь удобно смотреть, что за поля в сообщении.

2. **`contracts/generated/avdl/*.avdl`** — AVDL **там, где он корректен**:
   схемы ключей и весь профиль `full` (в нём нет полей на подчёркивание).
   Генерируется из `.avsc`, а не наоборот. Все выгруженные файлы проверены
   компиляцией `avro-tools 1.12.0`. Схемы профиля `slim` сознательно
   пропускаются, и скрипт печатает, почему.

3. **`.avsc` остаётся машинным источником истины** — именно он совпадает с
   тем, что лежит в Schema Registry.

Вот как профиль `full` выглядит в AVDL — на нём хорошо видно, из чего состоит
конверт и почему `slim` в 2,7 раза компактнее:

```avdl
@namespace("dbzf.sales.order_items")
protocol SalesOrderItemsEnvelope {

  record Value {                      // сама строка таблицы
    long id;
    long order_id;
    long product_id;
    int qty;
    decimal(18, 4) price;
  }

  @namespace("io.debezium.connector.postgresql")
  record Source {                     // 14 полей метаданных в КАЖДОМ сообщении
    string version;                   //  <- константа для всего коннектора
    string connector;                 //  <- константа
    string name;                      //  <- константа
    long ts_ms;
    union { string, null } snapshot;
    string db;                        //  <- константа
    union { null, string } sequence = null;
    long ts_us;
    long ts_ns;
    string schema;
    string table;
    union { null, long } txId = null;
    union { null, long } lsn = null;
    union { null, long } xmin = null;
  }

  record Envelope {
    union { null, dbzf.sales.order_items.Value } before = null;   // не нужен в (Б)
    union { null, dbzf.sales.order_items.Value } after = null;
    io.debezium.connector.postgresql.Source source;
    string op;
    union { null, long } ts_ms = null;
    union { null, long } ts_us = null;
    union { null, long } ts_ns = null;
    union { null, event.block } transaction = null;
  }
}
```

Профиль `slim` оставляет от всего этого только `Value` плюс семь служебных
полей — и как раз их имена AVDL записать не может.

---

## Как устроены subject'ы

Из-за единого топика в коннекторе задана `TopicRecordNameStrategy`:

```
subject = <топик>-<полное имя Avro-рекорда>
```

то есть `cdc.events-dbz.catalog.products.Value`, `cdc.events-dbz.sales.orders.Key`
и так далее — **у каждой таблицы свой subject**.

При дефолтной `TopicNameStrategy` subject считался бы по имени топика, схемы
всех 500 таблиц попали бы в один `cdc.events-value`, и Schema Registry
зарубил бы их по совместимости (`backward`) на второй же таблице.

Побочная польза: полное имя рекорда (`dbz.catalog.products.Value`) однозначно
определяет исходную таблицу — это второй, независимый от `__schema`/`__table`
способ её узнать.

---

## Профиль `slim` — что едет в сообщении

Значение = **сама строка** (все колонки таблицы, кроме исключённых) плюс
служебные поля:

| поле | тип | зачем |
|---|---|---|
| `__op` | string | `c` вставка, `u` обновление, `d` удаление, `r` снапшот |
| `__table` | string | имя таблицы без схемы |
| `__schema` | string | имя схемы |
| `__lsn` | long | **позиция в WAL. На ней держатся порядок и идемпотентность** |
| `__txId` | long | id транзакции PostgreSQL |
| `__ts_ms` | long | время события в источнике |
| `__deleted` | string | `"true"` у события удаления (`delete.handling.mode=rewrite`) |

Ключ = первичный ключ таблицы (`{"id": 123}`, `{"code": "USD"}`).

**Убирать `__lsn` и `__txId` ради экономии нельзя.** По `__lsn` consumer
дедуплицирует при рестарте, по нему же проверяется монотонность потока.

## Профиль `full` — что добавляется

Полный конверт Debezium: `before`, `after`, `source` (12 полей), `op`,
`ts_ms`, `transaction`. Из `source` в каждом сообщении повторяются поля,
константные для всего коннектора: `version`, `connector`, `name`, `db`,
`ts_ms`, `snapshot`, `sequence`.

Профиль нужен для двух вещей: померить разницу в объёме и иметь запасной
вариант, если на согласовании потребуют before-образ.

---

## Что теряется в `slim` и почему это безопасно

| теряется | почему не нужно |
|---|---|
| `before`-образ | В MS SQL применяется `after` по PK: UPDATE по PK, DELETE по PK. Diff по колонкам приёмник не строит. |
| события `truncate` | В публикации `publish = 'insert, update, delete'`. TRUNCATE на приёмнике — отдельная согласованная процедура, а не событие потока. |
| события `message` | Логические сообщения (`pg_logical_emit_message`) в этой схеме не используются. |
| `source.version`, `source.connector`, `source.name`, `source.db` | Константы для всего коннектора. Известны из конфигурации, в потоке бесполезны. |
| `source.sequence`, `source.xmin` | Диагностика. Позиция и так есть в `__lsn`. |
| `transaction` (блок в конверте) | Границы транзакций едут отдельным топиком `dbz.transaction` (`provide.transaction.metadata=true`), а `__txId` есть в каждом сообщении. |

Что **не** убрано сознательно:

- `decimal.handling.mode = precise` — денежные поля остаются `bytes` +
  `logicalType: decimal`. `double` уменьшил бы сообщение и **молча испортил
  значения**. Это банковские данные, размен неприемлем.
- `time.precision.mode` — дефолт.

---

## Размерные характеристики

Замеренные на стенде числа (средний размер сообщения по профилям, эффект
компрессии, пропускная способность) — в корневом `README.md`, раздел 5.
Здесь только структурная разница; фактические байты меряет
`scripts/benchmark.sh`, и мерить их надо на своих данных.
