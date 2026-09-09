#!/usr/bin/env bash
# =============================================================================
#  verify-order.sh — проверка ИНВАРИАНТА №1 по фактическому содержимому топика.
#
#  Читает весь cdc.events с Avro-десериализацией из Schema Registry, печатает
#  поток в виде  offset | lsn | txId | schema.table | op | pk  и:
#    1) проверяет МОНОТОННОСТЬ LSN — падает, показав окружающие записи;
#    2) проверяет отсутствие ДУБЛИКАТОВ по LSN;
#    3) проверяет, что все записи из ПАРТИЦИИ 0;
#    4) считает статистику по таблицам, операциям и транзакциям;
#    5) проверяет, что исключённой таблицы в потоке нет.
#
#  Работает с обоими профилями: в slim служебные поля — __lsn/__txId/__table,
#  в full — source.lsn/source.txId/source.table.
#
#  ------------------------------------------------------------------------
#  ПОЧЕМУ ЧИТАЕТ НЕ kcat, ХОТЯ kcat ЕСТЬ В ПРОФИЛЕ tools
#
#  Штатная команда
#     kcat -b kafka:9092 -t cdc.events -C -e -o beginning \
#          -s value=avro -r http://schema-registry:8081 -J
#  на этом потоке РАБОТАЕТ НЕ ДО КОНЦА. Она обрывается на первом же сообщении,
#  где есть Avro-поле bytes с logicalType=decimal:
#
#     % ERROR: Failed to deserialize value in message in cdc.events [0] at
#       offset 18: Error parsing JSON: \u0000 is not allowed without
#       JSON_ALLOW_NUL
#
#  Причина: decimal.handling.mode=precise (а его менять НЕЛЬЗЯ — банковские
#  данные) кодирует numeric как Avro bytes, а конвертер Avro->JSON внутри
#  libserdes не умеет представлять произвольные байты, включая NUL. Обрыв
#  ТИХИЙ в том смысле, что kcat выходит с кодом 1 и уже прочитанные сообщения
#  выглядят полным потоком — на нашем стенде это давало 18 записей из 53.
#
#  Поэтому читаем kafka-avro-console-consumer из образа Schema Registry: он
#  использует Avro JsonEncoder и кодирует bytes как строку с \uXXXX, то есть
#  выдаёт валидный JSON. kcat остаётся для того, где он надёжен: сырые
#  размеры сообщений (-f '%S') в benchmark.sh и разовый просмотр.
#  Подробнее — README, раздел 8 (Troubleshooting).
#  ------------------------------------------------------------------------
#
#  Использование:
#    ./scripts/verify-order.sh                 # топик cdc.events
#    ./scripts/verify-order.sh cdc.events.full # профиль full
#    ./scripts/verify-order.sh cdc.events 200  # печатать 200 строк потока
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TOPIC="${1:-$CDC_TOPIC}"
PRINT_LIMIT="${2:-40}"
# Сколько последних сообщений проверять. На стенде топик легко вырастает до
# миллионов сообщений (benchmark), а разбор миллиона записей через консольный
# консьюмер и jq занимает минуты и десятки МБ во временных файлах. Проверка
# монотонности LSN имеет смысл на любом непрерывном окне, поэтому по умолчанию
# берём последние 200 тысяч. VERIFY_MAX=0 — проверить топик целиком.
VERIFY_MAX="${VERIFY_MAX:-200000}"

SEP='|@#@|'

head2 "чтение ${TOPIC}"
COUNT=$(topic_count "$TOPIC")
[ "${COUNT:-0}" -gt 0 ] || fail "в топике ${TOPIC} нет сообщений — проверять нечего"
info "high watermark топика: ${COUNT}"

RAW="$(mktemp)"; TSV="$(mktemp)"; trap 'rm -f "$RAW" "$TSV"' EXIT

# Окно проверки: либо весь топик, либо последние VERIFY_MAX сообщений.
# --max-messages задаётся точно, поэтому consumer выходит сам, без таймаута.
FROM_OFFSET=0
READ_N="$COUNT"
if [ "$VERIFY_MAX" != "0" ] && [ "$COUNT" -gt "$VERIFY_MAX" ]; then
    FROM_OFFSET=$((COUNT-VERIFY_MAX))
    READ_N="$VERIFY_MAX"
    warn "в топике ${COUNT} сообщений — проверяю последние ${VERIFY_MAX} (с offset ${FROM_OFFSET})"
    info "VERIFY_MAX=0 проверит топик целиком (на миллионах записей это минуты)"
fi

if [ "$FROM_OFFSET" = "0" ]; then
    avro_consume "$TOPIC" "$READ_N" \
        --property print.partition=true --property print.offset=true \
        --property print.key=true --property "key.separator=${SEP}" \
        | grep '^Partition:' > "$RAW" || true
else
    avro_consume "$TOPIC" "$READ_N" --partition 0 --offset "$FROM_OFFSET" \
        --property print.partition=true --property print.offset=true \
        --property print.key=true --property "key.separator=${SEP}" \
        | grep '^Partition:' > "$RAW" || true
fi

MSGS=$(wc -l < "$RAW" | tr -d ' ')
[ "$MSGS" -gt 0 ] || fail "не прочитано ни одного сообщения из ${TOPIC}"
if [ "$MSGS" != "$READ_N" ]; then
    softfail "прочитано ${MSGS} сообщений из ожидаемых ${READ_N} — часть потока не разобрана"
else
    ok "прочитано сообщений: ${MSGS} (ожидалось ${READ_N})"
fi

# Строка вида:  Partition:0|@#@|Offset:18|@#@|{ключ}|@#@|{значение}
# Разделитель нарочно длинный: одиночный символ мог бы встретиться в данных.
#
# Собираем ОДИН поток JSON-строк и разбираем его ОДНИМ процессом jq: по одному
# jq на сообщение benchmark на 50 000 событий не пережил бы.
awk -F'\\|@#@\\|' '{
    o=$2; sub(/^Offset:/,"",o);
    p=$1; sub(/^Partition:/,"",p);
    printf "{\"off\":%s,\"part\":%s,\"key\":%s,\"val\":%s}\n", o, p, $3, $4
}' "$RAW" > "${RAW}.jsonl"

# Avro-union в JSON выглядит как {"long": 123} / {"string": "x"} —
# u разворачивает такую обёртку до значения.
JQ -r '
  def u: if . == null then null
         elif type=="object" and (keys|length)==1
              and (keys[0]|test("^(long|int|string|boolean|double|float|bytes|null)$"))
         then (to_entries[0].value) else . end;
  def pick(a; b): (a|u) as $x | if $x == null then (b|u) else $x end;
  . as $m
  | ($m.val // {}) as $p
  | (($p.source // {}) | (if type=="object" then . else {} end)) as $s
  | [ $m.off,
      $m.part,
      (pick($p.__lsn;   $s.lsn)   // "-"),
      (pick($p.__txId;  $s.txId)  // "-"),
      ((pick($p.__schema; $s.schema) // "?") + "." + (pick($p.__table; $s.table) // "?")),
      (pick($p.__op;    $p.op)    // "?"),
      ( $m.key
        | if type=="object" then [to_entries[] | "\(.key)=\((.value|u)|tostring)"] | join(",")
          elif . == null then "-" else tostring end ),
      ( ($p.__deleted | u) // "-" )
    ] | @tsv
' "${RAW}.jsonl" > "$TSV" || fail "не удалось разобрать сообщения (формат payload изменился?)"
rm -f "${RAW}.jsonl"

grep -q '[^[:space:]]' "$TSV" || fail "разбор дал пустой результат"

head2 "поток (первые ${PRINT_LIMIT} записей из ${MSGS})"
printf '  %-7s %-4s %-12s %-8s %-26s %-3s %-22s %s\n' \
       offset part lsn txId schema.table op pk deleted
awk -v lim="$PRINT_LIMIT" -F'\t' '
  NR<=lim { printf "  %-7s %-4s %-12s %-8s %-26s %-3s %-22s %s\n", $1,$2,$3,$4,$5,$6,$7,$8 }
  END { if (NR>lim) printf "  ... ещё %d записей\n", NR-lim }
' "$TSV"

# --- 1. монотонность LSN ----------------------------------------------------
head2 "проверка 1: монотонность LSN"
if ! awk -F'\t' '
  { lsn[NR]=$3; line[NR]=$0 }
  NR>1 && $3+0 < prev+0 {
      printf "  LSN ПОШЁЛ НАЗАД на записи %d: %s < %s\n", NR, $3, prev > "/dev/stderr"
      printf "  окружение:\n" > "/dev/stderr"
      for (i = (NR-3<1 ? 1 : NR-3); i <= NR+3; i++)
          if (i in line) printf "    %s%s\n", (i==NR ? ">>> " : "    "), line[i] > "/dev/stderr"
      bad=1
  }
  { prev=$3 }
  END { exit bad }
' "$TSV"; then
    softfail "монотонность LSN нарушена (см. окружение выше) — порядок применения в (Б) сломан"
else
    FIRST=$(head -1 "$TSV" | cut -f3); LAST=$(tail -1 "$TSV" | cut -f3)
    ok "LSN монотонен на всех ${MSGS} записях (${FIRST} -> ${LAST})"
fi

# --- 2. дубликаты LSN -------------------------------------------------------
head2 "проверка 2: дубликаты по LSN"
DUPS=$(cut -f3 "$TSV" | sort | uniq -d | head -5)
if [ -n "$DUPS" ]; then
    DUPN=$(cut -f3 "$TSV" | sort | uniq -d | wc -l | tr -d ' ')
    printf '  повторяющиеся LSN (первые 5 из %s):\n' "$DUPN" >&2
    printf '    %s\n' $DUPS >&2
    printf '  записи с этими LSN:\n' >&2
    for d in $DUPS; do awk -F'\t' -v d="$d" '$3==d {printf "    %s\n", $0}' "$TSV" >&2; done
    softfail "есть дубликаты по LSN — consumer части 2 обязан дедуплицировать по __lsn"
else
    ok "дубликатов по LSN нет"
fi

# --- 3. одна партиция -------------------------------------------------------
head2 "проверка 3: все записи из партиции 0"
PARTS=$(cut -f2 "$TSV" | sort -u | tr '\n' ' ')
if [ "$(printf '%s' "$PARTS" | tr -d ' ')" = "0" ]; then
    ok "все ${MSGS} записей из партиции 0 (инвариант №1 соблюдён)"
else
    softfail "записи найдены в партициях: ${PARTS} — порядок между партициями Kafka НЕ гарантирует"
fi
PCOUNT=$(ktopics --describe --topic "$TOPIC" | awk -F'PartitionCount: ' 'NR==1{split($2,a," ");print a[1]}')
[ "$PCOUNT" = "1" ] && ok "у топика ${TOPIC} ровно 1 партиция" \
                    || softfail "у топика ${TOPIC} партиций: ${PCOUNT}, ожидалась 1"

# --- 4. статистика ----------------------------------------------------------
head2 "проверка 4: статистика потока"
info "по таблицам:"
awk -F'\t' '{c[$5]++} END {for (t in c) printf "    %-30s %6d\n", t, c[t]}' "$TSV" | sort
info "по операциям (c=insert, u=update, d=delete, r=snapshot):"
awk -F'\t' '{c[$6]++} END {for (o in c) printf "    %-4s %6d\n", o, c[o]}' "$TSV" | sort
TXN=$(cut -f4 "$TSV" | sort -u | wc -l | tr -d ' ')
info "различных txId: ${TXN}"
info "событий на транзакцию (среднее): $(awk -v m="$MSGS" -v t="$TXN" 'BEGIN{printf "%.1f", m/t}')"

if awk -F'\t' '$5 ~ /order_items_archive/ {found=1} END {exit !found}' "$TSV"; then
    softfail "в потоке есть sales.order_items_archive — table.exclude.list не сработал"
else
    ok "исключённой таблицы sales.order_items_archive в потоке нет (table.exclude.list работает)"
fi

finish
