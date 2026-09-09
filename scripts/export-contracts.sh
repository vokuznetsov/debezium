#!/usr/bin/env bash
# =============================================================================
#  export-contracts.sh — выгрузка Avro-контрактов из Schema Registry.
#
#  При 500 таблицах это единственный работающий способ вести контракты:
#  руками столько .avsc не поддерживать. Результат КОММИТИТСЯ и ДИФАЕТСЯ —
#  тогда любое изменение структуры таблицы в (А) видно в git diff, а не
#  обнаруживается по падению consumer'а части 2.
#
#  Раскладка:
#    contracts/generated/<subject>.avsc   — последняя версия схемы, jq-формат
#                                           (МАШИННЫЙ ИСТОЧНИК ИСТИНЫ)
#    contracts/generated/INDEX.md         — таблица: subject, версия, id,
#                                           полей, размер схемы
#    contracts/generated/FIELDS.md        — ЧИТАЕМАЯ проекция: таблицы полей
#                                           с типами и nullability
#    contracts/generated/avdl/*.avdl      — Avro IDL для тех схем, которые он
#                                           способен выразить (см. ниже)
#
#  Subject'ы имеют вид  cdc.events-<полное имя Avro-рекорда>  из-за
#  TopicRecordNameStrategy: у каждой таблицы свой subject, и это же имя
#  рекорда однозначно определяет исходную таблицу для consumer'а части 2.
# =============================================================================
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

OUT="contracts/generated"
mkdir -p "$OUT"

step "Читаю список subject'ов из Schema Registry"
SUBJECTS=$(sapi "/subjects" | JQ -r '.[]' | sort)
[ -n "$SUBJECTS" ] || fail "Schema Registry не отдал ни одного subject. Коннектор поднимался? События шли?"
N=$(printf '%s\n' "$SUBJECTS" | wc -l | tr -d ' ')
ok "subject'ов: ${N}"

step "Выгружаю схемы"
# Старые файлы удаляем: subject мог исчезнуть (таблицу вывели из CDC), и
# устаревший .avsc в репозитории хуже отсутствующего.
rm -f "$OUT"/*.avsc
INDEX="${OUT}/INDEX.md"
FIELDS="${OUT}/FIELDS.md"
mkdir -p "${OUT}/avdl"
rm -f "${OUT}/avdl"/*.avdl
{
  printf '# Поля контрактов — читаемая проекция\n\n'
  printf 'Сгенерировано `scripts/export-contracts.sh` из Schema Registry.\n'
  printf 'Машинный источник истины — файлы `.avsc` рядом. Здесь то же самое,\n'
  printf 'но глазами: типы, nullability, точность денежных полей.\n\n'
} > "$FIELDS"
AVDL_OK=0; AVDL_SKIP=0
{
  printf '# Выгруженные Avro-контракты\n\n'
  printf 'Сгенерировано `scripts/export-contracts.sh`. Не редактировать руками:\n'
  printf 'источник истины — Schema Registry, а он наполняется коннектором.\n\n'
  printf 'Subject считается по `TopicRecordNameStrategy`: `<топик>-<полное имя Avro-рекорда>`.\n\n'
  printf '| subject | версия | id схемы | полей | размер схемы, Б |\n'
  printf '|---|---:|---:|---:|---:|\n'
} > "$INDEX"

TOTAL_FIELDS=0
for s in $SUBJECTS; do
    VER=$(sapi "/subjects/${s}/versions" | JQ -r 'max')
    META=$(sapi "/subjects/${s}/versions/${VER}")
    ID=$(printf '%s' "$META" | JQ -r '.id')
    # .schema — это СТРОКА с JSON внутри, поэтому fromjson
    printf '%s' "$META" | JQ -r '.schema' | JQ -S . > "${OUT}/${s}.avsc"
    NFIELDS=$(JQ -r '[.. | objects | select(has("name") and has("type")) | .name] | length' "${OUT}/${s}.avsc")
    SIZE=$(wc -c < "${OUT}/${s}.avsc" | tr -d ' ')
    printf '| `%s` | %s | %s | %s | %s |\n' "$s" "$VER" "$ID" "$NFIELDS" "$SIZE" >> "$INDEX"
    printf '    %-58s v%-3s id=%-4s полей=%-4s %sБ\n' "$s" "$VER" "$ID" "$NFIELDS" "$SIZE"
    TOTAL_FIELDS=$((TOTAL_FIELDS+NFIELDS))

    # --- читаемая проекция: таблица полей ---
    JQ -r --arg subject "$s" --arg id "$ID" \
        -f scripts/avsc-to-fields.jq "${OUT}/${s}.avsc" >> "$FIELDS"

    # --- AVDL: только там, где он корректен ---
    # ОГРАНИЧЕНИЕ AVDL, проверенное avro-tools 1.12.0: лексер Avro IDL не
    # принимает имена, начинающиеся с подчёркивания, и МОЛЧА их срезает
    # (__op становится op), причём обратные кавычки не помогают. В профиле
    # slim все служебные поля именно такие, поэтому для него AVDL не
    # генерируется вообще: неверная документация хуже отсутствующей.
    if JQ -e '[.. | objects | select(.fields?) | .fields[].name]
              | all(test("^[A-Za-z][A-Za-z0-9_]*$"))' "${OUT}/${s}.avsc" >/dev/null; then
        MAIN=$(JQ -r '.name' "${OUT}/${s}.avsc")
        # Имя протокола AVDL собираем из namespace и имени рекорда в
        # CamelCase, отбросив первый сегмент (это topic.prefix):
        # dbz.sales.order_items.Key -> SalesOrderItemsKey
        PROTO=$(JQ -r '"\(.namespace // "x").\(.name)"' "${OUT}/${s}.avsc" \
                | awk -F. '{o=""; for(i=2;i<=NF;i++){t=$i; gsub(/_/," ",t);
                            n=split(t,a," "); w="";
                            for(j=1;j<=n;j++) w = w toupper(substr(a[j],1,1)) substr(a[j],2);
                            o=o w} print o}')
        [ -n "$PROTO" ] || PROTO="Contract"
        JQ -r --arg protocol "$PROTO" --arg protocol_main "$MAIN" \
            -f scripts/avsc-to-avdl.jq "${OUT}/${s}.avsc" > "${OUT}/avdl/${s}.avdl"
        AVDL_OK=$((AVDL_OK+1))
    else
        AVDL_SKIP=$((AVDL_SKIP+1))
    fi
done

step "Сравнение профилей по фактически выгруженным схемам"
# Сравниваем схемы ОДНОЙ И ТОЙ ЖЕ таблицы в двух профилях: это и есть
# «разница в объёме видна прямо в схемах».
# Берём таблицу, для которой есть схема в ОБОИХ профилях, иначе сравнение
# считалось бы по разным таблицам и ничего не значило.
FULL=$(ls "$OUT" | grep -E "^${CDC_TOPIC_FULL}-.*\.Envelope\.avsc$" | head -1 || true)
SLIM=""
TABLE=""
if [ -n "$FULL" ]; then
    # cdc.events.full-dbzf.sales.order_items.Envelope.avsc -> sales.order_items
    TABLE=$(printf '%s' "$FULL" | sed -E "s/^${CDC_TOPIC_FULL}-${TOPIC_PREFIX_FULL}\.//; s/\.Envelope\.avsc$//")
    SLIM=$(ls "$OUT" | grep -E "^${CDC_TOPIC}-${TOPIC_PREFIX}\.${TABLE}\.Value\.avsc$" | head -1 || true)
fi
{
  printf '\n## Размер схем по профилям\n\n'
  if [ -n "$TABLE" ]; then printf 'Одна и та же таблица `%s` в двух профилях:\n\n' "$TABLE"; fi
  printf '| профиль | subject | полей в схеме | размер схемы, Б |\n'
  printf '|---|---|---:|---:|\n'
} >> "$INDEX"
for pair in "slim:$SLIM" "full:$FULL"; do
    prof="${pair%%:*}"; f="${pair#*:}"
    if [ -n "$f" ] && [ -f "${OUT}/${f}" ]; then
        fl=$(JQ -r '[.. | objects | select(has("name") and has("type")) | .name] | length' "${OUT}/${f}")
        sz=$(wc -c < "${OUT}/${f}" | tr -d ' ')
        printf '| %s | `%s` | %s | %s |\n' "$prof" "${f%.avsc}" "$fl" "$sz" >> "$INDEX"
        info "${prof}: ${f%.avsc} — полей ${fl}, схема ${sz} Б"
    else
        if [ "$prof" = "full" ]; then
            info "full: в этом прогоне профиль не поднимался (он нужен только для замера)."
            info "      Эталонные схемы профиля full лежат в contracts/examples/full/."
        else
            info "slim: схемы для таблицы ${TABLE:-?} нет — коннектор её не видел"
        fi
    fi
done

printf '\nВсего subject'"'"'ов: %s, суммарно именованных полей: %s\n' "$N" "$TOTAL_FIELDS" >> "$INDEX"
head2 "AVDL"
ok "сгенерировано .avdl: ${AVDL_OK}"
if [ "$AVDL_SKIP" -gt 0 ]; then
    warn "пропущено схем: ${AVDL_SKIP} — в них есть поля на подчёркивание (__op, __lsn ...)"
    info "Avro IDL такие имена не выражает: лексер срезает подчёркивания молча,"
    info "и __op в схеме превратился бы в op. Для этих схем читайте FIELDS.md"
    info "и .avsc. Подробно — contracts/README.md."
fi

ok "готово: ${OUT}/ ($(ls "$OUT"/*.avsc 2>/dev/null | wc -l | tr -d ' ') схем), ${INDEX}, ${FIELDS}"
info "результат нужно закоммитить: изменение структуры таблицы в (А) обязано быть видно в git diff"
