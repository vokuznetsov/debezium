# =============================================================================
#  avsc-to-avdl.jq — рендер Avro-схемы (.avsc) в Avro IDL (.avdl).
#
#  ЗАЧЕМ: .avsc — это JSON, он машинный и плохо читается человеком. AVDL —
#  тот же контракт в виде, похожем на объявление структуры, с decimal(p,s)
#  вместо восьми строк аннотаций.
#
#  ВАЖНО ПРО НАПРАВЛЕНИЕ ИСТИНЫ: .avdl здесь ГЕНЕРИРУЕТСЯ из .avsc, а не
#  наоборот. Источник истины — Schema Registry, куда схемы кладёт сам
#  Debezium. Писать .avdl руками и компилировать в .avsc нельзя: получится
#  второй источник, который разъедется с реальностью, а его компиляция не
#  даст побайтово тот же .avsc (connect.name, connect.version,
#  connect.parameters, defaults), то есть даст ложные диффы.
#
#  Вызов: jq -r --arg protocol <Имя> -f scripts/avsc-to-avdl.jq schema.avsc
# =============================================================================

# Идентификаторы AVDL начинаются с буквы: лексер не принимает ведущее
# подчёркивание, а служебные поля профиля slim называются __op, __lsn и т.п.
# Такие имена экранируются обратными кавычками — проверено компиляцией
# avro-tools 1.12.0.
def ident:
  if test("^[A-Za-z][A-Za-z0-9_]*$") then . else "`" + . + "`" end;

def render_type:
  if type == "string" then .
  elif type == "array" then
    (if length == 2 and (.[0] == "null")
     then "union { null, " + (.[1] | render_type) + " }"
     else "union { " + ([.[] | render_type] | join(", ")) + " }" end)
  elif type == "object" then
    if .logicalType == "decimal"          then "decimal(\(.precision), \(.scale))"
    elif .logicalType == "timestamp-millis" then "timestamp_ms"
    elif .logicalType == "timestamp-micros" then "timestamp_ms /* micros */"
    elif .logicalType == "date"             then "date"
    elif .logicalType == "time-millis"      then "time_ms"
    elif .logicalType == "uuid"             then "uuid"
    # Ссылка на запись — ПОЛНЫМ именем: вложенные записи Debezium лежат в
    # своих namespace (io.debezium.connector.postgresql.Source, event.block),
    # и короткое имя ищется в текущем namespace, что даёт
    # "Undefined schema: <ns>.Source" при компиляции.
    elif .type == "record"                  then (if .namespace then "\(.namespace).\(.name)" else .name end)
    elif .type == "enum"                    then (if .namespace then "\(.namespace).\(.name)" else .name end)
    elif .type == "array"                   then "array<" + (.items | render_type) + ">"
    elif .type == "map"                     then "map<" + (.values | render_type) + ">"
    else (.type | render_type) end
  else "/* неизвестный тип */ string" end;

# Комментарий с тем, что AVDL не выражает: connect-тип и дефолт
def annot:
  [ (if (.type|type) == "object" and (.type["connect.name"]) then .type["connect.name"] else empty end),
    (if (.type|type) == "array"
        and ((.type | length) == 2)
        and ((.type[1]|type) == "object")
        and (.type[1]["connect.name"])
     then .type[1]["connect.name"] else empty end),
    (if has("default") and (.default != null) then "default=\(.default|tostring)" else empty end)
  ] | if length > 0 then "   // " + join(", ") else "" end;

def field_line:
  . as $f
  | ($f.type | render_type) as $t
  | (if ($f.type|type) == "array" and ($f.type[0] == "null") then " = null" else "" end) as $d
  | (if $f.doc then "    /** " + ($f.doc | gsub("\\s+"; " ")) + " */\n" else "" end)
    + "    " + $t + " " + ($f.name | ident) + $d + ";" + ($f | annot);

def record_block($ns):
  "  "
  + (if (.namespace // $ns) != $ns then "@namespace(\"\(.namespace)\")\n  " else "" end)
  + (if .doc then "/** " + (.doc | gsub("\\s+"; " ")) + " */\n  " else "" end)
  + "record \(.name | ident) {\n"
  + ([.fields[] | field_line] | join("\n"))
  + "\n  }";

# Все определения записей. Повторные ссылки на ту же запись в .avsc — это
# строки, а не объекты, поэтому здесь их нет.
#
# ПОРЯДОК ОБРАТНЫЙ порядку появления: в AVDL тип обязан быть объявлен ДО
# использования, а в .avsc наоборот — внешняя запись идёт первой и содержит
# вложенные внутри себя. Поэтому главная запись (Envelope или Value)
# оказывается В КОНЦЕ файла. Без реверса avro-tools не разбирает файл вообще.
def records:
  [.. | objects | select(.type? == "record" and (.fields? != null))]
  | reduce .[] as $r ([]; if any(.[]; .name == $r.name and (.namespace // "") == ($r.namespace // "")) then . else . + [$r] end)
  | reverse;

(.namespace // "") as $ns
| "/**\n"
+ " * СГЕНЕРИРОВАНО scripts/export-contracts.sh из Schema Registry. Не править.\n"
+ " * Это ЧИТАЕМАЯ ПРОЕКЦИЯ контракта. Машинный источник истины — .avsc\n"
+ " * рядом и сам Schema Registry. AVDL здесь никуда не регистрируется.\n"
+ " *\n * ГЛАВНАЯ ЗАПИСЬ (то, что лежит в сообщении) — ПОСЛЕДНЯЯ в файле:\n"
+ " * \($protocol_main). В AVDL тип объявляется до использования, поэтому\n"
+ " * вложенные записи идут первыми.\n"
+ " */\n"
+ (if $ns != "" then "@namespace(\"\($ns)\")\n" else "" end)
+ "protocol \($protocol) {\n\n"
+ ([records[] | record_block($ns)] | join("\n\n"))
+ "\n}\n"
