# =============================================================================
#  avsc-to-fields.jq — .avsc в читаемую таблицу полей (Markdown).
#
#  Зачем: .avsc — машинный JSON, читать его человеку тяжело. AVDL был бы
#  нагляднее, но он НЕ УМЕЕТ имена полей на подчёркивание (__op, __lsn), а в
#  профиле slim все служебные поля именно такие — см. contracts/README.md,
#  раздел «Почему контракты не переписаны на AVDL». Поэтому читаемая проекция
#  делается таблицей, а не AVDL.
#
#  Вызов: jq -r --arg subject <subject> --arg id <id> -f scripts/avsc-to-fields.jq f.avsc
# =============================================================================

def tname:
  if type == "string" then .
  elif type == "array" then
    ([.[] | select(. != "null") | tname] | join(" | "))
  elif type == "object" then
    if .logicalType == "decimal" then "decimal(\(.precision),\(.scale))"
    elif .type == "record" then "запись \(.name)"
    elif .type == "enum"   then "enum \(.name)"
    elif .type == "array"  then "array<\(.items|tname)>"
    elif .type == "map"    then "map<\(.values|tname)>"
    else (.type | tname) end
  else "?" end;

def is_null_ok:
  (type == "array") and (any(.[]; . == "null"));

def note:
  [ (if .logicalType == "decimal"
     then "точное десятичное: unscaled value + scale=\(.scale)" else empty end),
    (if .["connect.name"] then .["connect.name"] else empty end)
  ] | join("; ");

def field_row:
  . as $f
  | ($f.type) as $t
  | ($t | if type == "array" then ([.[] | select(type=="object")] | first) // {} else (if type=="object" then . else {} end) end) as $inner
  | "| `\($f.name)` | \($t|tname) | \(if ($t|is_null_ok) then "да" else "нет" end) | \($inner|note) |";

def records:
  [.. | objects | select(.type? == "record" and (.fields? != null))]
  | reduce .[] as $r ([]; if any(.[]; .name == $r.name) then . else . + [$r] end);

"### `\($subject)`\n"
+ "\nid схемы: \($id) · полное имя рекорда: `\(.namespace // "").\(.name)`\n"
+ ([ records[]
     | "\n**\(.namespace // "").\(.name)** — полей \(.fields|length)\n\n"
       + "| поле | тип | может быть null | примечание |\n|---|---|---|---|\n"
       + ([.fields[] | field_row] | join("\n"))
   ] | join("\n"))
+ "\n"
