-- =============================================================================
--  04-publication.sql — публикация и REPLICA IDENTITY
--
--  ОГРАНИЧЕНИЕ КОНТУРА: Debezium не создаёт публикаций. У роли debezium нет
--  CREATE на базе, поэтому она физически не может. Публикация ведётся ЭТИМ
--  скриптом, а в коннекторе стоит publication.autocreate.mode=disabled.
--
--  Состав CDC при ~500 таблицах живёт здесь, а не в table.include.list
--  коннектора: список на 500 строк в JSON неуправляем. Коннектор фильтрует
--  грубо (schema.include.list) и точечно исключает (table.exclude.list).
--
--  Идемпотентен: публикация — через DO с проверкой pg_publication, состав
--  досоздаётся через ALTER PUBLICATION ... ADD TABLE только для того, чего в
--  ней ещё нет.
--
--  Переменные psql:
--    :cdc_schemas            — список схем CDC через запятую (из CDC_SCHEMAS в .env)
--    :publication            — имя публикации
--    :replica_identity_full  — список таблиц (через запятую), которым нужен
--                              REPLICA IDENTITY FULL; пустая строка = никому
-- =============================================================================

SELECT set_config('cdcinit.publication',  :'publication', false),
       set_config('cdcinit.ri_full',      :'replica_identity_full', false),
       -- Список схем CDC — из CDC_SCHEMAS в .env, единственное место правки
       set_config('cdcinit.cdc_schemas',  :'cdc_schemas', false);

-- -----------------------------------------------------------------------------
--  Публикация
--
--  publish = 'insert, update, delete' — БЕЗ truncate. Причины:
--    1) в профиле slim события truncate всё равно отбрасываются
--       (ExtractNewRecordState нечего разворачивать: у truncate нет after);
--    2) consumer части 2 применяет построчные операции, TRUNCATE на приёмнике
--       должен быть отдельной согласованной процедурой, а не событием в потоке.
--
--  Альтернатива, которую стоит знать: CREATE PUBLICATION ... FOR TABLES IN
--  SCHEMA ref, catalog, sales — тогда новые таблицы попадают в публикацию
--  автоматически и ALTER ... ADD TABLE не нужен. Не выбрано сознательно:
--  теряется контроль над составом (любая новая таблица сразу уезжает в CDC) и
--  нельзя опубликовать подмножество колонок. Для 500 таблиц вариант рабочий,
--  но решение о нём принимает владелец данных.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_pub text := current_setting('cdcinit.publication');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = v_pub) THEN
        EXECUTE format(
            'CREATE PUBLICATION %I WITH (publish = ''insert, update, delete'')', v_pub);
        RAISE NOTICE 'публикация % создана', v_pub;
    ELSE
        RAISE NOTICE 'публикация % уже есть — проверяю состав', v_pub;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
--  Состав публикации
--
--  Добавляются все обычные таблицы схем ref/catalog/sales, которых в
--  публикации ещё нет. Схема internal НЕ публикуется.
--
--  Процедура добавления новой таблицы в контуре — ровно этот ALTER, см.
--  README раздел 3. Рестарт коннектора при этом не нужен: Debezium увидит
--  таблицу, как только по ней придёт первое событие из pgoutput. Но данные,
--  которые были в таблице ДО добавления в публикацию, в топик не попадут —
--  для них нужен ad-hoc снапшот или отдельная заливка.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_pub text := current_setting('cdcinit.publication');
    r     record;
    n     int := 0;
BEGIN
    FOR r IN
        SELECT n.nspname AS sch, c.relname AS tbl
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname = ANY (string_to_array(current_setting('cdcinit.cdc_schemas'), ','))
          AND NOT EXISTS (
                SELECT 1 FROM pg_publication_tables pt
                WHERE pt.pubname = v_pub
                  AND pt.schemaname = n.nspname
                  AND pt.tablename  = c.relname)
        ORDER BY 1, 2
    LOOP
        EXECUTE format('ALTER PUBLICATION %I ADD TABLE %I.%I', v_pub, r.sch, r.tbl);
        RAISE NOTICE 'в публикацию % добавлена %.%', v_pub, r.sch, r.tbl;
        n := n + 1;
    END LOOP;
    IF n = 0 THEN
        RAISE NOTICE 'состав публикации % актуален', v_pub;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
--  REPLICA IDENTITY
--
--  DEFAULT (только PK) на всех таблицах — это дефолт PostgreSQL, здесь он
--  выставляется явно, чтобы состояние было описано кодом, а не унаследовано.
--
--  ПОЧЕМУ НЕ FULL: при FULL в WAL пишется полный СТАРЫЙ образ строки на каждый
--  UPDATE/DELETE. На 500 таблицах это кратный рост WAL и кратный рост объёма
--  сообщений — прямое нарушение инварианта №2. Для применения в MS SQL
--  достаточно after-образа и PK: UPDATE идёт по PK, DELETE идёт по PK.
--
--  FULL нужен точечно и только по обоснованию (например, приёмник строит diff
--  по колонкам или нужен before-образ для аудита). Такие таблицы
--  перечисляются в переменной :replica_identity_full.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    r      record;
    v_tbl  text;
    v_full_arr text[] := ARRAY(
        SELECT btrim(x) FROM unnest(string_to_array(coalesce(current_setting('cdcinit.ri_full'),''), ',')) AS x
        WHERE btrim(x) <> ''
    );
BEGIN
    FOR r IN
        SELECT n.nspname AS sch, c.relname AS tbl, c.relreplident AS ri
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname = ANY (string_to_array(current_setting('cdcinit.cdc_schemas'), ','))
        ORDER BY 1, 2
    LOOP
        v_tbl := r.sch || '.' || r.tbl;
        IF v_tbl = ANY (v_full_arr) THEN
            IF r.ri <> 'f' THEN
                EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', r.sch, r.tbl);
                RAISE NOTICE '%: REPLICA IDENTITY FULL (по явному списку)', v_tbl;
            END IF;
        ELSIF r.ri <> 'd' THEN
            EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY DEFAULT', r.sch, r.tbl);
            RAISE NOTICE '%: REPLICA IDENTITY DEFAULT', v_tbl;
        END IF;
    END LOOP;
    IF array_length(v_full_arr, 1) IS NULL THEN
        RAISE NOTICE 'список REPLICA IDENTITY FULL пуст — все таблицы DEFAULT (только PK)';
    END IF;
END $$;

\echo '>>> 04-publication.sql: состав публикации и replica identity'
SELECT pt.pubname                      AS "публикация",
       pt.schemaname || '.' || pt.tablename AS "таблица",
       CASE c.relreplident WHEN 'd' THEN 'DEFAULT (PK)'
                           WHEN 'f' THEN 'FULL'
                           WHEN 'n' THEN 'NOTHING'
                           WHEN 'i' THEN 'INDEX' END AS "replica identity"
FROM pg_publication_tables pt
JOIN pg_class c ON c.relname = pt.tablename
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = pt.schemaname
WHERE pt.pubname = :'publication'
ORDER BY 2;
