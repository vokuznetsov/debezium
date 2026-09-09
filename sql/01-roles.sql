-- =============================================================================
--  01-roles.sql — роли кластера
--
--  Идемпотентен: роли создаются через DO-блок с проверкой pg_roles, гранты
--  накладываются повторно без ошибок.
--
--  Запускается сервисом pg-init, а НЕ через /docker-entrypoint-initdb.d:
--  initdb-хуки отрабатывают только на пустом каталоге данных, а тома у нас
--  сохраняются между `make down` / `make up`.
--
--  ГРАБЛИ psql, из-за которых код выглядит именно так:
--  psql НЕ подставляет свои переменные (:'var') внутрь строк в долларовых
--  кавычках — тело DO $$ ... $$ для него непрозрачный литерал. Поэтому
--  значения сначала кладутся в GUC через set_config() (там интерполяция
--  работает, это обычный SQL-вызов), а DO-блок читает их current_setting().
-- =============================================================================

SELECT set_config('cdcinit.repl_user',     :'repl_user',     false),
       set_config('cdcinit.repl_password', :'repl_password', false),
       set_config('cdcinit.dbz_user',      :'dbz_user',      false),
       set_config('cdcinit.dbz_password',  :'dbz_password',  false);

-- -----------------------------------------------------------------------------
--  Роль физической репликации: под ней работают pg-replica-1/2 и pg_basebackup
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_user text := current_setting('cdcinit.repl_user');
    v_pass text := current_setting('cdcinit.repl_password');
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = v_user) THEN
        EXECUTE format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', v_user, v_pass);
        RAISE NOTICE 'роль % создана', v_user;
    ELSE
        RAISE NOTICE 'роль % уже есть — пропуск', v_user;
    END IF;
END $$;

-- -----------------------------------------------------------------------------
--  Роль Debezium
--
--  Что ей нужно и почему:
--    LOGIN        — обычное подключение, чтение структуры таблиц из каталога
--    REPLICATION  — открыть replication-соединение и читать поток pgoutput
--    USAGE/SELECT — Debezium строит Avro-схемы по каталогу; при
--                   snapshot.mode=no_data данные он не вычитывает, но SELECT
--                   нужен для валидации и для точечного ad-hoc снапшота
--
--  Чего у неё НЕТ и это принципиально:
--    NOSUPERUSER, NOCREATEDB, NOCREATEROLE
--    нет CREATE на базе -> НЕ МОЖЕТ создать публикацию (запрет настоящий)
--    отозван EXECUTE на pg_create_*_replication_slot -> закрыт SQL-путь
--                                                       создания слотов
--
--  ЧЕСТНАЯ ОГОВОРКА (продублирована в README, разделы 2 и 8):
--  полностью запретить создание слота роли с атрибутом REPLICATION в
--  PostgreSQL НЕЛЬЗЯ. Команда CREATE_REPLICATION_SLOT идёт по
--  репликационному протоколу и проверяет только сам атрибут REPLICATION, а
--  без него Debezium не прочитает поток вообще. Отдельного права
--  «читать готовый слот, но не создавать» в PostgreSQL не существует.
--  Поэтому: SQL-путь закрыт правами (его и используют люди и devops-скрипты),
--  протокольный путь закрыт конфигом коннектора (slot.name на готовый слот,
--  publication.autocreate.mode=disabled, slot.drop.on.stop=false) и ревью.
--  Публикации закрыты по-настоящему, правами. verify-infra.sh проверяет и то,
--  и другое и печатает эту разницу, чтобы она не потерялась.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_user text := current_setting('cdcinit.dbz_user');
    v_pass text := current_setting('cdcinit.dbz_password');
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = v_user) THEN
        EXECUTE format(
            'CREATE ROLE %I WITH LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
            v_user, v_pass);
        RAISE NOTICE 'роль % создана', v_user;
    ELSE
        EXECUTE format(
            'ALTER ROLE %I WITH LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE',
            v_user);
        RAISE NOTICE 'роль % уже есть — атрибуты выровнены', v_user;
    END IF;
END $$;

-- Снимаем CREATE на базе с PUBLIC и с самой роли debezium: без CREATE на базе
-- нельзя создать ни публикацию, ни схему.
DO $$
DECLARE
    v_user text := current_setting('cdcinit.dbz_user');
    v_db   text := current_database();
BEGIN
    EXECUTE format('REVOKE CREATE ON DATABASE %I FROM PUBLIC', v_db);
    EXECUTE format('REVOKE CREATE ON DATABASE %I FROM %I', v_db, v_user);
END $$;

-- Закрываем SQL-путь создания и удаления слотов. Функции по умолчанию
-- исполняемы для PUBLIC и полагаются только на внутреннюю проверку атрибута
-- REPLICATION — этого нам мало. Суперпользователь проверки прав обходит,
-- поэтому devops-скрипты (работают под postgres) продолжают работать.
REVOKE EXECUTE ON FUNCTION
    pg_catalog.pg_create_logical_replication_slot(name, name, boolean, boolean, boolean)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION
    pg_catalog.pg_create_physical_replication_slot(name, boolean, boolean)
    FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION
    pg_catalog.pg_drop_replication_slot(name)
    FROM PUBLIC;

\echo '>>> 01-roles.sql: атрибуты ролей'
SELECT rolname, rolcanlogin, rolreplication, rolsuper, rolcreatedb, rolcreaterole
FROM pg_roles
WHERE rolname IN (:'repl_user', :'dbz_user')
ORDER BY rolname;
