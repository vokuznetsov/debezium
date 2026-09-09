-- =============================================================================
--  03-slots.sql — слоты репликации
--
--  ОГРАНИЧЕНИЕ КОНТУРА, которое воспроизводит стенд: Debezium не создаёт
--  слотов. Все слоты создаёт devops заранее, вот этим скриптом, под
--  суперпользователем. Роль debezium этого сделать не может (см. 01-roles.sql).
--
--  Идемпотентен: каждый слот создаётся только при NOT EXISTS в
--  pg_replication_slots.
--
--  Переменные psql:
--    :replica1_slot, :replica2_slot — имена физических слотов реплик
--    :cdc_slot                      — имя логического слота CDC
--    :create_cdc_slot               — true/false: создавать ли логический слот
--
--  Зачем :create_cdc_slot. Граница «что попадёт в топик» проходит по моменту
--  СОЗДАНИЯ СЛОТА, а не по моменту старта коннектора. scenario-cold-start.sh
--  доказывает это явно: сначала поднимает базу с данными БЕЗ слота
--  (CREATE_CDC_SLOT=false), потом создаёт слот, потом пишет изменения, и
--  только потом поднимает коннектор. При обычном `make up` значение true и
--  слот создаётся сразу.
-- =============================================================================

SELECT set_config('cdcinit.replica1_slot', :'replica1_slot', false),
       set_config('cdcinit.replica2_slot', :'replica2_slot', false),
       set_config('cdcinit.cdc_slot',      :'cdc_slot',      false);

-- -----------------------------------------------------------------------------
--  Физические слоты реплик
--
--  Создаются ДО старта реплик: pg_basebackup вызывается с -S <slot> и БЕЗ -C,
--  то есть слот обязан уже существовать. Порядок обеспечивает compose
--  (depends_on: pg-init: service_completed_successfully), а не sleep.
--
--  immediately_reserve = true: слот резервирует WAL сразу, а не при первом
--  подключении. Иначе между созданием слота и стартом реплики мастер может
--  удалить нужный WAL, и базовая копия окажется бесполезной.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    v_slot text;
BEGIN
    FOREACH v_slot IN ARRAY ARRAY[
        current_setting('cdcinit.replica1_slot'),
        current_setting('cdcinit.replica2_slot')
    ] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = v_slot) THEN
            PERFORM pg_create_physical_replication_slot(v_slot, true, false);
            RAISE NOTICE 'физический слот % создан', v_slot;
        ELSE
            RAISE NOTICE 'физический слот % уже есть — пропуск', v_slot;
        END IF;
    END LOOP;
END $$;

-- -----------------------------------------------------------------------------
--  Логический слот CDC — с failover => true
--
--  Логические слоты живут только на мастере. Без failover-флага переключение
--  на реплику означает потерю слота и ПОЛНУЮ пересинхронизацию CDC: (Б)
--  придётся заливать заново.
--
--  В PostgreSQL 17 это решается штатно: слот с failover => true
--  синхронизируется на реплики встроенным slotsync-воркером
--  (sync_replication_slots = on на реплике), и после промоушена он там уже
--  есть, с подтверждённым LSN.
--
--  Debezium 2.6 сам этот флаг НЕ выставляет (DBZ-8412 не реализован), и
--  выставить его postfactum у существующего слота нельзя — только пересоздать,
--  а пересоздание = потеря позиции. Поэтому создаём сразу правильным.
--  Решение принятое и согласованное.
--
--  Позиционные аргументы (в PG 17 именно такой порядок):
--    slot_name, plugin, temporary, twophase, failover
-- -----------------------------------------------------------------------------
\if :create_cdc_slot
DO $$
DECLARE
    v_slot text := current_setting('cdcinit.cdc_slot');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = v_slot) THEN
        PERFORM pg_create_logical_replication_slot(
                    v_slot,
                    'pgoutput',
                    false,   -- temporary: слот должен переживать разрыв соединения
                    false,   -- two_phase:  подготовленные транзакции не используем
                    true);   -- failover:   ГЛАВНЫЙ флаг, синхронизация на реплики
        RAISE NOTICE 'логический слот % создан (failover = true)', v_slot;
    ELSE
        RAISE NOTICE 'логический слот % уже есть — пропуск', v_slot;
    END IF;
END $$;
\else
\echo '>>> 03-slots.sql: CREATE_CDC_SLOT=false — логический слот СОЗНАТЕЛЬНО не создан'
\echo '    (режим scenario-cold-start.sh: слот создаётся отдельным шагом)'
\endif

\echo '>>> 03-slots.sql: состояние слотов'
SELECT slot_name        AS "слот",
       slot_type        AS "тип",
       plugin,
       temporary        AS "врем.",
       failover,
       synced,
       active,
       restart_lsn,
       invalidation_reason
FROM pg_replication_slots
ORDER BY slot_type, slot_name;
