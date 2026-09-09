# =============================================================================
#  CDC-стенд: PostgreSQL -> Debezium -> Kafka
#
#  Все цели запускаются С ХОСТА, но вся работа идёт в контейнерах: на хосте
#  нужны только docker, make, bash и awk (jq — опционально, иначе берётся из
#  контейнера).
# =============================================================================
SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help
include .env

.PHONY: help up up-sink down clean reset-cdc build topics connector connector-full \
        connector-status connector-restart connector-delete load verify verify-infra \
        scenario benchmark contracts failover-test logs ps psql

help: ## показать эту справку
	@printf '\nЦели:\n'
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-20s\033[0m %s\n", $$1, $$2}'
	@printf '\nПорядок первого запуска: make up -> make connector -> make verify\n\n'

# -----------------------------------------------------------------------------
#  Жизненный цикл
# -----------------------------------------------------------------------------
build: ## собрать образ Connect с плагином Debezium
	docker compose build connect

up: ## поднять стенд (ДАННЫЕ СОХРАНЯЮТСЯ между up/down)
	docker compose up -d --build
	@printf '\nЖду готовности сервисов...\n'
	@until [ "$$(docker compose ps connect --format '{{.Health}}')" = healthy ]; do sleep 3; done
	@# -a обязательно: одноразовые pg-init и kafka-init уже вышли, и увидеть
	@# их код выхода (должен быть 0) важнее, чем короткий список.
	@docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
	@printf '\nKafka UI: http://localhost:$(KAFKA_UI_PORT)   Connect: $(CONNECT_HOST_URL)   Schema Registry: $(SCHEMA_REGISTRY_HOST_URL)\n'
	@printf 'Коннектор НЕ создаётся автоматически: make connector\n'

up-sink: ## дополнительно поднять MS SQL (приёмник Б, в части 1 не используется)
	@printf 'Образ MS SQL существует только под linux/amd64 — на arm64 это эмуляция.\n'
	docker compose --profile sink up -d mssql

down: ## остановить стенд, ДАННЫЕ СОХРАНИТЬ
	docker compose down --remove-orphans
	@printf '\nТома на месте: '
	@docker volume ls --filter name=$(COMPOSE_PROJECT_NAME) --format '{{.Name}}' | tr '\n' ' '
	@printf '\nСостояние БД и Kafka сохранено. make up продолжит с прежнего LSN.\n'

clean: ## ПОЛНЫЙ СБРОС: down -v, все тома удаляются (с подтверждением)
	@printf '\n\033[31mВНИМАНИЕ: будут удалены ВСЕ тома стенда:\033[0m\n'
	@printf '  pg_master_data, pg_replica1_data, pg_replica2_data, kafka_data, mssql_data\n\n'
	@printf 'Состояние PostgreSQL и Kafka описывает ОДНУ точку: подтверждённый LSN\n'
	@printf 'в слоте и offset'"'"'ы в connect-offsets. Поэтому сбрасывать их можно\n'
	@printf 'только ВМЕСТЕ — см. README, раздел 3. Отдельный сброс состояния CDC\n'
	@printf 'при живых данных — это make reset-cdc.\n\n'
	@read -p 'Удалить все тома? [yes/NO] ' a; [ "$$a" = yes ] || { echo 'отменено'; exit 1; }
	docker compose --profile sink --profile tools down -v --remove-orphans
	@printf '\nВсё удалено. make up поднимет стенд с нуля.\n'

reset-cdc: ## сбросить ТОЛЬКО состояние CDC, данные БД оставить
	./scripts/reset-cdc.sh

# -----------------------------------------------------------------------------
#  Топики и коннектор
# -----------------------------------------------------------------------------
topics: ## пересоздать/досоздать топики (идемпотентно) и показать их
	docker compose up -d kafka-init
	@docker compose logs kafka-init --no-log-prefix | tail -20
	@docker compose exec -T kafka kafka-topics --bootstrap-server $(KAFKA_INTERNAL) --list | sort

connector: ## поставить коннектор, профиль slim (основной)
	./scripts/create-connector.sh slim

connector-full: ## поставить коннектор, профиль full (только для замера объёма)
	./scripts/create-connector.sh full

connector-status: ## состояние коннекторов и задач
	@for c in $$(docker compose exec -T connect curl -sS $(CONNECT_HOST_URL:8083=8083)/connectors | tr -d '[]"' | tr ',' ' '); do \
	  printf '\n--- %s ---\n' "$$c"; \
	  docker compose exec -T connect curl -sS http://localhost:8083/connectors/$$c/status; printf '\n'; \
	done
	@printf '\n--- слоты ---\n'
	@docker compose exec -T pg-master psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) \
	  -c "SELECT slot_name, slot_type, failover, synced, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots ORDER BY 1;"

connector-restart: ## перезапустить задачу коннектора (перечитать ProducerConfig)
	docker compose exec -T connect curl -sS -X POST http://localhost:8083/connectors/$(CONNECTOR_NAME)/tasks/0/restart
	@printf 'задача перезапущена\n'

connector-delete: ## удалить коннектор (слот и топик остаются)
	docker compose exec -T connect curl -sS -X DELETE http://localhost:8083/connectors/$(CONNECTOR_NAME) || true
	@printf 'коннектор $(CONNECTOR_NAME) удалён; слот $(CDC_SLOT) НЕ удалён (slot.drop.on.stop=false)\n'

# -----------------------------------------------------------------------------
#  Нагрузка и проверки
# -----------------------------------------------------------------------------
load: ## смешанная нагрузка (переопределить: make load ARGS="bulk 5000 10")
	./scripts/load.sh $(or $(ARGS),mixed 20)

verify: ## проверка порядка по фактическому содержимому топика
	./scripts/verify-order.sh

verify-infra: ## проверка ограничений эксплуатации (слот, права, топики, связка producer'а)
	./scripts/verify-infra.sh

scenario: ## ГЛАВНЫЙ ТЕСТ: холодный старт (СБРАСЫВАЕТ ТОМА!)
	./scripts/scenario-cold-start.sh

benchmark: ## замер объёма и пропускной способности (профили slim и full, MAX_IN_FLIGHT 1 и 5)
	./scripts/benchmark.sh

failover-test: ## промоушен реплики и переключение CDC на новый мастер
	./scripts/test-failover.sh

contracts: ## выгрузить Avro-контракты из Schema Registry в contracts/generated/
	./scripts/export-contracts.sh

# -----------------------------------------------------------------------------
#  Наблюдение
# -----------------------------------------------------------------------------
logs: ## логи (make logs S=connect)
	docker compose logs -f --tail=200 $(or $(S),connect)

ps: ## статусы контейнеров
	@docker compose ps -a --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'

psql: ## psql на мастере
	docker compose exec pg-master psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)
