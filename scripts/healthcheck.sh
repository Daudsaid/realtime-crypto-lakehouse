#!/usr/bin/env bash
# Smoke-check the whole stack: services, Kafka, Postgres serving tables, Metabase and Binance REST.
# Usage: bash scripts/healthcheck.sh   (exits non-zero if any check fails)
set -u
cd "$(dirname "$0")/.." || exit 1

failures=0
check() {
  local name=$1; shift
  if output=$("$@" 2>&1); then
    printf 'PASS  %-34s %s\n' "$name" "$(echo "$output" | head -1)"
  else
    printf 'FAIL  %-34s %s\n' "$name" "$(echo "$output" | head -1)"
    failures=$((failures + 1))
  fi
}

service_up() { [ -n "$(docker compose ps --status running --services | grep -x "$1")" ] && echo running; }
kafka_messages() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 \
    --topic crypto.trades.raw | awk -F: '{s += $3} END {if (s > 0) print s " messages in topic"; else exit 1}'
}
pg_candles() {
  docker compose exec -T postgres psql -U "${POSTGRES_USER:-crypto}" -d "${POSTGRES_DB:-crypto}" -At \
    -c "select count(*) || ' candles, newest ' || max(minute) from gold.ohlc_1m"
}

for svc in kafka postgres metabase producer pipeline; do check "service $svc" service_up "$svc"; done
check "kafka topic crypto.trades.raw" kafka_messages
check "postgres gold.ohlc_1m" pg_candles
check "metabase /api/health" curl -fsS --max-time 10 http://localhost:3000/api/health
check "binance REST ping" curl -fsS --max-time 10 https://data-api.binance.vision/api/v3/ping

echo
[ "$failures" -eq 0 ] && echo "All checks passed" || echo "$failures check(s) failed"
exit "$failures"
