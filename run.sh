#!/usr/bin/env bash
# ============================================================
# run.sh — Script otomatis untuk menjalankan seluruh pipeline
# Pangan Big Data Analytics
# ============================================================
# Usage: bash run.sh [start|stop|restart|status|logs]
# ============================================================

set -e

COMPOSE="docker compose"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

step() { echo -e "\n${BLUE}══ $1 ══${NC}"; }
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }
err()  { echo -e "  ${RED}✗${NC} $1"; }

# ────────────────────────────────────────────────────────────
start() {
  step "1. Menjalankan Docker containers"
  $COMPOSE up -d --build
  ok "Containers berjalan"

  step "2. Menunggu Kafka & HDFS siap (±60 detik)"
  sleep 60

  step "3. Init Kafka topics + HDFS direktori"
  pip install -q kafka-python-ng requests
  python scripts/init_infrastructure.py

  step "4. Menjalankan Kafka Producer (background)"
  pip install -q -r requirements.txt
  python kafka/producer_api.py &
  PRODUCER_API_PID=$!
  ok "producer_api.py PID=$PRODUCER_API_PID"

  python kafka/producer_rss.py &
  PRODUCER_RSS_PID=$!
  ok "producer_rss.py PID=$PRODUCER_RSS_PID"

  step "5. Menjalankan Kafka Consumer (background)"
  python kafka/consumer_to_hdfs.py &
  CONSUMER_PID=$!
  ok "consumer_to_hdfs.py PID=$CONSUMER_PID"

  echo -e "\n${GREEN}╔══════════════════════════════════════════╗"
  echo    "║  PIPELINE BERJALAN ✓                     ║"
  echo    "║  Dashboard : http://localhost:5002        ║"
  echo    "║  Kafka UI  : http://localhost:9092        ║"
  echo    "║  HDFS UI   : http://localhost:19870       ║"
  echo    "║  Spark UI  : http://localhost:8080        ║"
  echo    "║  YARN UI   : http://localhost:8088        ║"
  echo -e "╚══════════════════════════════════════════╝${NC}"
  echo ""
  warn "Untuk Spark Analysis, jalankan: python spark/analysis.py"
}

stop() {
  step "Menghentikan pipeline"
  pkill -f producer_api.py   2>/dev/null && ok "producer_api dihentikan"
  pkill -f producer_rss.py   2>/dev/null && ok "producer_rss dihentikan"
  pkill -f consumer_to_hdfs.py 2>/dev/null && ok "consumer dihentikan"
  $COMPOSE down
  ok "Containers dihentikan"
}

status() {
  step "Status Services"
  $COMPOSE ps
}

logs() {
  $COMPOSE logs -f --tail=50
}

case "${1:-start}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop && start ;;
  status)  status ;;
  logs)    logs ;;
  *) echo "Usage: bash run.sh [start|stop|restart|status|logs]" ;;
esac
