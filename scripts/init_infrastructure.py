#!/usr/bin/env python3
"""
INIT INFRASTRUCTURE
===================
Buat Kafka topics + direktori HDFS.
Jalankan SETELAH docker-compose up selesai (semua container healthy).

Usage:
  python scripts/init_infrastructure.py
"""

import time, requests, json, sys, io
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Force UTF-8 output on Windows to support Unicode icons/box-drawing
if sys.platform.startswith('win'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

KAFKA_BOOTSTRAP = "localhost:9092"
HDFS_URL        = "http://localhost:19870/webhdfs/v1"
HDFS_USER       = "root"

TOPICS = [
    NewTopic(name="pangan-api", num_partitions=3, replication_factor=1),
    NewTopic(name="pangan-rss", num_partitions=2, replication_factor=1),
]

HDFS_DIRS = [
    "/data",
    "/data/pangan",
    "/data/pangan/pangan-api",
    "/data/pangan/pangan-rss",
    "/data/pangan/hasil",
]

R, G, Y, C, B, X = "\033[91m", "\033[92m", "\033[93m", "\033[96m", "\033[1m", "\033[0m"

def ok(s):   print(f"  {G}✓{X} {s}")
def fail(s): print(f"  {R}✗{X} {s}")
def step(s): print(f"\n{B}{C}── {s} ──{X}")


def init_kafka():
    step("Kafka Topics")
    for attempt in range(10):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=10_000)
            break
        except Exception as e:
            print(f"  Menunggu Kafka ({attempt+1}/10)... {e}")
            time.sleep(5)
    else:
        fail("Kafka tidak bisa dihubungi setelah 10 percobaan")
        return False

    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            ok(f"Topic '{topic.name}' berhasil dibuat ({topic.num_partitions} partisi)")
        except TopicAlreadyExistsError:
            ok(f"Topic '{topic.name}' sudah ada (skip)")
        except Exception as e:
            fail(f"Gagal buat topic '{topic.name}': {e}")

    admin.close()
    return True


def init_hdfs():
    step("HDFS Direktori")
    for attempt in range(10):
        try:
            r = requests.get(f"{HDFS_URL}/?op=GETFILESTATUS&user.name={HDFS_USER}", timeout=5)
            if r.status_code in (200, 404):
                break
        except Exception as e:
            print(f"  Menunggu HDFS ({attempt+1}/10)... {e}")
            time.sleep(6)
    else:
        fail("HDFS tidak bisa dihubungi")
        return False

    for path in HDFS_DIRS:
        url = f"{HDFS_URL}{path}?op=MKDIRS&user.name={HDFS_USER}"
        try:
            r = requests.put(url, timeout=10)
            if r.status_code in (200, 201):
                ok(f"HDFS: {path}")
            else:
                fail(f"HDFS {path}: status {r.status_code}")
        except Exception as e:
            fail(f"HDFS {path}: {e}")

    return True


def verify():
    step("Verifikasi")
    import subprocess
    checks = {
        "Kafka":              "docker exec kafka_pangan kafka-topics.sh --bootstrap-server localhost:9092 --list",
        "HDFS NameNode":      "curl -sf http://localhost:19870/",
        "Spark Master":       "curl -sf http://localhost:8080/",
        "YARN ResourceManager": "curl -sf http://localhost:8088/",
    }
    for name, cmd in checks.items():
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if result.returncode == 0:
            ok(f"{name} aktif")
        else:
            fail(f"{name} tidak merespons")


if __name__ == "__main__":
    print(f"\n{B}{C}╔══════════════════════════════════════════╗")
    print(f"║  INIT INFRASTRUCTURE — Pangan BigData    ║")
    print(f"╚══════════════════════════════════════════╝{X}\n")

    kafka_ok = init_kafka()
    hdfs_ok  = init_hdfs()
    verify()

    print(f"\n{B}Status:{X}")
    print(f"  Kafka : {'OK' if kafka_ok else 'GAGAL'}")
    print(f"  HDFS  : {'OK' if hdfs_ok else 'GAGAL'}")
    print(f"\n{G}Infrastruktur siap. Lanjut jalankan producer dan consumer.{X}\n")
