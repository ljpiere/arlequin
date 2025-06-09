#!/usr/bin/env python
import argparse, json, time, random
from pathlib import Path
import yaml
from faker import Faker
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parent
fake = Faker()

def load_cfg(path):  # YAML → dict
    with open(path) as f:
        return yaml.safe_load(f)

def synth_row(has_drift: bool):
    """Fila simulada: churn de clientes telco."""
    base = {
        "customer_id": fake.uuid4(),
        "plan": random.choice(["pre", "post"]),
        "tenure": random.randint(1, 72),
        "monthly_charges": round(random.uniform(15, 120), 2),
        "total_charges": 0.0,      # rellenamos abajo
        "churn": random.choice([0, 1]),
        "ts": fake.date_time_this_year().isoformat()
    }
    base["total_charges"] = round(base["tenure"] * base["monthly_charges"], 2)

    if has_drift:
        # ejemplo de DRIFT tipo 'shift' sobre monthly_charges
        base["monthly_charges"] = round(base["monthly_charges"] * 1.5, 2)
    return base

def main(cfg):
    prod = KafkaProducer(
        bootstrap_servers=cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        batch_size=32_768,
        acks="all"
    )

    total, batch, pct_drift = cfg["rows"], cfg["batch"], cfg["drift"]["start_after_pct"]
    drift_start = int(total * pct_drift / 100)
    for i in range(1, total + 1):
        drift_now = (i >= drift_start) and cfg["drift"]["type"] != "none"
        prod.send(cfg["topic"], synth_row(drift_now))

        if i % batch == 0:
            prod.flush()
            time.sleep(cfg["sleep_ms"] / 1000)

    prod.flush()
    prod.close()
    print(f"✔  Enviadas {total} filas → topic {cfg['topic']}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=str(ROOT / "config.yaml"))
    args = p.parse_args()
    main(load_cfg(args.config))
