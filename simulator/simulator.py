"""Simulador del taller.

1. Prepara tópicos Kafka o publica en colas SQS, según el transporte elegido.
2. Reproduce data/events.jsonl en el broker al ritmo de `replay_at`, acelerado.
3. Sirve un panel (http://localhost:8090) para la demo: abrir una orden, capturar y
   aprobar/rechazar/anular su inspección, cambiar de etapa, dar de baja y registrar la
   llegada de una compra. Esos eventos salen con la hora real.

Variables de entorno (o argumentos, ver --help):
    EVENT_TRANSPORT kafka         BROKER  localhost:19092
    EVENTS  data/events.jsonl     SPEED   3600 (1 h simulada por segundo; 0 = sin pausas)
    PORT    8090                  SQS_QUEUE_URLS  JSON de tópico a URL, para transporte SQS
    SQS_QUEUE_PREFIX  inventory-broker (sin SQS_QUEUE_URLS ni endpoint: busca las colas de AWS por nombre)
    SIM_STATE_PARAM   /hack-saltillo/simulator/state (SSM; solo SQS de AWS)
    API_URL           API de inventario, para saber qué parte del dataset ya procesó

Contra SQS de AWS el dataset se carga una sola vez: el avance se guarda en SSM Parameter Store
(SIM_STATE_PARAM). Si el simulador se detiene a medias, la siguiente vez retoma donde se quedó; ya
completo, solo levanta el panel. `--force-replay` lo vuelve a mandar completo (la API descarta los
duplicados por `event_id`, así que reenviar no cambia los datos).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import signal
import sys
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
INPUT_TOPICS = ["shop.catalog", "shop.work_orders", "shop.inspections", "shop.purchasing"]
OUTPUT_TOPICS = ["inventory.events", "inventory.dlq", "notifications"]
STAGES = ["awaiting_quick_inspection", "quick_inspection", "awaiting_full_inspection", "full_inspection",
          "cleaning", "assembly", "run_in", "final_assembly", "final_release"]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# --- broker --------------------------------------------------------------------
def kafka_config(broker: str) -> dict:
    config = {"bootstrap.servers": broker}
    security_protocol = os.environ.get("BROKER_SECURITY_PROTOCOL", "PLAINTEXT")
    if security_protocol != "PLAINTEXT":
        config["security.protocol"] = security_protocol
    for env_name, kafka_name in (
        ("BROKER_SASL_MECHANISM", "sasl.mechanism"),
        ("BROKER_SASL_USERNAME", "sasl.username"),
        ("BROKER_SASL_PASSWORD", "sasl.password"),
        ("BROKER_SSL_CA_LOCATION", "ssl.ca.location"),
    ):
        value = os.environ.get(env_name)
        if value:
            config[kafka_name] = value
    return config


class KafkaPublisher:
    def __init__(self, broker: str) -> None:
        from confluent_kafka import Producer

        self.producer = Producer({**kafka_config(broker), "enable.idempotence": True, "acks": "all"})
        self.lock = threading.Lock()

    def send(self, topic: str, event: dict) -> None:
        with self.lock:
            self.producer.produce(topic, key=event["key"].encode(), value=json.dumps(event, ensure_ascii=False).encode())
            self.producer.poll(0)

    def flush(self) -> None:
        with self.lock:
            self.producer.flush(10)


class SqsPublisher:
    def __init__(self) -> None:
        import boto3

        required_topics = set(INPUT_TOPICS + OUTPUT_TOPICS)
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        endpoint_url = os.environ.get("SQS_ENDPOINT_URL")
        self.client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)
        configured_urls = os.environ.get("SQS_QUEUE_URLS")
        if configured_urls:
            self.queue_urls = json.loads(configured_urls)
            missing_topics = required_topics - set(self.queue_urls)
            if missing_topics:
                raise ValueError(f"faltan URLs SQS para: {', '.join(sorted(missing_topics))}")
        elif endpoint_url:
            self.queue_urls = self._create_local_queues()
        else:
            self.queue_urls = self._find_aws_queues()

        # Un solo lote pendiente, de una sola cola: los mensajes salen en el mismo orden en que
        # llegan a send(), también entre colas distintas, y nunca se quedan detenidos esperando a
        # juntar 10 de su cola (antes un evento podía esperar minutos y perderse al detener).
        self.pending: list[dict] = []
        self.pending_url: str | None = None
        self.lock = threading.Lock()

    def _find_aws_queues(self) -> dict[str, str]:
        prefix = os.environ.get("SQS_QUEUE_PREFIX", "inventory-broker")
        queue_urls = {}
        for topic in INPUT_TOPICS + OUTPUT_TOPICS:
            # Mismos nombres que sqs.tf: shop.work_orders → inventory-broker-shop-work-orders.fifo
            queue_name = f"{prefix}-{topic.replace('.', '-').replace('_', '-')}.fifo"
            queue_urls[topic] = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        return queue_urls

    def _create_local_queues(self) -> dict[str, str]:
        prefix = os.environ.get("SQS_QUEUE_PREFIX", "inventory-broker")
        attributes = {
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
            "VisibilityTimeout": "60",
            "MessageRetentionPeriod": "1209600",
            "ReceiveMessageWaitTimeSeconds": "20",
        }
        queue_urls = {}
        for topic in INPUT_TOPICS + OUTPUT_TOPICS:
            queue_name = f"{prefix}-{topic.replace('.', '-')}.fifo"
            for attempt in range(30):
                try:
                    response = self.client.create_queue(QueueName=queue_name, Attributes=attributes)
                    queue_urls[topic] = response["QueueUrl"]
                    break
                except Exception:
                    if attempt == 29:
                        raise
                    if attempt % 5 == 0:
                        print("esperando al endpoint SQS local", flush=True)
                    time.sleep(1)
        return queue_urls

    def send(self, topic: str, event: dict) -> None:
        queue_url = self.queue_urls[topic]
        entry = {
            "MessageBody": json.dumps({"topic": topic, "event": event}, ensure_ascii=False),
            "MessageGroupId": str(event["key"]),
            "MessageDeduplicationId": str(event["event_id"]),
        }
        with self.lock:
            if self.pending and self.pending_url != queue_url:
                self._send_pending()
            self.pending_url = queue_url
            self.pending.append(entry)
            if len(self.pending) >= 10:
                self._send_pending()

    def _send_pending(self) -> None:
        batch = self.pending
        for attempt in range(5):
            entries = [{"Id": str(index), **entry} for index, entry in enumerate(batch)]
            response = self.client.send_message_batch(QueueUrl=self.pending_url, Entries=entries)
            failures = response.get("Failed", [])
            if not failures:
                break
            # Reintenta solo los que fallaron, en su mismo orden.
            failed_ids = {int(item["Id"]) for item in failures}
            batch = [entry for index, entry in enumerate(batch) if index in failed_ids]
            if attempt == 4:
                details = ", ".join(f"{item['Id']}: {item['Message']}" for item in failures)
                raise RuntimeError(f"falló el envío de mensajes SQS: {details}")
            time.sleep(1 + attempt)
        self.pending = []

    def flush(self) -> None:
        with self.lock:
            if self.pending:
                self._send_pending()


def ensure_topics(broker: str, reset: bool) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(kafka_config(broker))
    topics = INPUT_TOPICS + OUTPUT_TOPICS
    for attempt in range(60):
        try:
            existing = set(admin.list_topics(timeout=5).topics)
            break
        except Exception as exc:  # broker todavía arrancando
            print(f"esperando al broker ({exc})", flush=True)
            time.sleep(2)
    else:
        raise SystemExit("el broker no respondió")

    if reset:
        to_delete = [t for t in topics if t in existing]
        if to_delete:
            for fut in admin.delete_topics(to_delete, operation_timeout=30).values():
                fut.result()
            time.sleep(2)
            existing -= set(to_delete)
        print(f"tópicos borrados: {to_delete}", flush=True)

    replication_factor = int(os.environ.get("TOPIC_REPLICATION_FACTOR", "1"))
    retention_ms = os.environ.get("TOPIC_RETENTION_MS", "7776000000")
    missing = [NewTopic(t, num_partitions=3, replication_factor=replication_factor,
                        config={"retention.ms": retention_ms})
               for t in topics if t not in existing]
    if missing:
        for topic, fut in admin.create_topics(missing).items():
            fut.result()
            print(f"tópico creado: {topic}", flush=True)


# --- estado persistente ----------------------------------------------------------
class SimState:
    """Avance del dataset y consecutivos del panel, compartidos por quien corra el simulador.

    Con SQS de AWS vive en SSM Parameter Store (un JSON); con Kafka o un SQS local queda en memoria
    y el simulador se comporta como antes (reproduce todo en cada arranque).
    """

    def __init__(self, parameter: str | None) -> None:
        self.parameter = parameter
        self.lock = threading.Lock()
        self.client = None
        self.data: dict = {}
        if parameter:
            import boto3

            self.client = boto3.client("ssm", region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
            try:
                value = self.client.get_parameter(Name=parameter)["Parameter"]["Value"]
                self.data = json.loads(value)
            except self.client.exceptions.ParameterNotFound:
                self.data = {}

    @property
    def persistent(self) -> bool:
        return self.client is not None

    def save(self, **changes) -> None:
        with self.lock:
            self.data.update(changes, updated_at=now_iso())
            if self.client:
                self.client.put_parameter(Name=self.parameter, Value=json.dumps(self.data), Type="String",
                                          Overwrite=True)


def dataset_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def first_unprocessed(records: list[dict], api_url: str | None) -> int:
    """Índice del primer registro del dataset que la API todavía no tiene en `event_inbox`.

    Sirve la primera vez, cuando no hay avance guardado pero alguien ya mandó parte del dataset.
    Si la API no responde, empieza desde 0: reenviar es seguro, la API descarta duplicados.
    """
    if not api_url:
        return 0
    seen: set[str] = set()
    try:
        for topic in INPUT_TOPICS:
            url = f"{api_url.rstrip('/')}/events?origin=inbox&topic={topic}&limit=500"
            with urllib.request.urlopen(url, timeout=30) as response:
                seen |= {item["event_id"] for item in json.load(response)["items"]}
    except Exception as exc:
        print(f"no se pudo consultar la API ({exc}); el dataset se manda desde el inicio", flush=True)
        return 0
    for index, rec in enumerate(records):
        if rec["event"]["event_id"] not in seen:
            return index
    return len(records)


# --- estado del taller (para el panel) ------------------------------------------
class Shop:
    def __init__(self, events_path: Path, publisher: KafkaPublisher | SqsPublisher,
                 state: SimState | None = None) -> None:
        self.pub = publisher
        self.store = state or SimState(None)
        self.lock = threading.Lock()
        self.records = [json.loads(line) for line in events_path.open(encoding="utf-8") if line.strip()]
        self.dataset_id = dataset_hash(events_path)
        self.parts: dict[int, dict] = {}
        self.models: dict[int, dict] = {}
        for rec in self.records:  # catálogo vigente al final del archivo
            ev = rec["event"]
            if ev["event_type"] == "part.upserted":
                self.parts[ev["data"]["part_id"]] = ev["data"]
            elif ev["event_type"] == "bom.upserted":
                self.models[ev["data"]["model_id"]] = ev["data"]
        self.replay = {"sent": 0, "total": len(self.records), "sim_time": None, "done": False, "speed": None}
        self.replay_cursor = 0
        self.orders: dict[int, dict] = {}
        self.inspections: dict[int, dict] = {}
        # Los ids del panel no pueden repetirse entre arranques: una orden 90001 nueva con el código
        # de otra ya procesada se mezclaba con la anterior en la base. Con estado persistente se
        # continúa la secuencia; sin él (primer arranque) parten de la hora, lejos del dataset.
        base = int(time.time()) // 10
        self.seq = self.store.data.get("seq") or {
            "work_order": base, "inspection": base, "item": base,
            "request": base, "purchase_request": base, "line": base}
        today = datetime.now(timezone.utc).strftime("%d%m%y")
        if self.store.data.get("codes_day") == today:
            self.codes_today = self.store.data["codes_today"]
        else:
            # Código R + ddmmyy + 3 dígitos; el arranque del día parte de la hora para no chocar
            # con órdenes abiertas hoy antes de guardar estado.
            seconds = int(time.time()) % 86400
            self.codes_today = 100 + seconds // 120

    def next(self, kind: str) -> int:
        value = self.seq[kind]
        self.seq[kind] += 1
        return value

    def _persist_panel(self) -> None:
        self.store.save(seq=self.seq, codes_day=datetime.now(timezone.utc).strftime("%d%m%y"),
                        codes_today=self.codes_today)

    def emit(self, topic: str, event_type: str, key: str, data: dict) -> dict:
        event = {"event_id": str(uuid.uuid4()), "event_type": event_type, "event_version": 1,
                 "occurred_at": now_iso(), "source": "shop-floor", "key": key, "data": data}
        self.pub.send(topic, event)
        self.pub.flush()
        self._persist_panel()
        return event

    def emit_notification(self, event_type: str, key: str, data: dict) -> dict:
        return self.emit("notifications", event_type, key, data)

    # --- replay ---
    def save_progress(self, done: bool = False) -> None:
        """Manda lo pendiente y guarda hasta dónde llegó el dataset (solo lo que ya salió)."""
        self.pub.flush()
        self.store.save(dataset=self.dataset_id, next=self.replay_cursor, total=len(self.records), done=done)

    def run_replay(self, speed: float, start: int = 0) -> None:
        self.replay["speed"] = speed
        self.replay["sent"] = start
        self.replay_cursor = start
        previous = None
        last_save = time.monotonic()
        print(f"dataset: mandando registros {start + 1}-{len(self.records)} de {len(self.records)}", flush=True)
        for index in range(start, len(self.records)):
            rec = self.records[index]
            at = parse_iso(rec["replay_at"])
            if previous is not None and speed > 0:
                wait = (at - previous).total_seconds() / speed
                if wait > 0:
                    # Antes de esperar sale todo lo anterior: nada se queda en el lote.
                    self.pub.flush()
                    if time.monotonic() - last_save > 10:
                        self.save_progress()
                        last_save = time.monotonic()
                    time.sleep(min(wait, 30))
            previous = at
            self.pub.send(rec["topic"], rec["event"])
            self.replay_cursor = index + 1
            self.replay["sent"] = index + 1
            self.replay["sim_time"] = rec["replay_at"]
            if speed <= 0 and index % 200 == 0:
                self.save_progress()
        self.save_progress(done=True)
        self.replay["done"] = True
        print(f"replay terminado: {len(self.records)} registros del dataset en las colas", flush=True)

    # --- acciones del panel ---
    def open_order(self, model_id: int) -> dict:
        with self.lock:
            model = self.models[model_id]
            wo_id = self.next("work_order")
            day = datetime.now(timezone.utc).strftime("%d%m%y")
            code = f"R{day}{self.codes_today:03d}"
            self.codes_today += 1
            order = {"work_order_id": wo_id, "code": code, "model_id": model_id, "model_name": model["model_name"],
                     "stage": "awaiting_quick_inspection", "deleted": False}
            self.orders[wo_id] = order
        self.emit("shop.work_orders", "work_order.opened", code, {
            "work_order_id": wo_id, "code": code, "model_id": model_id,
            "department": "spindles", "received_at": now_iso()})
        self.change_stage(wo_id, "quick_inspection")
        return order

    def change_stage(self, wo_id: int, to_stage: str) -> None:
        order = self.orders[wo_id]
        if to_stage not in STAGES:
            raise ValueError(f"etapa desconocida: {to_stage}")
        self.emit("shop.work_orders", "work_order.stage_changed", order["code"],
                  {"work_order_id": wo_id, "from_stage": order["stage"], "to_stage": to_stage})
        order["stage"] = to_stage

    def submit_inspection(self, wo_id: int, kind: str, rows: list[dict]) -> dict:
        order = self.orders[wo_id]
        lines = {ln["bom_line_id"]: ln for ln in self.models[order["model_id"]]["lines"]}
        items = []
        for row in rows:
            line = lines[int(row["bom_line_id"])]
            condition = row["condition"]
            action = None if condition == "ok" else ("buy" if condition == "missing" else row.get("action") or "buy")
            qty = row.get("quantity")
            items.append({
                "inspection_item_id": self.next("item"), "bom_line_id": line["bom_line_id"],
                "part_id": line["part_id"], "name": line["name"], "group_name": line["group_name"],
                "condition": condition, "action": action,
                "quantity": None if condition == "ok" or qty in (None, "") else int(qty),
                "note": row.get("note") or None,
            })
        inspection = {"inspection_id": self.next("inspection"), "work_order_id": wo_id, "kind": kind,
                      "items": items, "status": "submitted"}
        self.inspections[inspection["inspection_id"]] = inspection
        self.emit("shop.inspections", "inspection.submitted", order["code"], self._inspection_data(inspection))
        return inspection

    def _inspection_data(self, inspection: dict) -> dict:
        return {k: inspection[k] for k in ("inspection_id", "work_order_id", "kind", "items")}

    def close_inspection(self, inspection_id: int, outcome: str, reason: str | None) -> dict:
        inspection = self.inspections[inspection_id]
        order = self.orders[inspection["work_order_id"]]
        base = {"inspection_id": inspection_id, "work_order_id": order["work_order_id"]}
        if outcome == "approve":
            self.emit("shop.inspections", "inspection.approved", order["code"],
                      {**self._inspection_data(inspection), "approved_by": 12, "approved_at": now_iso()})
            inspection["status"] = "approved"
            if inspection["kind"] == "quick":
                self.change_stage(order["work_order_id"], "awaiting_full_inspection")
        elif outcome == "reject":
            self.emit("shop.inspections", "inspection.rejected", order["code"],
                      {**base, "reason": reason or "Revisar la evaluación"})
            inspection["status"] = "rejected"
        elif outcome == "discard":
            self.emit("shop.inspections", "inspection.discarded", order["code"], base)
            inspection["status"] = "discarded"
        elif outcome == "void":
            self.change_stage(order["work_order_id"], "quick_inspection")
            self.emit("shop.inspections", "inspection.voided", order["code"],
                      {**base, "reason": reason or "Se regresó la etapa"})
            inspection["status"] = "voided"
        else:
            raise ValueError(outcome)
        return inspection

    def delete_order(self, wo_id: int) -> None:
        order = self.orders[wo_id]
        self.emit("shop.work_orders", "work_order.deleted", order["code"],
                  {"work_order_id": wo_id, "code": order["code"]})
        order["deleted"] = True

    def receive_purchase(self, body: dict) -> dict:
        number = str(self.next("request"))
        data = {
            "purchase_request_id": self.next("purchase_request"), "request_number": number,
            "line_id": self.next("line"), "part_number": body["part_number"],
            "description": body.get("description") or body["part_number"],
            "quantity": int(body["quantity"]), "unit_price": f"{float(body.get('unit_price') or 0):.2f}",
            "currency": body.get("currency") or "MXN", "purpose": body.get("purpose") or "restock",
            "received_at": now_iso(), "work_order_code": body.get("work_order_code") or None,
        }
        return self.emit("shop.purchasing", "purchase.item_received", f"PR-{number}", data)

    def state(self) -> dict:
        return {
            "replay": self.replay,
            "models": [{"model_id": m["model_id"], "brand": m["brand"], "model_name": m["model_name"],
                        "lines": m["lines"]} for m in self.models.values()],
            "parts": [{"part_id": p["part_id"], "sku": p["sku"], "name": p["name"]}
                      for p in sorted(self.parts.values(), key=lambda p: p["part_id"])],
            "orders": [{**o, "inspections": [i for i in self.inspections.values()
                                             if i["work_order_id"] == o["work_order_id"]]}
                       for o in sorted(self.orders.values(), key=lambda o: -o["work_order_id"])],
        }


# --- HTTP ----------------------------------------------------------------------
def make_handler(shop: Shop):
    page = (HERE / "panel.html").read_bytes()

    # Desplegado detrás de CloudFront: el panel vive en BASE_PATH (/simulador) y pide contraseña
    # (PANEL_PASSWORD, Basic Auth con cualquier usuario). En local las dos van vacías.
    base_path = os.environ.get("BASE_PATH", "").rstrip("/")
    password = os.environ.get("PANEL_PASSWORD", "")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silencio
            pass

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, payload) -> None:
            self._send(status, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")

        def _route(self) -> str | None:
            """Ruta sin BASE_PATH, o None si ya se respondió (redirección o falta de contraseña)."""
            path = self.path.split("?", 1)[0]
            if base_path:
                if path == base_path:  # sin la "/" final las rutas relativas del panel no resuelven
                    self.send_response(301)
                    self.send_header("Location", f"{base_path}/")
                    self.end_headers()
                    return None
                path = path[len(base_path):] if path.startswith(f"{base_path}/") else path
            if path == "/health":  # el balanceador revisa sin contraseña
                return path
            if password and not self._authorized():
                self.send_response(401)
                self.send_header("WWW-Authenticate", 'Basic realm="Simulador del taller", charset="UTF-8"')
                self.send_header("Content-Length", "0")
                self.end_headers()
                return None
            return path

        def _authorized(self) -> bool:
            header = self.headers.get("Authorization", "")
            if not header.startswith("Basic "):
                return False
            try:
                _, _, given = base64.b64decode(header[6:]).decode().partition(":")
            except Exception:
                return False
            return hmac.compare_digest(given.encode(), password.encode())

        def do_GET(self):
            path = self._route()
            if path is None:
                return
            if path in ("/", "/index.html"):
                self._send(200, page, "text/html; charset=utf-8")
            elif path == "/health":
                self._json(200, {"status": "ok", "replay": shop.replay})
            elif path == "/api/state":
                self._json(200, shop.state())
            else:
                self._json(404, {"detail": "no existe"})

        def do_POST(self):
            path = self._route()
            if path is None:
                return
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            parts = path.strip("/").split("/")
            try:
                if parts == ["api", "orders"]:
                    self._json(201, shop.open_order(int(body["model_id"])))
                elif parts[:2] == ["api", "orders"] and parts[3:] == ["inspections"]:
                    self._json(201, shop.submit_inspection(int(parts[2]), body.get("kind", "quick"), body["items"]))
                elif parts[:2] == ["api", "orders"] and parts[3:] == ["stage"]:
                    shop.change_stage(int(parts[2]), body["to_stage"])
                    self._json(200, {"ok": True})
                elif parts[:2] == ["api", "orders"] and parts[3:] == ["delete"]:
                    shop.delete_order(int(parts[2]))
                    self._json(200, {"ok": True})
                elif parts[:2] == ["api", "inspections"] and len(parts) == 4:
                    self._json(200, shop.close_inspection(int(parts[2]), parts[3], body.get("reason")))
                elif parts == ["api", "purchases"]:
                    self._json(201, shop.receive_purchase(body))
                else:
                    self._json(404, {"detail": "no existe"})
            except (KeyError, ValueError) as exc:
                self._json(400, {"detail": str(exc)})

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--broker", default=os.environ.get("BROKER", "localhost:19092"))
    parser.add_argument("--transport", choices=("kafka", "sqs"),
                        default=os.environ.get("EVENT_TRANSPORT", "kafka"))
    parser.add_argument("--events", default=os.environ.get("EVENTS", str(HERE.parent / "data" / "events.jsonl")))
    parser.add_argument("--speed", type=float, default=float(os.environ.get("SPEED", "3600")))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8090")))
    parser.add_argument("--reset", action="store_true", default=os.environ.get("RESET") == "1",
                        help="borra y recrea los tópicos antes de empezar")
    parser.add_argument("--no-replay", action="store_true", default=os.environ.get("NO_REPLAY") == "1",
                        help="solo el panel (sin reproducir el dataset)")
    parser.add_argument("--force-replay", action="store_true", default=os.environ.get("FORCE_REPLAY") == "1",
                        help="vuelve a mandar el dataset completo aunque ya se haya cargado")
    parser.add_argument("--state-param", default=os.environ.get("SIM_STATE_PARAM", "/hack-saltillo/simulator/state"),
                        help="parámetro SSM con el avance del dataset (solo SQS de AWS)")
    parser.add_argument("--api", default=os.environ.get("API_URL"),
                        help="API de inventario, para retomar un dataset cargado a medias")
    args = parser.parse_args()

    aws_sqs = args.transport == "sqs" and not os.environ.get("SQS_ENDPOINT_URL")
    if args.transport == "kafka":
        ensure_topics(args.broker, args.reset)
        publisher = KafkaPublisher(args.broker)
    else:
        if args.reset:
            raise SystemExit("--reset solo está disponible con el transporte Kafka")
        publisher = SqsPublisher()
        print("colas SQS:", ", ".join(f"{t} → {u.rsplit('/', 1)[-1]}" for t, u in publisher.queue_urls.items()),
              flush=True)
    state = SimState(args.state_param if aws_sqs else None)
    shop = Shop(Path(args.events), publisher, state)

    def replay() -> None:
        # Corre aparte para que el panel abra de inmediato (consultar la API puede tardar).
        start = 0
        if state.persistent and not args.force_replay:
            saved = state.data
            if saved.get("dataset") == shop.dataset_id and saved.get("done"):
                shop.replay.update(done=True, sent=len(shop.records))
                print(f"dataset ya cargado ({saved.get('updated_at')}); solo queda el panel. "
                      "Usa --force-replay para mandarlo otra vez.", flush=True)
                return
            if saved.get("dataset") == shop.dataset_id:
                start = int(saved.get("next", 0))
                print(f"retomando el dataset donde se quedó: registro {start + 1}", flush=True)
            else:
                start = first_unprocessed(shop.records, args.api)
                print(f"primera conexión: la API ya tiene los primeros {start} registros del dataset", flush=True)
        if start >= len(shop.records):
            shop.replay_cursor = start
            shop.save_progress(done=True)
            shop.replay.update(done=True, sent=start)
            return
        shop.run_replay(args.speed, start)

    if args.no_replay:
        shop.replay["done"] = True
    else:
        threading.Thread(target=replay, daemon=True).start()

    # Ctrl+C / SIGTERM: manda lo que quedó en el lote y guarda el avance antes de salir.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    server = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(shop))
    print(f"panel del simulador en http://localhost:{args.port}", flush=True)
    try:
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        if not shop.replay["done"]:
            shop.save_progress()
            print(f"\nsimulador detenido en el registro {shop.replay_cursor} de {len(shop.records)}; "
                  "la próxima vez retoma desde ahí", flush=True)


if __name__ == "__main__":
    main()
