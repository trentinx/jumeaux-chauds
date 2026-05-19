"""Consumer MQTT → TimescaleDB.

S'abonne aux topics ``dt/#`` et ``dt/+/+/fault`` du broker Mosquitto
et insère les données dans les tables ``telemetry`` et ``events``
de TimescaleDB.

Démarrage ::

    python -m consumer.mqtt_to_timescale

Variables d'environnement
-------------------------
MQTT_BROKER_HOST, MQTT_BROKER_PORT
POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone

import aiomqtt
import asyncpg

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ── Configuration via variables d'environnement ────────────────────────
MQTT_HOST = os.environ.get("MQTT_BROKER_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_BROKER_PORT", 1883))

PG_DSN = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'jumeaux')}"
    f":{os.environ.get('POSTGRES_PASSWORD', 'jumeaux')}"
    f"@{os.environ.get('POSTGRES_HOST', 'localhost')}"
    f":{os.environ.get('POSTGRES_PORT', 5432)}"
    f"/{os.environ.get('POSTGRES_DB', 'jumeaux')}"
)

# Regex pour extraire cluster_id et machine_id depuis le topic MQTT
# ex: dt/cluster_alpha/srv-master-01/telemetry
_TOPIC_RE = re.compile(r"^dt/(?P<cluster>[^/]+)/(?P<machine>[^/]+)/(?P<kind>.+)$")


class MqttConsumer:
    """Lit les messages MQTT et les persiste dans TimescaleDB."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._insert_count = 0

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Dispatche le message selon son topic."""
        m = _TOPIC_RE.match(topic)
        if not m:
            return

        cluster_id = m.group("cluster")
        machine_id = m.group("machine")
        kind = m.group("kind")

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug("Payload non-JSON ignoré : %s", topic)
            return

        if kind == "telemetry":
            await self._insert_telemetry(cluster_id, machine_id, data)
        elif kind == "fault":
            await self._insert_event(cluster_id, machine_id, "fault", data)
        elif kind == "status":
            await self._insert_event(cluster_id, machine_id, "status_change", data)

    def _convert_ts(self, ts_str: str) -> datetime:
        ts = ts_str
        if ts:
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            ts = datetime.fromisoformat(ts)
        else:
            ts = datetime.now(timezone.utc).isoformat()
        return ts
    
    async def _insert_telemetry(
        self,
        cluster_id: str,
        machine_id: str,
        data: dict,
    ) -> None:
        ts = self._convert_ts(data.get("ts"))

        # Température principale : premier capteur trouvé
        sensors = data.get("sensors", [])
        temp_c = sensors[0].get("temp_c") if sensors else data.get("temperature_c")

        # Vitesse moyenne des fans
        fans = data.get("fans", [])
        fan_rpm_avg = (
            sum(f.get("rpm", 0) for f in fans) / len(fans) if fans else None
        )

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO telemetry
                    (ts, cluster_id, machine_id, status,
                     temperature_c, power_w, energy_kwh,
                     load_factor, fan_rpm_avg)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                ts,
                cluster_id,
                machine_id,
                data.get("status"),
                temp_c,
                data.get("power_w"),
                data.get("energy_kwh"),
                data.get("load_factor"),
                fan_rpm_avg,
            )

        self._insert_count += 1
        if self._insert_count % 500 == 0:
            logger.info("%.0f insertions telemetry", self._insert_count)

    async def _insert_event(
        self,
        cluster_id: str,
        machine_id: str,
        event_type: str,
        data: dict,
    ) -> None:
        ts = self._convert_ts(data.get("ts"))
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO events (ts, cluster_id, machine_id, event_type, payload)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                """,
                ts,
                cluster_id,
                machine_id,
                event_type,
                json.dumps(data),
            )


async def _wait_for_postgres(dsn: str, retries: int = 20, delay: float = 3.0) -> asyncpg.Pool:
    """Attend que PostgreSQL soit prêt avant de créer le pool."""
    for attempt in range(1, retries + 1):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
            logger.info("Connecté à TimescaleDB")
            return pool
        except Exception as exc:
            logger.warning("Postgres non prêt (%d/%d) : %s", attempt, retries, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("Impossible de se connecter à TimescaleDB")


async def main() -> None:
    """Point d'entrée principal du consumer."""
    pool = await _wait_for_postgres(PG_DSN)
    consumer = MqttConsumer(pool)

    logger.info("Connexion au broker MQTT %s:%s", MQTT_HOST, MQTT_PORT)

    while True:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_HOST,
                port=MQTT_PORT,
                identifier="consumer-timescale",
            ) as client:
                await client.subscribe("dt/#", qos=0)
                logger.info("Abonné à dt/# — en attente de messages…")
                async for message in client.messages:
                    await consumer.handle_message(
                        str(message.topic),
                        message.payload,
                    )
        except aiomqtt.MqttError as exc:
            logger.warning("Déconnexion MQTT : %s — reconnexion dans 5 s", exc)
            await asyncio.sleep(5)
        except Exception as exc:  # noqa: BLE001
            logger.error("Erreur inattendue : %s", exc)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
