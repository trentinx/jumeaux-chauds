"""Point d'entrée FastAPI avec pattern lifespan.

Le lifespan :
  1. Charge la config OmegaConf (base + scénario via ENV SCENARIO).
  2. Instancie MqttPublisher (si MQTT_ENABLED != '0').
  3. Instancie ConnectionManager WebSocket.
  4. Instancie ClusterSimulator.
  5. Lance ClusterSimulator.run() en background task asyncio.
  6. À l'arrêt, stoppe proprement la boucle de simulation.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.loader import load_config
from simulation.cluster import ClusterSimulator
from api.ws import ConnectionManager
from api import deps

logger = logging.getLogger(__name__)

APP_VERSION = "0.4.0"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Démarre et arrête les ressources de l'application."""

    # --- Configuration ---------------------------------------------------
    scenario = os.environ.get("SCENARIO", "nominal")
    cfg = load_config(scenario=scenario)
    logger.info("Config chargée — scénario : %s, cluster : %s", scenario, cfg["cluster"]["id"])

    # --- WebSocket manager -----------------------------------------------
    ws_manager = ConnectionManager()
    deps._ws_manager = ws_manager

    # --- Publisher MQTT (optionnel) --------------------------------------
    mqtt_enabled = os.environ.get("MQTT_ENABLED", "1") != "0"
    publisher = None
    if mqtt_enabled:
        try:
            from mqtt.publisher import MqttPublisher
            host = os.environ.get("MQTT_BROKER_HOST", "localhost")
            port = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
            publisher = MqttPublisher(dict(broker_host=host, broker_port=port))
            await publisher.__aenter__()
            logger.info("MqttPublisher connecté sur %s:%s", host, port)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MQTT indisponible, simulation sans publication : %s", exc)
            publisher = None

    # --- Simulateur -------------------------------------------------------
    simulator = ClusterSimulator(config=cfg)
    deps._simulator = simulator
    deps._config = cfg

    # --- Lancement de la boucle en background ----------------------------
    sim_task = asyncio.create_task(
        simulator.run(publisher=publisher, ws_manager=ws_manager)
    )
    logger.info("ClusterSimulator démarré (cluster_id=%s)", simulator.cluster_id)

    yield  # L'API est opérationnelle

    # --- Arrêt propre ----------------------------------------------------
    simulator.stop()
    sim_task.cancel()
    try:
        await asyncio.wait_for(sim_task, timeout=3.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    if publisher is not None:
        try:
            await publisher.__aexit__(None, None, None)
        except Exception:  # noqa: BLE001
            pass

    logger.info("API arrêtée proprement.")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Jumeaux Chauds — API",
    version=APP_VERSION,
    description="API de contrôle et d'observation du simulateur de cluster thermique.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8501",  # Dashboard Streamlit
        "http://127.0.0.1:8501",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Import des routers (après création de `app`) --------------------------
from api.routes import machines, cluster, simulation  # noqa: E402
from api.ws import router as ws_router  # noqa: E402

app.include_router(machines.router, prefix="/machines", tags=["machines"])
app.include_router(cluster.router, prefix="/cluster", tags=["cluster"])
app.include_router(simulation.router, prefix="/simulation", tags=["simulation"])
app.include_router(ws_router, tags=["websocket"])


# ---------------------------------------------------------------------------
# Route racine
# ---------------------------------------------------------------------------
@app.get("/", tags=["info"])
async def root() -> dict:
    """Informations générales sur l'API et l'état du simulateur."""
    simulator: ClusterSimulator = deps.get_cluster()
    cfg = deps.get_config()
    scenario = cfg.get("simulation", {}).get("load_profile", {}).get("type", "unknown")
    return {
        "name": "Jumeaux Chauds API",
        "version": APP_VERSION,
        "cluster_id": simulator.cluster_id,
        "scenario_active": scenario,
        "machines_count": len(simulator.machines),
        "running": simulator._running,
    }
