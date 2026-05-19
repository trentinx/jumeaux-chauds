"""Orchestrateur de cluster de machines.

Ce module fournit `ClusterSimulator`, responsable de :
- instancier les `MachineSimulator` à partir de la config mergée,
- orchestrer la boucle de simulation (ticks),
- calculer les métriques agrégées (énergie, coût, PUE effectif),
- exposer un snapshot consolidé pour MQTT / WebSocket / API.

Phase 3 : la méthode ``run()`` accepte un ``MqttPublisher`` optionnel et
un ``ConnectionManager`` WebSocket optionnel (branché en Phase 4).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from config.loader import get_machine_config
from .machine import MachineSimulator, SensorConfig, ThermalConfig
from .physics import compute_cost
from .scenarios import FaultConfig, FaultScheduler, LoadProfileConfig, ScenarioEngine

if TYPE_CHECKING:
    from mqtt.publisher import MqttPublisher

logger = logging.getLogger(__name__)


@dataclass
class ClusterMetrics:
    """Métriques agrégées du cluster."""

    energy_kwh_total: float
    cost_eur_total: float
    pue_effective: float


class ClusterSimulator:
    """Orchestrateur de N machines simulées."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._cfg = config
        self.cluster_id: str = config["cluster"]["id"]
        self._tick_rate_hz: float = float(config["simulation"]["tick_rate_hz"])
        self._events_per_sec: float = float(
            config["simulation"].get("events_per_sec", 1.0)
        )

        # PUE et coût
        self._pue: float = float(config["cluster"].get("pue", 1.4))
        self._price_eur_kwh: float = float(
            config["cluster"].get("electricity_price_eur_kwh", 0.2)
        )

        # Construction des machines
        self.machines: dict[str, MachineSimulator] = {}
        self._build_machines()

        # Scénario de charge (global pour le cluster)
        lp_cfg = LoadProfileConfig(
            type=config["simulation"]["load_profile"]["type"],
            params={
                k: v
                for k, v in config["simulation"]["load_profile"].items()
                if k != "type"
            },
        )
        self._scenario_engine = ScenarioEngine(profile_cfg=lp_cfg)

        # Scheduler de pannes
        fault_cfgs: list[FaultConfig] = []
        fault_section = config["simulation"].get("fault_injection", {})
        for raw in fault_section.get("faults", []):
            fault_cfgs.append(
                FaultConfig(
                    type=raw["type"],
                    distribution=raw["distribution"],
                    shape=raw.get("shape"),
                    scale_s=raw.get("scale_s"),
                    probability_per_tick=raw.get("probability_per_tick"),
                    magnitude=raw.get("magnitude", 1.0),
                )
            )

        self._fault_scheduler = FaultScheduler(
            fault_configs=fault_cfgs,
            recovery_delay_s=float(fault_section.get("recovery_delay_s", 60.0)),
        )

        # Temps et métriques agrégées
        self._running = False
        self._t_elapsed_s: float = 0.0
        self.energy_kwh_total: float = 0.0
        self.cost_eur_total: float = 0.0
        self.pue_effective: float = self._pue

        # Mémorisation des états précédents (détection de changements)
        self._prev_status: dict[str, str] = {}
        self._prev_fans: dict[str, list[dict]] = {}

    # ------------------------------------------------------------------
    # Construction des machines
    # ------------------------------------------------------------------
    def _build_machines(self) -> None:
        """Instancie les MachineSimulator à partir des role_profiles.

        Utilise get_machine_config() pour merger le profil de rôle avec
        les surcharges individuelles de chaque machine, conformément à la
        structure de base.yaml (cluster.role_profiles.{role}.thermal / .fans).

        L'état initial de chaque machine est déterminé par la clé
        ``initial_status`` (priorité : machine > role_profile, défaut : "off").
        """
        cluster_cfg = self._cfg["cluster"]
        tick_rate_hz = float(self._cfg["simulation"]["tick_rate_hz"])

        for m_entry in cluster_cfg["machines"]:
            machine_id = m_entry["id"]
            role = m_entry.get("role", "worker")

            # Merge role_profile + surcharges individuelles
            m_cfg = get_machine_config(self._cfg, machine_id)

            th = m_cfg["thermal"]
            fans = m_cfg["fans"]
            power = m_cfg["power"]

            thermal_cfg = ThermalConfig(
                idle_w=float(power["idle_watts"]),
                max_w=float(power["max_watts"]),
                alpha=float(th.get("alpha_load_exponent", 1.5)),
                heat_ratio=float(power.get("heat_ratio", 0.9)),
                tau_max_s=float(th["tau_max_s"]),
                k_cool=float(th["k_cool_rpm_factor"]),
                c_th_j_per_c=float(th["thermal_capacity_j_per_c"]),
                ambient_temp_c=float(th["ambient_temp_c"]),
                t_shutdown_c=float(th["t_shutdown_c"]),
                t_restart_c=float(th["t_restart_c"]),
                recovery_delay_s=float(th.get("recovery_delay_s", 60.0)),
                fan_gain_rpm_per_c=float(fans["auto_policy"]["gain_rpm_per_c"]),
                fan_max_rpm=int(fans["max_rpm"]),
                fan_power_w=float(fans["power_per_fan_w"]),
                tick_rate_hz=tick_rate_hz,
            )

            sensor_configs: list[SensorConfig] = []
            for s_cfg in m_cfg.get("temperature_sensors", []):
                sensor_configs.append(
                    SensorConfig(
                        sensor_id=s_cfg["id"],
                        bias_c=float(s_cfg.get("bias_c", 0.0)),
                        noise_std_c=float(s_cfg.get("noise_std_c", 0.0)),
                        drift_rate_c_per_s=float(s_cfg.get("drift_rate_c_per_s", 0.0)),
                    )
                )

            fan_count = int(fans.get("count", 2))

            machine = MachineSimulator(
                machine_id=machine_id,
                role=role,
                thermal=thermal_cfg,
                sensor_configs=sensor_configs,
                fan_count=fan_count,
            )

            # ── État initial ──────────────────────────────────────────────────
            # Priorité : surcharge individuelle > role_profile > défaut "off"
            role_profile = cluster_cfg["role_profiles"].get(role, {})
            initial_status = (
                m_entry.get("initial_status")
                or role_profile.get("initial_status", "off")
            )
            if initial_status == "on":
                machine.power_on()
                logger.debug("Machine %s démarrée (initial_status=on)", machine_id)
            # ─────────────────────────────────────────────────────────────────

            self.machines[machine_id] = machine

    # ------------------------------------------------------------------
    # Boucle principale
    # ------------------------------------------------------------------
    async def run(
        self,
        publisher: "MqttPublisher | None" = None,
        ws_manager: Any = None,
    ) -> None:  # pragma: no cover - testé via intégration
        """Boucle de simulation principale.

        Parameters
        ----------
        publisher :
            Instance de :class:`mqtt.publisher.MqttPublisher` injecte dans
            la boucle pour la publication MQTT. Si ``None`` (flag --no-mqtt),
            la publication est silencieusement ignorée.
        ws_manager :
            ``ConnectionManager`` FastAPI (branché en Phase 4). Ignoré si
            ``None``.
        """
        self._running = True
        dt = 1.0 / self._tick_rate_hz

        # Timers pour les publications périodiques
        ticks_per_event = max(1, round(self._tick_rate_hz / self._events_per_sec))
        ticks_per_summary = max(1, round(self._tick_rate_hz * 5))
        ticks_per_energy = max(1, round(self._tick_rate_hz * 60))
        tick_counter: int = 0

        while self._running:
            await asyncio.sleep(dt)
            self._t_elapsed_s += dt
            tick_counter += 1

            # Charge globale fournie par le scénario
            load_factor = self._scenario_engine.get_load_factor(self._t_elapsed_s)

            # Tick de chaque machine
            for machine in self.machines.values():
                machine.tick(load_factor=load_factor, dt=dt)

            # Planification de pannes
            self._fault_scheduler.tick(self.machines, dt=dt)

            # Mise à jour des métriques agrégées
            self._update_metrics()

            # ---- Publications MQTT --------------------------------
            if publisher is not None:
                await self._publish_tick(
                    publisher,
                    tick_counter,
                    ticks_per_event,
                    ticks_per_summary,
                    ticks_per_energy,
                )

            # ---- Broadcast WebSocket (Phase 4) --------------------
            if ws_manager is not None and tick_counter % ticks_per_event == 0:
                try:
                    await ws_manager.broadcast(self.get_snapshot())
                except Exception as exc:  # noqa: BLE001
                    logger.debug("WS broadcast échec : %s", exc)

    async def _publish_tick(
        self,
        publisher: "MqttPublisher",
        tick_counter: int,
        ticks_per_event: int,
        ticks_per_summary: int,
        ticks_per_energy: int,
    ) -> None:
        """Gère toutes les publications MQTT pour le tick courant."""
        # --- Télémétrie par machine (fréquence events_per_sec) -----------
        if tick_counter % ticks_per_event == 0:
            for machine in self.machines.values():
                snap = machine.snapshot()
                snap["cluster_id"] = self.cluster_id
                snap["machine_id"] = machine.id
                snap["temperatures"] = {
                    s["sensor_id"]: {"value_c": s["temp_c"]}
                    for s in snap.get("sensors", [])
                }
                snap["power_w"] = snap.get("power_w", 0.0)


                await publisher.publish_telemetry(snap)

                # Changement de statut ?
                mid = machine.id
                current_status = snap.get("status", "")
                if self._prev_status.get(mid) != current_status:
                    self._prev_status[mid] = current_status
                    await publisher.publish_status(
                        self.cluster_id, mid, current_status
                    )

                # Changement d'état des fans ?
                current_fans = snap.get("fans", [])
                if self._prev_fans.get(mid) != current_fans:
                    self._prev_fans[mid] = current_fans
                    await publisher.publish_fan_state(
                        self.cluster_id, mid, current_fans
                    )

                # Panne active ?
                for fault in snap.get("active_faults", []):
                    fault_key = f"{mid}:{fault.get('type')}:{fault.get('ts_start')}"
                    if not hasattr(self, "_published_faults"):
                        self._published_faults: set[str] = set()
                    if fault_key not in self._published_faults:
                        self._published_faults.add(fault_key)
                        await publisher.publish_fault(
                            self.cluster_id, mid, fault, event="injected"
                        )

        # --- Summary cluster (toutes les 5 s) ----------------------
        if tick_counter % ticks_per_summary == 0:
            await publisher.publish_summary(self.get_snapshot())

        # --- Métriques énergétiques (toutes les 60 s) ---------------
        if tick_counter % ticks_per_energy == 0:
            await publisher.publish_energy(
                self.cluster_id,
                {
                    "energy_kwh_total": round(self.energy_kwh_total, 6),
                    "cost_eur_total": round(self.cost_eur_total, 4),
                    "pue_effective": self.pue_effective,
                },
            )

    def stop(self) -> None:
        """Demande l'arrêt de la boucle de simulation."""

        self._running = False

    # ------------------------------------------------------------------
    # Métriques & snapshot
    # ------------------------------------------------------------------
    def _update_metrics(self) -> None:
        self.energy_kwh_total = sum(
            m.energy_kwh_cumulated for m in self.machines.values()
        )
        self.cost_eur_total = compute_cost(
            energy_kwh=self.energy_kwh_total,
            pue=self._pue,
            price_eur_kwh=self._price_eur_kwh,
        )
        self.pue_effective = self._pue

    def get_snapshot(self) -> dict:
        """Retourne un snapshot consolidé du cluster."""

        return {
            "cluster_id": self.cluster_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "metrics": {
                "energy_kwh_total": self.energy_kwh_total,
                "cost_eur_total": self.cost_eur_total,
                "pue_effective": self.pue_effective,
            },
            "machines": {mid: m.snapshot() for mid, m in self.machines.items()},
        }
