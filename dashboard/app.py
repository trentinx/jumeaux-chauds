"""
Jumeaux Chauds — Dashboard Streamlit (Phase 5)
===============================================
Lancer :
    python -m streamlit run dashboard/app.py

Pré-requis :
    L'API FastAPI doit tourner sur http://localhost:8000
    (MQTT_ENABLED=0 uvicorn api.main:app --reload)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# sys.path : garantit que la racine du projet est dans le path Python,
# quel que soit le répertoire depuis lequel Streamlit est lancé.
# ---------------------------------------------------------------------------
import sys
import os

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Imports standard
# ---------------------------------------------------------------------------
import collections
import time
from typing import Any

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.api_client import ApiClient
from dashboard.ws_client import ClusterWSClient

# ---------------------------------------------------------------------------
# Variables d'environnement (ex. pour configurer l'URL de l'API ou du WS)
# ---------------------------------------------------------------------------
API_BASE_URL = os.getenv("API_BASE_URL","http://localhost:8000")
WS_URL = os.getenv("WS_URL","ws://localhost:8000/ws/cluster")


# ---------------------------------------------------------------------------
# Configuration page
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Jumeaux Chauds — Digital Twin",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Ressources partagées (une instance par session Streamlit)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_ws_client() -> ClusterWSClient:
    return ClusterWSClient(url=WS_URL)


@st.cache_resource
def get_api_client() -> ApiClient:
    return ApiClient(base_url=API_BASE_URL)


# Buffer circulaire par machine pour les courbes de température
if "temp_buffers" not in st.session_state:
    st.session_state.temp_buffers: dict[str, collections.deque] = {}

if "event_log" not in st.session_state:
    st.session_state.event_log: list[str] = []

if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()


def log_event(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    st.session_state.event_log.insert(0, f"[{ts}] {msg}")
    st.session_state.event_log = st.session_state.event_log[:20]


# ---------------------------------------------------------------------------
# Auto-refresh (remplace @st.fragment pour Streamlit < 1.37)
# ---------------------------------------------------------------------------

REFRESH_INTERVAL_S = 2

def _auto_refresh() -> None:
    """Planifie un st.rerun() si l'intervalle est écoulé."""
    now = time.time()
    if now - st.session_state.last_refresh >= REFRESH_INTERVAL_S:
        st.session_state.last_refresh = now
        time.sleep(0.05)
        st.rerun()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STATUS_COLOR = {"on": "🟢", "off": "⚫", "degraded": "🔴"}


def _safe_get(d: dict, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
        if d is None:
            return default
    return d


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar(snapshot: dict[str, Any] | None) -> None:
    with st.sidebar:
        st.title("🌡️ Jumeaux Chauds")
        st.caption("Digital Twin — La Plateforme, Marseille 2026")
        st.divider()
        if snapshot:
            m = snapshot.get("metrics", {})
            machines = snapshot.get("machines", {})
            nb_on = sum(1 for m_ in machines.values() if m_.get("status") == "on")
            st.metric("💻 Machines actives", f"{nb_on} / {len(machines)}")
            st.metric("⚡ Énergie cumulée", f"{m.get('energy_kwh_total', 0):.3f} kWh")
            st.metric("💶 Coût cumulé", f"{m.get('cost_eur_total', 0):.4f} €")
            st.metric("🌡️ PUE", f"{m.get('pue_effective', 1.0):.2f}")
            st.caption(f"Cluster : `{snapshot.get('cluster_id', '?')}`")
            st.caption(f"Tick : `{snapshot.get('ts', '?')}`")
        else:
            st.warning("⏳ En attente du simulateur…")
        st.divider()
        st.caption("API : http://localhost:8000")
        st.caption(f"Refresh : {REFRESH_INTERVAL_S}s")


# ---------------------------------------------------------------------------
# Onglet 1 — Vue Cluster
# ---------------------------------------------------------------------------

def tab_cluster(snapshot: dict[str, Any] | None) -> None:
    if not snapshot:
        st.info("⏳ Connexion au simulateur en cours…")
        return

    machines = snapshot.get("machines", {})
    if not machines:
        st.warning("Aucune machine dans le snapshot.")
        return

    m = snapshot.get("metrics", {})
    nb_on = sum(1 for v in machines.values() if v.get("status") == "on")
    t_max = max((v.get("temperature_c", 0) for v in machines.values()), default=0)
    w_total = m.get("energy_kwh_total", 0)
    cost_h = m.get("cost_eur_total", 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💻 Machines ON", f"{nb_on} / {len(machines)}")
    c2.metric("🌡️ Tₚₑₐₖ", f"{t_max:.1f} °C",
              delta=f"+{t_max - 22:.1f} °C vs amb.",
              delta_color="inverse")
    c3.metric("⚡ Énergie cumulée", f"{w_total:.3f} kWh")
    c4.metric("💶 Coût cumulé", f"{cost_h:.4f} €")

    st.divider()

    ids = list(machines.keys())
    temps = [machines[i].get("temperature_c", 0) for i in ids]
    statuses = [machines[i].get("status", "?") for i in ids]
    labels = [
        f"{mid}<br>{STATUS_COLOR.get(s, '?')} {t:.1f}°C"
        for mid, s, t in zip(ids, statuses, temps)
    ]

    fig = go.Figure(go.Heatmap(
        z=[temps],
        x=ids,
        y=["🌡️ Temp (°C)"],
        text=[labels],
        texttemplate="%{text}",
        colorscale="RdYlGn_r",
        zmin=20,
        zmax=90,
        showscale=True,
        colorbar=dict(title="°C"),
    ))
    fig.update_layout(
        title="Carte thermique du cluster",
        height=180,
        margin=dict(l=10, r=10, t=40, b=10),
        xaxis=dict(tickangle=-30),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📊 État des machines")
    rows = []
    for mid, mv in machines.items():
        faults = mv.get("faults", [])
        rows.append({
            "Machine": mid,
            "Rôle": mv.get("role", ""),
            "Statut": f"{STATUS_COLOR.get(mv.get('status'), '?')} {mv.get('status', '?')}",
            "Temp (°C)": f"{mv.get('temperature_c', 0):.1f}",
            "Énergie (kWh)": f"{mv.get('energy_kwh_cumulated', 0):.3f}",
            "Pannes": len(faults),
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Onglet 2 — Vue Machine
# ---------------------------------------------------------------------------

def tab_machine(snapshot: dict[str, Any] | None, api: ApiClient) -> None:
    if not snapshot:
        st.info("⏳ En attente du snapshot…")
        return

    machines = snapshot.get("machines", {})
    machine_ids = list(machines.keys())
    if not machine_ids:
        st.warning("Aucune machine disponible.")
        return

    selected = st.selectbox("🔍 Choisir une machine", machine_ids, key="sel_machine")
    mv = machines.get(selected, {})
    if not mv:
        st.error(f"Machine `{selected}` introuvable dans le snapshot.")
        return

    status = mv.get("status", "off")
    temp = mv.get("temperature_c", 0)
    faults = mv.get("faults", [])
    fans = mv.get("fans", [])
    sensors = mv.get("sensors", [])

    color = "red" if status == "degraded" else ("green" if status == "on" else "gray")
    st.markdown(
        f"<div style='padding:8px 16px; background:{color}20; "
        f"border-left:4px solid {color}; border-radius:4px; margin-bottom:12px;'>"
        f"<b>{STATUS_COLOR.get(status, '?')} {selected}</b> — statut : <b>{status.upper()}</b> — "
        f"rôle : {mv.get('role','?')}"
        + (f" — ⚠️ {len(faults)} panne(s)" if faults else "")
        + "</div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("🌡️ Temp CPU", f"{temp:.1f} °C")
    c2.metric("⚡ Énergie", f"{mv.get('energy_kwh_cumulated', 0):.3f} kWh")
    c3.metric("🌀 Fans", f"{len(fans)} ventilateur(s)")

    if selected not in st.session_state.temp_buffers:
        st.session_state.temp_buffers[selected] = collections.deque(maxlen=100)
    st.session_state.temp_buffers[selected].append(temp)

    buf = list(st.session_state.temp_buffers[selected])
    if len(buf) > 1:
        st.line_chart({"temp_c": buf}, height=180, use_container_width=True)

    if sensors:
        st.subheader("🌡️ Sondes thermiques")
        st.dataframe(
            [{"Sonde": s["sensor_id"], "Temp (°C)": f"{s['temp_c']:.1f}"} for s in sensors],
            use_container_width=True, hide_index=True,
        )

    if fans:
        st.subheader("🌀 Ventilateurs")
        st.dataframe(
            [{"Idx": f["idx"], "RPM": f["rpm"], "Mode": f["mode"]} for f in fans],
            use_container_width=True, hide_index=True,
        )

    if faults:
        st.subheader("⚠️ Pannes actives")
        st.dataframe(
            [{"Type": f["type"], "Restant (s)": f"{f['remaining_s']:.1f}",
              "Magnitude": f"{f['magnitude']:.2f}"} for f in faults],
            use_container_width=True, hide_index=True,
        )

    st.divider()
    st.subheader("🛠️ Commandes")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**🔌 Alimentation**")
        if st.button("▶️ Power ON", key="btn_on", disabled=(status == "on")):
            res = api.power_machine(selected, "on")
            if "error" in res:
                st.error(f"❌ {res['error']} (code {res.get('status_code')})")
            else:
                log_event(f"Power ON → {selected}")
                st.success("✅ Allumage demandé")
        if st.button("⏹️ Power OFF", key="btn_off", disabled=(status == "off")):
            res = api.power_machine(selected, "off")
            if "error" in res:
                st.error(f"❌ {res['error']}")
            else:
                log_event(f"Power OFF → {selected}")
                st.success("✅ Extinction demandée")

    with col2:
        st.markdown("**🌀 Ventilation manuelle**")
        if fans:
            fan_idx = st.number_input("Fan idx", min_value=0,
                                      max_value=max(len(fans) - 1, 0),
                                      step=1, key="fan_idx")
            rpm_val = st.slider("RPM cible", 0, 5000, 2000, step=100, key="fan_rpm")
            if st.button("✅ Appliquer RPM", key="btn_rpm"):
                api.set_fan_speed(selected, int(fan_idx), rpm_val)
                log_event(f"Fan {fan_idx} → {rpm_val} RPM sur {selected}")
                st.success(f"✅ Fan {fan_idx} réglé à {rpm_val} RPM")
        else:
            st.caption("Aucun fan détecté.")

    with col3:
        st.markdown("**⚙️ Mode fan**")
        if fans:
            fan_idx2 = st.number_input("Fan idx", min_value=0,
                                       max_value=max(len(fans) - 1, 0),
                                       step=1, key="fan_idx2")
            mode = st.radio("Mode", ["auto", "manual"], key="fan_mode")
            if st.button("✅ Appliquer mode", key="btn_mode"):
                api.set_fan_mode(selected, int(fan_idx2), mode)
                log_event(f"Fan {fan_idx2} mode={mode} sur {selected}")
                st.success(f"✅ Fan {fan_idx2} passé en mode {mode}")


# ---------------------------------------------------------------------------
# Onglet 3 — Simulation
# ---------------------------------------------------------------------------

def tab_simulation(snapshot: dict[str, Any] | None, api: ApiClient) -> None:
    machines = list(snapshot.get("machines", {}).keys()) if snapshot else []

    st.subheader("🎬 Scénario actif")
    col1, col2 = st.columns([2, 1])
    with col1:
        scenario = st.selectbox("Choisir un scénario", ["nominal", "stress"], key="sel_scenario")
    with col2:
        st.write("")
        if st.button("🔄 Changer de scénario", key="btn_scenario"):
            res = api.change_scenario(scenario)
            if "error" in res:
                st.error(f"❌ {res['error']}")
            else:
                log_event(f"Scénario changé → {scenario}")
                st.success(f"✅ Scénario **{scenario}** activé")

    st.divider()
    st.subheader("⚡ Injection de panne")

    if not machines:
        st.warning("⏳ En attente du simulateur…")
    else:
        c1, c2, c3, c4 = st.columns(4)
        target = c1.selectbox("Machine cible", machines, key="fault_machine")
        ftype = c2.selectbox(
            "Type de panne",
            ["fan_failure", "power_surge", "thermal_runaway", "network_glitch"],
            key="fault_type",
        )
        duration = c3.number_input("Durée (s)", min_value=5.0, max_value=300.0,
                                   value=30.0, step=5.0, key="fault_duration")
        magnitude = c4.slider("Magnitude", 0.1, 2.0, 1.0, step=0.1, key="fault_magnitude")

        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("⚡ Injecter la panne", key="btn_inject"):
                res = api.inject_fault(target, ftype, duration, magnitude)
                if "error" in res:
                    st.error(f"❌ {res['error']}")
                else:
                    log_event(f"Panne {ftype} injectée sur {target} ({duration}s, x{magnitude})")
                    st.success(f"✅ Panne **{ftype}** injectée sur `{target}`")
        with col_b:
            if st.button("🧹 Effacer les pannes", key="btn_clear"):
                api.clear_faults(target)
                log_event(f"Pannes effacées sur {target}")
                st.success(f"✅ Pannes annulées pour `{target}`")

    st.divider()
    st.subheader("📓 Journal des événements")
    if st.session_state.event_log:
        for entry in st.session_state.event_log:
            st.text(entry)
    else:
        st.caption("Aucun événement pour l'instant.")


# ---------------------------------------------------------------------------
# Onglet 4 — Énergie
# ---------------------------------------------------------------------------

def tab_energy(snapshot: dict[str, Any] | None) -> None:
    if not snapshot:
        st.info("⏳ En attente du snapshot…")
        return

    machines = snapshot.get("machines", {})
    m = snapshot.get("metrics", {})

    c1, c2, c3 = st.columns(3)
    kwh = m.get("energy_kwh_total", 0)
    cost = m.get("cost_eur_total", 0)
    pue = m.get("pue_effective", 1.0)
    c1.metric("⚡ Énergie cumulée", f"{kwh:.3f} kWh")
    c2.metric("💶 Coût cumulé", f"{cost:.4f} €")
    c3.metric("🌡️ PUE effectif", f"{pue:.2f}")

    projection_mois = cost * 24 * 30
    st.info(f"📅 Projection mensuelle (estimation) : **{projection_mois:.2f} €**")
    st.divider()

    if machines:
        ids = list(machines.keys())
        energies = [machines[i].get("energy_kwh_cumulated", 0) for i in ids]
        statuses = [machines[i].get("status", "off") for i in ids]

        fig = px.bar(
            x=ids, y=energies, color=statuses,
            color_discrete_map={"on": "#27ae60", "degraded": "#e74c3c", "off": "#95a5a6"},
            labels={"x": "Machine", "y": "Énergie (kWh)", "color": "Statut"},
            title="Énergie cumulée par machine",
        )
        fig.update_layout(height=350, margin=dict(l=10, r=10, t=50, b=30))
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("📊 Détail par machine")
    rows = []
    for mid, mv in machines.items():
        e = mv.get("energy_kwh_cumulated", 0)
        rows.append({
            "Machine": mid,
            "Statut": mv.get("status", "?"),
            "Énergie (kWh)": f"{e:.4f}",
            "Part (%)": f"{100 * e / kwh:.1f}" if kwh > 0 else "0.0",
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def main() -> None:
    ws = get_ws_client()
    api = get_api_client()
    snapshot = ws.get_snapshot()

    render_sidebar(snapshot)

    tab1, tab2, tab3, tab4 = st.tabs([
        "🌡️ Vue Cluster",
        "🖥️ Vue Machine",
        "🎬 Simulation",
        "⚡ Énergie",
    ])

    with tab1:
        tab_cluster(snapshot)
    with tab2:
        tab_machine(snapshot, api)
    with tab3:
        tab_simulation(snapshot, api)
    with tab4:
        tab_energy(snapshot)

    _auto_refresh()


if __name__ == "__main__":
    main()
