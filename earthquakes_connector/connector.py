# ============================================
# connector.py — Earthquakes Connector (MLDS 430)
# Custom connector: USGS Earthquakes → Snowflake_GECKO
# ============================================

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import requests
from datetime import datetime, timezone

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


# ------------------------------------------------------------
# UPDATE FUNCTION (required by old-style SDK)
# ------------------------------------------------------------
def update(configuration: dict, state: dict):
    """
    Fetch USGS earthquake events from the last 30 days
    and send them to Snowflake through Fivetran.
    """

    log.warning("🌎 Starting Earthquake Connector — Fetching USGS all_month feed")

    # --------------------------------------
    # STEP 1: Fetch data
    # --------------------------------------
    try:
        resp = requests.get(USGS_URL)
        resp.raise_for_status()
        geojson = resp.json()
    except Exception as e:
        log.error(f"❌ Error fetching USGS feed: {e}")
        return state

    features = geojson.get("features", [])
    log.warning(f"📦 Retrieved {len(features)} earthquake events")

    # --------------------------------------
    # STEP 2: Flatten events & upsert
    # --------------------------------------
    for f in features:
        props = f.get("properties", {}) or {}
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None, None]

        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None
        depth = coords[2] if len(coords) > 2 else None

        time_ms = props.get("time")
        time_utc = (
            datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc).isoformat()
            if time_ms else None
        )

        op.upsert(
            table="earthquakes",
            data={
                "event_id": f.get("id"),
                "time_utc": time_utc,
                "time_ms": time_ms,
                "latitude": lat,
                "longitude": lon,
                "depth_km": depth,
                "magnitude": props.get("mag"),
                "mag_type": props.get("magType"),
                "place": props.get("place"),
                "type": props.get("type"),
                "status": props.get("status"),
                "tsunami": props.get("tsunami"),
                "alert": props.get("alert"),
                "updated_ms": props.get("updated"),
                "detail_url": props.get("url"),
            },
        )

    # --------------------------------------
    # STEP 3: Save checkpoint
    # --------------------------------------
    op.checkpoint(state)
    log.warning("✅ Earthquake connector run complete — data sent to Fivetran.")

    return state


# ------------------------------------------------------------
# CONNECTOR OBJECT — IMPORTANT: old SDK only allows update=update
# ------------------------------------------------------------
connector = Connector(update=update)


# ------------------------------------------------------------
# LOCAL TESTING ENTRY POINT
# ------------------------------------------------------------
if __name__ == "__main__":
    connector.debug()
