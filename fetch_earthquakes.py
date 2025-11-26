import requests
import pandas as pd
from datetime import datetime, timezone

# USGS feed: all earthquakes in the past 30 days
USGS_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
)


def fetch_earthquakes(url: str = USGS_URL) -> dict:
    """
    Call USGS Earthquake API and return the GeoJSON payload.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()  # will raise an error if status != 200
    return resp.json()


def flatten_earthquakes(geojson: dict) -> pd.DataFrame:
    """
    Flatten the GeoJSON into a tabular structure suitable for CSV / Snowflake.
    """
    features = geojson.get("features", [])
    rows = []

    for f in features:
        props = f.get("properties", {}) or {}
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None, None]

        # coordinates = [lon, lat, depth_km]
        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None
        depth = coords[2] if len(coords) > 2 else None

        # time is in milliseconds since epoch (UTC)
        time_ms = props.get("time")
        if time_ms is not None:
            dt_utc = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc)
            time_iso = dt_utc.isoformat()
        else:
            time_iso = None

        rows.append(
            {
                "event_id": f.get("id"),
                "time_utc": time_iso,
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
            }
        )

    return pd.DataFrame(rows)


def main(output_csv_path: str = "earthquakes_all_month.csv") -> None:
    print("📡 Fetching earthquake data from USGS...")
    geojson = fetch_earthquakes()
    print(f"✅ Retrieved {len(geojson.get('features', []))} events")

    print("🧹 Flattening GeoJSON into a table...")
    df = flatten_earthquakes(geojson)
    print(df.head())

    print(f"💾 Writing CSV to {output_csv_path} ...")
    df.to_csv(output_csv_path, index=False)
    print(f"🎉 Done! Saved {len(df)} rows.")


if __name__ == "__main__":
    main()
