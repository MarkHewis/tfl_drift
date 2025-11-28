import os, json, time, math
from datetime import datetime, timezone
import requests

TFL_KEY = os.environ.get("tfl_key_primary")
if not TFL_KEY:
    raise SystemExit("Missing env var tfl_key_primary")

LINE_ID = "430"
DIRECTION_FILTER = "inbound"  # change later to expand to outbound too

# Local files (direction-aware filenames)
STATE_PATH = f"{LINE_ID}-{DIRECTION_FILTER}-state.json"
GEOJSON_OUT = f"stops-{LINE_ID}-{DIRECTION_FILTER}.geojson"

# Tuning knobs
POLL_SECONDS = 60                 # poll interval
ARRIVAL_TTS_SEC = 30              # <= this means "arriving now"
PREDICTION_MAX_AGE_SEC = 25 * 60  # expire predictions after this
PHANTOM_GRACE_SEC = 6 * 60        # allow this much late before calling phantom

session = requests.Session()


def fetch_stop_point(stop_id: str):
    url = f"https://api.tfl.gov.uk/StopPoint/{stop_id}"
    r = session.get(url, params={"app_key": TFL_KEY}, timeout=10)
    r.raise_for_status()
    return r.json()


# get stop information if not already present - save api call as huge and only really need lat/long
def ensure_stop_metadata(state, arrivals):
    for a in arrivals:
        stop_id = a.get("naptanId")
        if not stop_id:
            continue

        st = state["stops"].get(stop_id)
        if st and st.get("lat") is not None and st.get("lon") is not None:
            continue

        # create stub if needed
        if not st:
            state["stops"][stop_id] = {
                "lat": None, "lon": None,
                "name": a.get("stationName") or a.get("platformName") or stop_id
            }

        # fetch real coords once
        try:
            sp = fetch_stop_point(stop_id)
            state["stops"][stop_id]["lat"] = sp.get("lat")
            state["stops"][stop_id]["lon"] = sp.get("lon")
            state["stops"][stop_id]["name"] = sp.get("commonName") or state["stops"][stop_id]["name"]
        except Exception:
            pass  # keep stub; try again next cycle


def now_ts():
    return int(time.time())


def load_state():
    empty = {
        "lastUpdated": None,
        "direction": DIRECTION_FILTER,
        "stops": {},          # stopId -> {lat, lon, name}
        "predictions": {},    # stopId -> list of predictions
        "stopStats": {}       # stopId -> rolling stats
    }

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            if not txt:
                return empty
            return json.loads(txt)
    except (FileNotFoundError, json.JSONDecodeError):
        return empty


def save_state(state):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


# root query - note attribute direction can be inbound or outbound
def fetch_arrivals():
    url = f"https://api.tfl.gov.uk/Line/{LINE_ID}/Arrivals"
    r = session.get(url, params={"app_key": TFL_KEY}, timeout=10)
    r.raise_for_status()
    return r.json()


def parse_iso(ts_str):
    return int(datetime.fromisoformat(ts_str.replace("Z","+00:00")).timestamp())


def filter_direction(arrivals, direction=DIRECTION_FILTER):
    # Some feeds use "direction", some "dir" — you said it's "direction" in your data
    out = []
    for a in arrivals:
        if (a.get("direction") or "").lower() == direction.lower():
            out.append(a)
    return out


def update_predictions(state, arrivals):
    ts = now_ts()
    preds_by_stop = state["predictions"]

    for a in arrivals:
        stop_id = a.get("naptanId")
        veh_id  = a.get("vehicleId")
        tts     = a.get("timeToStation")  # seconds
        exp_arr = a.get("expectedArrival")

        if not stop_id or not veh_id or tts is None or not exp_arr:
            continue

        rec = {
            "vehicleId": veh_id,
            "predictionTs": ts,
            "timeToStation": int(tts),
            "expectedArrivalTs": parse_iso(exp_arr),
            "matched": False
        }
        preds_by_stop.setdefault(stop_id, []).append(rec)

    # prune old predictions
    for stop_id, preds in list(preds_by_stop.items()):
        newp = []
        for p in preds:
            age = ts - p["predictionTs"]
            if age <= PREDICTION_MAX_AGE_SEC:
                newp.append(p)
        if newp:
            preds_by_stop[stop_id] = newp
        else:
            preds_by_stop.pop(stop_id, None)


def detect_arrivals_and_score(state, arrivals):
    """
    Time-only arrival detection.

    Rule:
      - if we see an inbound arrival for (stop, vehicle) with timeToStation <= ARRIVAL_TTS_SEC,
        we treat actual arrival as ts_now + timeToStation.
      - we match the earliest unmatched prediction for that (stop, vehicle).
    """
    ts_now = now_ts()
    preds_by_stop = state["predictions"]
    stats = state["stopStats"]

    # Index current arrivals by (stop, vehicle) -> tts
    curr = {}
    for a in arrivals:
        stop_id = a.get("naptanId")
        veh_id  = a.get("vehicleId")
        tts     = a.get("timeToStation")
        if stop_id and veh_id and tts is not None:
            curr[(stop_id, veh_id)] = int(tts)

    # For each current (stop,veh) that is "arriving", match a prediction
    for (stop_id, veh_id), tts in curr.items():
        if tts > ARRIVAL_TTS_SEC:
            continue

        preds = preds_by_stop.get(stop_id, [])
        # choose oldest unmatched prediction for this vehicle
        candidate = None
        for p in preds:
            if (not p.get("matched")) and p.get("vehicleId") == veh_id:
                candidate = p
                break

        if candidate is None:
            continue

        actual_arr_ts = ts_now + max(0, tts)
        predicted_arr_ts = candidate["predictionTs"] + candidate["timeToStation"]
        drift_sec = predicted_arr_ts - actual_arr_ts  # + => optimistic

        candidate["matched"] = True
        candidate["actualArrivalTs"] = actual_arr_ts
        candidate["driftSec"] = drift_sec

        s = stats.setdefault(stop_id, {
            "samples": 0,
            "sumDriftSec": 0.0,
            "phantoms": 0
        })
        s["samples"] += 1
        s["sumDriftSec"] += float(drift_sec)

    # phantom detection: predictions that are too old and never matched
    for stop_id, preds in preds_by_stop.items():
        s = stats.setdefault(stop_id, {"samples": 0, "sumDriftSec": 0.0, "phantoms": 0})
        for p in preds:
            if p.get("matched"):
                continue
            if ts_now > p["expectedArrivalTs"] + PHANTOM_GRACE_SEC:
                p["matched"] = True
                p["phantom"] = True
                s["phantoms"] += 1


def bias_color(bias_sec):
    ab = abs(bias_sec)
    if ab < 60:
        return "#00ff00"  # green
    if ab < 180:
        return "#f1c40f"  # amber
    return "ff0000"      # red


def export_geojson(state):
    features = []
    for stop_id, stop in state["stops"].items():
        lat = stop.get("lat"); lon = stop.get("lon")
        if lat is None or lon is None:
            continue

        st = state["stopStats"].get(stop_id, {"samples": 0, "sumDriftSec": 0.0, "phantoms": 0})
        samples = st["samples"]
        bias_sec = (st["sumDriftSec"] / samples) if samples > 0 else 0.0
        phantom_rate = (st["phantoms"] / max(1, samples + st["phantoms"]))

        feat = {
            "type": "Feature",
            "properties": {
                "stopId": stop_id,
                "name": stop.get("name"),
                "direction": state.get("direction"),
                "predictionDriftSec": round(bias_sec, 1),
                "samples": samples,
                "phantomRate": round(phantom_rate, 3),
                "color": bias_color(bias_sec)
            },
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            }
        }
        features.append(feat)

    geo = {
        "type": "FeatureCollection",
        "features": features,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "lineId": LINE_ID,
        "direction": state.get("direction")
    }
    with open(GEOJSON_OUT, "w", encoding="utf-8") as f:
        json.dump(geo, f, indent=2)


def one_cycle(state):
    arrivals = fetch_arrivals()
    arrivals = filter_direction(arrivals, DIRECTION_FILTER)

    ensure_stop_metadata(state, arrivals)
    update_predictions(state, arrivals)
    detect_arrivals_and_score(state, arrivals)
    export_geojson(state)

    state["lastUpdated"] = datetime.now(timezone.utc).isoformat()
    save_state(state)


def main():
    state = load_state()
    while True:
        try:
            one_cycle(state)
            print(f"[{state['lastUpdated']}] wrote {GEOJSON_OUT} ({state.get('direction')}) with {len(state['stops'])} stops")
        except Exception as e:
            print("Cycle error:", repr(e))
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
