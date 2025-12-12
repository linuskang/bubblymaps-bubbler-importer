#!/usr/bin/env python3
import json
from typing import Dict, Any, List
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# -----------------------------
# CONFIG
# -----------------------------
CONFIG = {
    "base_url": "YOUR_BUBBLYMAPS_INSTANCE",
    "headers": {
        "Content-Type": "application/json",
        "Authorization": "Bearer YOUR_API_TOKEN",
    },
    "timeout": 10.0,
    "dry_run": False,   # Set True to test parsing without sending
    "max_workers": 3,  # Number of threads for concurrent sending
    # If True, use broader/looser duplicate detection (may produce false positives).
    # Set to False to only detect clear Prisma P2002 / "Unique constraint failed" errors.
    "aggressive_duplicate_detection": False,
}

INPUT_JSON_FILE = "./LOCATION.json"

# -----------------------------
# SESSION (reuse connections)
# -----------------------------
session = requests.Session()
session.headers.update(CONFIG["headers"])

# -----------------------------
# API CALLS
# -----------------------------
def send_waypoint(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Send a single waypoint and return result dict."""
    url = CONFIG["base_url"].rstrip("/") + "/api/waypoints"

    if CONFIG["dry_run"]:
        return {"ok": True, "status_code": None, "payload": payload, "skipped": False}

    try:
        resp = session.post(url, json=payload, timeout=CONFIG["timeout"])
        ok = resp.ok
        status = resp.status_code
        # Try to parse structured JSON response when available
        try:
            body = resp.json()
        except Exception:
            body = resp.text

        skipped = False

        # Detect duplicate/unique-constraint rejections from the server and mark as skipped
        # Server often returns Prisma errors (e.g. P2002 / "Unique constraint failed on the fields: (`latitude`, `longitude`)" )
        if not ok:
            # body might be dict with `error` key, or a plain string
            err_text = ""
            if isinstance(body, dict):
                # possible shapes: { error: 'message' } or { message: '...' }
                err_text = (body.get("error") or body.get("message") or str(body))
            else:
                err_text = str(body)

            lowered = err_text.lower()
            # Strict detection (recommended): look for Prisma P2002 / unique constraint language
            is_prisma_p2002 = "unique constraint" in lowered or "p2002" in lowered or "unique constraint failed" in lowered

            # Broader detection (optional): look for generic words like 'duplicate' or both 'latitude' and 'longitude'
            is_loose_duplicate = ("duplicate" in lowered) or ("latitude" in lowered and "longitude" in lowered)

            if is_prisma_p2002 or (CONFIG.get("aggressive_duplicate_detection") and is_loose_duplicate):
                skipped = True
    except requests.RequestException as e:
        ok = False
        status = None
        body = str(e)
        skipped = False

    return {"ok": ok, "status_code": status, "response": body, "payload": payload, "skipped": skipped}


# -----------------------------
# CONVERT YOUR DATA FORMAT → API FORMAT
# -----------------------------
def convert_entry(entry: list) -> Dict[str, Any]:
    code = entry[0]
    categories = entry[1]
    lat = entry[2]
    lon = entry[3]
    return {
        "name": f"{categories[0]} {code}",
        "latitude": lat,
        "longitude": lon,
        "description": f"Auto-imported feature c1 '{categories[0]}' (code {code})",
        "addedByUserId": "data2",
    }

# -----------------------------
# MAIN IMPORTER
# -----------------------------
def main():
    with open(INPUT_JSON_FILE, "r", encoding="utf-8") as f:
        blob = json.load(f)

    # Support both top-level list or dict with "data"
    if isinstance(blob, dict):
        data_entries = blob.get("data", [])
    elif isinstance(blob, list):
        data_entries = blob
    else:
        raise ValueError("Unexpected format for import JSON")

    total = len(data_entries)
    print(f"Loaded {total} entries from file")

    payloads = [convert_entry(e) for e in data_entries]

    sent = 0
    failed = 0
    skipped = 0
    processed = 0
    lock = Lock()  # For thread-safe counter increment

    # Use ThreadPoolExecutor for concurrent sending
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
        futures = {executor.submit(send_waypoint, p): p for p in payloads}

        for future in as_completed(futures):
            result = future.result()
            name = result["payload"]["name"]

            with lock:
                processed += 1
                progress = f"[{processed}/{total}]"

                if result.get("skipped"):
                    resp = result.get("response")
                    status = result.get("status_code")
                    summary = resp if not isinstance(resp, dict) else json.dumps(resp)
                    short = (summary[:200] + "...") if isinstance(summary, str) and len(summary) > 200 else summary
                    print(f"{progress} ⏭ Skipped duplicate: {name} | {status} | {short}")
                    skipped += 1
                elif result["ok"]:
                    print(f"{progress} ✔ Success: {name}")
                    sent += 1
                else:
                    print(f"{progress} ❌ Failed: {name} | {result['status_code']} | {result.get('response')}")
                    failed += 1

    print("\n============================")
    print(f"Finished import.")
    print(f"Total sent:    {sent}")
    print(f"Total skipped: {skipped}")
    print(f"Total failed:  {failed}")
    print("============================")


if __name__ == "__main__":
    main()
