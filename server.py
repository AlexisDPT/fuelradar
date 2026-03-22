"""
FUEL_RADAR — Serveur tout-en-un
- Fetch les prix toutes les 2h depuis data.economie.gouv.fr
- Les stocke dans SQLite (fichier local, zéro config)
- Sert l'API REST + le frontend HTML
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Base de données SQLite ─────────────────────────────────────────────────────

DB_PATH = Path(os.environ.get("DATA_DIR", ".")) / "fuelradar.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stations (
            id       TEXT PRIMARY KEY,
            name     TEXT,
            address  TEXT,
            city     TEXT,
            lat      REAL,
            lon      REAL,
            services TEXT
        );
        CREATE TABLE IF NOT EXISTS prices (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            fuel       TEXT,
            prix       REAL,
            day        TEXT,
            UNIQUE(station_id, fuel, day)
        );
        CREATE INDEX IF NOT EXISTS idx_prices_lookup
            ON prices(station_id, fuel, day);
        CREATE INDEX IF NOT EXISTS idx_stations_pos
            ON stations(lat, lon);
    """)
    conn.commit()
    conn.close()
    log.info(f"✅ Base SQLite prête : {DB_PATH}")

# ── Fetch données ──────────────────────────────────────────────────────────────

DATASET  = "prix-des-carburants-en-france-flux-instantane-v2"
API_URL  = f"https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/{DATASET}/records"
FUELS    = ["gazole","sp95","sp98","e10","e85","gplc"]
LABELS   = {"gazole":"Gazole","sp95":"SP95","sp98":"SP98","e10":"E10","e85":"E85","gplc":"GPLc"}

async def fetch_all_stations():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = 0
    offset = 0

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                r = await client.get(API_URL, params={"limit": 100, "offset": offset})
                r.raise_for_status()
                results = r.json().get("results", [])
            except Exception as e:
                log.error(f"Erreur API offset={offset}: {e}")
                break

            if not results:
                break

            conn = get_db()
            try:
                for rec in results:
                    geom = rec.get("geom") or {}
                    lat  = geom.get("lat")
                    lon  = geom.get("lon")
                    if not lat or not lon:
                        continue

                    sid  = str(rec.get("id", f"{lat}_{lon}"))
                    name = rec.get("nom") or rec.get("enseignes") or rec.get("marque") or "Station"
                    svcs = rec.get("services_service") or []

                    conn.execute("""
                        INSERT INTO stations(id,name,address,city,lat,lon,services)
                        VALUES(?,?,?,?,?,?,?)
                        ON CONFLICT(id) DO UPDATE SET
                            name=excluded.name, address=excluded.address,
                            city=excluded.city, lat=excluded.lat,
                            lon=excluded.lon,   services=excluded.services
                    """, (sid, name, rec.get("adresse",""), rec.get("ville",""),
                          float(lat), float(lon), json.dumps(svcs)))

                    for fuel in FUELS:
                        val = rec.get(f"{fuel}_prix")
                        if val is None:
                            continue
                        try:
                            prix = float(val)
                            if not (0.5 < prix < 5.0):
                                continue
                        except (ValueError, TypeError):
                            continue

                        conn.execute("""
                            INSERT INTO prices(station_id,fuel,prix,day)
                            VALUES(?,?,?,?)
                            ON CONFLICT(station_id,fuel,day) DO UPDATE SET prix=excluded.prix
                        """, (sid, LABELS[fuel], prix, today))

                conn.commit()
            finally:
                conn.close()

            total  += len(results)
            offset += len(results)
            log.info(f"  {total} stations traitées...")

            if len(results) < 100:
                break

    # Purge les prix > 30 jours
    conn = get_db()
    conn.execute("DELETE FROM prices WHERE day < date('now','-30 days')")
    conn.commit()
    conn.close()
    log.info(f"✅ Fetch terminé — {total} stations.")

# ── Cron toutes les 2h ─────────────────────────────────────────────────────────

FETCH_INTERVAL = 2 * 60 * 60  # secondes

async def cron():
    while True:
        log.info("⛽ Début fetch carburants...")
        try:
            await fetch_all_stations()
        except Exception as e:
            log.error(f"Erreur fetch: {e}")
        await asyncio.sleep(FETCH_INTERVAL)

# ── App FastAPI ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(cron())
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# ── Routes API ─────────────────────────────────────────────────────────────────

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

@app.get("/api/stations")
def api_stations(
    lat:    float = Query(...),
    lng:    float = Query(...),
    radius: float = Query(5.0, ge=1, le=50)
):
    conn = get_db()

    # Bounding box rapide avant haversine exact
    deg = radius / 111
    rows = conn.execute("""
        SELECT id, name, address, city, lat, lon, services
        FROM stations
        WHERE lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
    """, (lat - deg, lat + deg, lng - deg, lng + deg)).fetchall()

    stations = []
    for row in rows:
        dist = haversine(lat, lng, row["lat"], row["lon"])
        if dist > radius:
            continue

        # Dernier prix connu par carburant
        prices_rows = conn.execute("""
            SELECT fuel, prix, day
            FROM prices
            WHERE station_id = ?
            ORDER BY day DESC
        """, (row["id"],)).fetchall()

        prices = {}
        for p in prices_rows:
            if p["fuel"] not in prices:  # garde uniquement le plus récent
                prices[p["fuel"]] = {"prix": p["prix"], "maj": p["day"]}

        if not prices:
            continue

        stations.append({
            "id":      row["id"],
            "name":    row["name"],
            "address": row["address"],
            "city":    row["city"],
            "lat":     row["lat"],
            "lon":     row["lon"],
            "dist":    round(dist, 2),
            "services": json.loads(row["services"] or "[]"),
            "prices":  prices
        })

    conn.close()
    stations.sort(key=lambda s: s["dist"])
    return {"stations": stations, "count": len(stations)}


@app.get("/api/history")
def api_history(
    ids:  str = Query(...),
    fuel: str = Query("SP95"),
    days: int = Query(30, ge=1, le=30)
):
    station_ids = [s.strip() for s in ids.split(",") if s.strip()][:20]
    if not station_ids:
        raise HTTPException(400, "Aucun ID")

    placeholders = ",".join("?" * len(station_ids))
    conn = get_db()
    rows = conn.execute(f"""
        SELECT station_id, day, AVG(prix) as avg_prix
        FROM prices
        WHERE station_id IN ({placeholders})
          AND fuel = ?
          AND day >= date('now', ? || ' days')
        GROUP BY station_id, day
        ORDER BY station_id, day
    """, (*station_ids, fuel, f"-{days}")).fetchall()
    conn.close()

    result = {}
    for row in rows:
        sid = row["station_id"]
        if sid not in result:
            result[sid] = {}
        result[sid][row["day"]] = round(row["avg_prix"], 3)

    return {"history": result, "fuel": fuel}


@app.get("/api/status")
def api_status():
    conn = get_db()
    stations = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    prices   = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    last     = conn.execute("SELECT MAX(day) FROM prices").fetchone()[0]
    conn.close()
    return {"status": "ok", "stations": stations, "prices": prices, "last_update": last}


# ── Sert le frontend ───────────────────────────────────────────────────────────

FRONTEND = Path(__file__).parent / "index.html"

@app.get("/")
def index():
    return HTMLResponse(FRONTEND.read_text(encoding="utf-8"))
