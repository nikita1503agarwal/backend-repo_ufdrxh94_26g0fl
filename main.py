import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
from pydantic import BaseModel

from database import db, create_document, get_documents
from schemas import Turbine

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}

@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}

@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    
    try:
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
            
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"
    
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
    
    return response


# ---------- Turbine ingestion from Google Sheets ----------
class ImportResult(BaseModel):
    inserted: int
    updated: int


def fetch_google_sheet_csv(sheet_url: str) -> str:
    """Convert a Google Sheets URL to CSV export URL and fetch raw CSV text."""
    # Accept either full edit URL or already-export URL
    if "export?format=csv" in sheet_url:
        export_url = sheet_url
    else:
        # Parse doc id and optional gid
        # Typical form: https://docs.google.com/spreadsheets/d/{doc_id}/edit?gid={gid}
        try:
            parts = sheet_url.split("/d/")[1]
            doc_id = parts.split("/")[0]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid Google Sheets URL")
        # Extract gid if provided
        gid = None
        if "gid=" in sheet_url:
            try:
                gid = sheet_url.split("gid=")[1].split("&")[0]
            except Exception:
                gid = None
        if gid:
            export_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}"
        else:
            export_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv"

    resp = requests.get(export_url, timeout=20)
    if resp.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Failed to fetch sheet CSV: {resp.status_code}")
    return resp.text


def parse_turbine_csv(csv_text: str) -> List[Turbine]:
    import csv
    from io import StringIO

    f = StringIO(csv_text)
    reader = csv.DictReader(f)

    records: List[Turbine] = []
    # Map common column aliases
    for row in reader:
        # Normalize keys to lowercase for matching
        normalized = { (k or '').strip().lower(): (v or '').strip() for k, v in row.items() }
        name = normalized.get('name') or normalized.get('turbine') or normalized.get('id') or normalized.get('identifier')
        status = normalized.get('status') or normalized.get('state')
        lat = normalized.get('lat') or normalized.get('latitude')
        lng = normalized.get('lng') or normalized.get('longitude')
        capacity = normalized.get('capacity_mw') or normalized.get('capacity') or normalized.get('mw')
        location = normalized.get('location') or normalized.get('site') or normalized.get('address')

        def to_float(x: Optional[str]) -> Optional[float]:
            try:
                return float(x) if x not in (None, '', 'NA', 'N/A') else None
            except Exception:
                return None

        record = Turbine(
            name=name or "Unnamed Turbine",
            status=(status or "Unknown").title(),
            latitude=to_float(lat),
            longitude=to_float(lng),
            capacity_mw=to_float(capacity),
            location=location or None,
        )
        records.append(record)

    return records


@app.post("/api/turbines/import", response_model=ImportResult)
def import_turbines(sheet_url: str = Query(..., description="Google Sheet URL or CSV export URL")):
    """Fetch turbines from Google Sheets and upsert into DB.
    Uses the 'name' as a simple identifier for upsert.
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")

    csv_text = fetch_google_sheet_csv(sheet_url)
    turbines = parse_turbine_csv(csv_text)

    inserted = 0
    updated = 0
    for t in turbines:
        # Upsert by name
        existing = db['turbine'].find_one({ 'name': t.name })
        payload = t.model_dump()
        if existing:
            db['turbine'].update_one({'_id': existing['_id']}, {'$set': payload})
            updated += 1
        else:
            create_document('turbine', payload)
            inserted += 1

    return ImportResult(inserted=inserted, updated=updated)


class TurbineOut(Turbine):
    id: Optional[str] = None


def serialize_doc(doc: dict) -> dict:
    if not doc:
        return {}
    d = dict(doc)
    if '_id' in d:
        d['id'] = str(d.pop('_id'))
    return d


@app.get("/api/turbines", response_model=List[TurbineOut])
def list_turbines(status: Optional[str] = Query(None, description="Filter by status e.g., Active or Inactive")):
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    filt = {}
    if status:
        filt['status'] = status.title()
    docs = get_documents('turbine', filt)
    return [serialize_doc(d) for d in docs]


class TurbineStats(BaseModel):
    active: int
    inactive: int
    unknown: int


@app.get("/api/turbines/stats", response_model=TurbineStats)
def turbine_stats():
    if db is None:
        raise HTTPException(status_code=500, detail="Database not configured")
    def count(s: str) -> int:
        return db['turbine'].count_documents({'status': s})
    return TurbineStats(
        active=count('Active'),
        inactive=count('Inactive'),
        unknown=count('Unknown'),
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
