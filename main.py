from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI()
DB_FILE = Path("db.json")

class Record(BaseModel):
    marketId: str
    marketLabel: str
    threshold1: float
    threshold2: float
    threshold3: float

# Helper functions
def load_db():
    if not DB_FILE.exists():
        return []
    with open(DB_FILE, "r") as f:
        return json.load(f)

def save_db(data):
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)

# CRUD Endpoints
@app.get("/records")
def get_records():
    return load_db()

@app.get("/records/{market_id}")
def get_record(market_id: str):
    records = load_db()
    for r in records:
        if r["marketId"] == market_id:
            return r
    raise HTTPException(status_code=404, detail="Record not found")

@app.post("/records")
def create_record(record: Record):
    records = load_db()
    if any(r["marketId"] == record.marketId for r in records):
        raise HTTPException(status_code=400, detail="Market ID already exists")
    records.append(record.dict())
    save_db(records)
    return record

@app.put("/records/{market_id}")
def update_record(market_id: str, record: Record):
    records = load_db()
    for i, r in enumerate(records):
        if r["marketId"] == market_id:
            records[i] = record.dict()
            save_db(records)
            return record
    raise HTTPException(status_code=404, detail="Record not found")

@app.delete("/records/{market_id}")
def delete_record(market_id: str):
    records = load_db()
    records = [r for r in records if r["marketId"] != market_id]
    save_db(records)
    return {"detail": "Record deleted"}
