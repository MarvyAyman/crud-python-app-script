from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI()
DB_FILE = Path("db.json")

class Record(BaseModel):
    id: int
    name: str
    email: str

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

@app.get("/records/{record_id}")
def get_record(record_id: int):
    records = load_db()
    for r in records:
        if r["id"] == record_id:
            return r
    raise HTTPException(status_code=404, detail="Record not found")

@app.post("/records")
def create_record(record: Record):
    records = load_db()
    if any(r["id"] == record.id for r in records):
        raise HTTPException(status_code=400, detail="ID already exists")
    records.append(record.dict())
    save_db(records)
    return record

@app.put("/records/{record_id}")
def update_record(record_id: int, record: Record):
    records = load_db()
    for i, r in enumerate(records):
        if r["id"] == record_id:
            records[i] = record.dict()
            save_db(records)
            return record
    raise HTTPException(status_code=404, detail="Record not found")

@app.delete("/records/{record_id}")
def delete_record(record_id: int):
    records = load_db()
    records = [r for r in records if r["id"] != record_id]
    save_db(records)
    return {"detail": "Record deleted"}
