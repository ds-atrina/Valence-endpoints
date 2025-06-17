import hashlib
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from pydub import AudioSegment
from pymongo import MongoClient
from google.cloud import storage, pubsub_v1
import uuid, io, datetime, urllib.parse, os, json
from pydantic import BaseModel, Field
from typing import List, Optional
from bson import ObjectId
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "accounts_key.json"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
BUCKET_NAME = os.getenv("BUCKET_NAME")
url=os.getenv("URL")
port=os.getenv("MONGO_PORT")
db_name = os.getenv("DB_NAME")
collection_name = os.getenv("COLLECTION_NAME")

user = urllib.parse.quote_plus(os.getenv("USERNAME"))
pw   = urllib.parse.quote_plus(os.getenv("PASSWORD"))
 
MONGO_URI = f"mongodb://{user}:{pw}@{url}:{port}/{db_name}?authSource=admin"
collection = MongoClient(MONGO_URI)[db_name][collection_name]

MAX_SIZE = 10 * 1024 * 1024  # 10 MB


def upload_with_retries(bucket, blob_name, buffer, retries=3):
    for attempt in range(1, retries+1):
        try:
            buffer.seek(0)
            bucket.blob(blob_name).upload_from_file(buffer, content_type="audio/wav")
            return
        except Exception:
            if attempt == retries:
                raise


@app.post("/upload-audio")
async def upload_audio(
    file: UploadFile = File(...),
    region: str = Form(...),
    product_name: str = Form(...),
    date: str = Form(...)
):
    # 1. True size check
    contents = await file.read()
    if len(contents) > MAX_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="File too large (max 10 MB)."
        )
    await file.seek(0)

    # 2. Content-type & extension check
    if not file.content_type.startswith("audio/"):
        raise HTTPException(400, "Must upload an audio file")
    ext = file.filename.rsplit(".",1)[-1].lower()
    if ext not in {"mp3", "wav", "ogg"}:
        raise HTTPException(400, "Only .mp3, .wav or .ogg allowed")

    # 3. Compute idempotency hash
    hasher = hashlib.sha256()
    hasher.update(contents)
    # include metadata
    hasher.update(region.encode())
    hasher.update(product_name.encode())
    hasher.update(date.encode())
    file_hash = hasher.hexdigest()

    # 4. Check for duplicate
    existing = collection.find_one({"file_hash": file_hash})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Duplicate upload: record {existing['_id']}"
        )

    # 5. Load & measure duration
    try:
        audio = AudioSegment.from_file(file.file)
    except Exception:
        raise HTTPException(400, "Could not parse audio file")
    total_secs = int(round(audio.duration_seconds))
    minutes = total_secs // 60
    seconds = total_secs % 60
    duration_mmss = f"{minutes:02d}:{seconds:02d}"
    audio = audio.set_channels(1).set_sample_width(2).set_frame_rate(16000)
    buf = io.BytesIO()
    audio.export(buf, format="wav")

    # 6. GCS upload
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"{uuid.uuid4().hex}_{file.filename.rsplit('.',1)[0]}.wav"
    try:
        upload_with_retries(bucket, blob_name, buf)
        gcs_uri = f"gs://{BUCKET_NAME}/{blob_name}"
    except Exception as e:
        raise HTTPException(500, f"GCS upload failed: {e}")

    # 7. Insert into MongoDB (including file_hash)
    doc = {
        "audio_url": gcs_uri,
        "file_name": file.filename,
        "file_size": len(contents),
        "duration": duration_mmss,
        "upload_timestamp": datetime.utcnow(),
        "region": region,
        "product_name": product_name,
        "date": date,
        "file_hash": file_hash,
        "status": "Processing",
        "transcript": "",
        "translation": "",
        "transcript_status": "Processing",
        "translation_status": "Processing",
        "insights_status": "Processing",
    }
    res = collection.insert_one(doc)
    record_id = str(res.inserted_id)

    # 8. Publish Pub/Sub message
    try:
        payload = json.dumps({"gcs_uri": gcs_uri, "record_id": record_id}).encode("utf-8")
        publisher.publish(PUBSUB_TOPIC, payload).result()
    except Exception as e:
        # rollback
        collection.delete_one({"_id": res.inserted_id})
        bucket.blob(blob_name).delete()
        raise HTTPException(500, f"Pub/Sub publish failed; rollback done: {e}")

    return {"message": "Success", "gcs_uri": gcs_uri, "record_id": record_id }


class RecordListItem(BaseModel):
    id: str = Field(..., alias="_id")
    date: str
    product_name: str
    duration: str
    status: str
    region: str
    model_config = {
        "validate_by_name": True
    }

class RecordDetail(BaseModel):
    id: str = Field(..., alias="_id")
    transcript: str
    translation: str
    model_config = {
        "validate_by_name": True
    }

class RecordSummary(BaseModel):
    total: int
    transcript_completed: int
    translation_completed: int
    insight_completed: int

def serialize_id(doc):
    doc["_id"] = str(doc["_id"])
    return doc




@app.get("/summary", response_model=RecordSummary)
def get_summary(
    status: Optional[List[str]] = Query(None),
    product_name: Optional[List[str]] = Query(None),
    region: Optional[List[str]] = Query(None),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD")
):
    filt = {}
    print("hello1")
    if status and "All" not in status:
        filt["status"] = {"$in": status}
    if product_name and "All" not in product_name:
        filt["product_name"] = {"$in": product_name}
    if region and "All" not in region:
        filt["region"] = {"$in": region}
    print("hello2")
    if from_date or to_date:
        try:
            date_filter = {}
            if from_date:
                date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d").date().isoformat()
            if to_date:
                date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d").date().isoformat()
            filt["date"] = date_filter
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    print("hello3")
    return RecordSummary(
        total=collection.count_documents(filt),
        transcript_completed=collection.count_documents({**filt, "transcript_status": "Completed"}),
        translation_completed=collection.count_documents({**filt, "translation_status": "Completed"}),
        insight_completed=collection.count_documents({**filt, "insight_status": "Completed"})
    )


@app.get("/records", response_model=List[RecordListItem])
def list_records(
    status: Optional[List[str]]        = Query(None, description="Processing, Completed, Failed, All"),
    product_name: Optional[List[str]]  = Query(None, description="All, Osopaan-D, Virilex"),
    region: Optional[List[str]]        = Query(None, description="All, Maharashtra, Punjab, Rajasthan"),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD")
):
    filt = {}
    # status filter
    if status and "All" not in status:
        filt["status"] = {"$in": status}
    # product filter
    if product_name and "All" not in product_name:
        filt["product_name"] = {"$in": product_name}
    # region filter
    if region and "All" not in region:
        filt["region"] = {"$in": region}

    if from_date or to_date:
        try:
            date_filter = {}
            if from_date:
                date_filter["$gte"] = datetime.strptime(from_date, "%Y-%m-%d").date().isoformat()
            if to_date:
                date_filter["$lte"] = datetime.strptime(to_date, "%Y-%m-%d").date().isoformat()
            filt["date"] = date_filter
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    docs = collection.find(filt, {
        "_id": 1,
        "date": 1,
        "product_name": 1,
        "duration": 1,
        "status": 1,
        "region": 1
    }).sort("upload_timestamp", -1)

    return [serialize_id(doc) for doc in docs]


@app.get("/records/{record_id}", response_model=RecordDetail)
def get_record(record_id: str):
    try:
        oid = ObjectId(record_id)
    except Exception:
        raise HTTPException(400, "Invalid record ID")

    doc = collection.find_one({"_id": oid}, {"audio_url":1, "transcript": 1, "translation": 1})
    if not doc:
        raise HTTPException(404, "Record not found")

    return serialize_id(doc)
