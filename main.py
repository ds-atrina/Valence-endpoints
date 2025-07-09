import hashlib
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, status, Query, Depends
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
from jose import jwt, JWTError
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "accounts_key.json"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    # allow_origins=["*"],
    allow_origins=["http://localhost:3000", "https://dev-pharma-ai.valenceai.in", "https://mankind.valenceai.in"],
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
SECRET_KEY = os.getenv("NEXTAUTH_SECRET")
ALGORITHM = "HS256"
auth_scheme = HTTPBearer()

user = urllib.parse.quote_plus(os.getenv("USERNAME"))
pw   = urllib.parse.quote_plus(os.getenv("PASSWORD"))
 
MONGO_URI = f"mongodb://{user}:{pw}@{url}:{port}/{db_name}?authSource=admin"
collection = MongoClient(MONGO_URI)[db_name][collection_name]
insights_col = MongoClient(MONGO_URI)[db_name]["insights_records"]
faq_col = MongoClient(MONGO_URI)[db_name]["faq_records"]
actions_col = MongoClient(MONGO_URI)[db_name]["actions_records"]

MAX_SIZE = 10 * 1024 * 1024  # 10 MB

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    token = credentials.credentials
    try:
        print("Incoming token:", token)
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        print("Decoded payload:", payload)
        email = payload.get("email")
        if email is None:
            raise HTTPException(status_code=401, detail="Email missing in token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

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
    date: str = Form(...),
    user_email: str = Depends(verify_token)
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
        "user_email": user_email
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
    audio_url: str
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
    # if status and "All" not in status:
    #     filt["status"] = {"$in": status}
    # if product_name and "All" not in product_name:
    #     filt["product_name"] = {"$in": product_name}
    # if region and "All" not in region:
    #     filt["region"] = {"$in": region}
    if status and not any(s.lower() == "all" for s in status):
        filt["status"] = {"$in": status}
    if product_name and not any(p.lower() == "all" for p in product_name):
        filt["product_name"] = {"$in": product_name}
    if region and not any(r.lower() == "all" for r in region):
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
        
    print("Filter for summary:", filt)
    return RecordSummary(
        total=collection.count_documents(filt),
        transcript_completed=collection.count_documents({**filt, "transcript_status": "Completed"}),
        translation_completed=collection.count_documents({**filt, "translation_status": "Completed"}),
        insight_completed=collection.count_documents({**filt, "insights_status": "Completed"})
    )


@app.get("/records", response_model=List[RecordListItem])
def list_records(
    user_email: Optional[str] = Query(None),
    status: Optional[List[str]]        = Query(None, description="Processing, Completed, Failed, All"),
    product_name: Optional[List[str]]  = Query(None, description="All, Osopaan-D, Virilex"),
    region: Optional[List[str]]        = Query(None, description="All, Maharashtra, Punjab, Rajasthan"),
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD")
):
    filt = {}
    if user_email:
        filt["user_email"] = user_email
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

    print("Filter for records:", filt)
    docs = collection.find(filt, {
        "_id": 1,
        "date": 1,
        "product_name": 1,
        "duration": 1,
        "status": 1,
        "region": 1,
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
    print("Final response doc:", doc)

    return serialize_id(doc)


class Insight(BaseModel):
    product: str
    theme: str
    region: str
    occurrence: int
    summary: str
    insight_id: str


@app.get(
    "/insights",
    response_model=List[Insight],
    summary="Get insights by product + region with occurrence > min_occurrence"
)
def get_insights(
    product: str = Query(..., examples=["Virilex"]),
    region: str = Query(..., examples=["Delhi"]),
    min_occurrence: int = Query(..., ge=1, examples=[3])
):
    """
    Return all documents for the given **product** and **region**
    whose `occurrence` value is **strictly greater than** `min_occurrence`.
    """
    mongo_filter = {
        "product": product,
        "region": region,
        "occurrence": {"$gt": min_occurrence}     # $gt == “greater than” :contentReference[oaicite:0]{index=0}
    }

    # Projection excludes _id to keep response clean
    cursor = insights_col.find(mongo_filter, {"_id": 0})
    results = list(cursor)

    if not results:
        raise HTTPException(
            status_code=404,
            detail="No insights matched your filter."
        )

    return results



@app.get(
    "/canonical_questions",
    summary="Get canonical questions by product/region",
)
def get_canonical_questions(
    product: str = Query(..., examples=["Zymora-AP"]),          # FastAPI turns query-string into typed vars :contentReference[oaicite:1]{index=1}
    region:  str = Query(..., examples=["All"])
) -> List[Dict[str, Any]]:
    """
    * **region = All** → `[{'canonical_question': str, 'occurrences': int}, ...]`
    * **region = <city>** → `[{'canonical_question': str,
                               'region_count': int,
                               'occurrences' : int}, ...]`
    """
    base = {"product": product}

    # ── Case 1 : aggregate across all regions ────────────────────────────
    if region.lower() == "all":
        cursor = faq_col.find(
            base,
            {"_id": 0, "canonical_question": 1, "occurrences": 1}  # simple projection
        )
        results = list(cursor)
        if not results:
            raise HTTPException(404, "No matching questions.")
        return results

    # ── Case 2 : filter for a single region ──────────────────────────────
    region_field = f"region_counts.{region}"
    base[region_field] = {"$gt": 0}                                  # only docs with >0 hits in that city :contentReference[oaicite:2]{index=2}

    cursor = faq_col.find(
        base,
        {"_id": 0, "canonical_question": 1, "occurrences": 1, region_field: 1}
    )

    bucket: List[Dict[str, Any]] = []
    for doc in cursor:
        bucket.append({
            "canonical_question": doc["canonical_question"],
            "region_count"     : doc.get(region_field.split(".")[0], {}).get(region, 0)
        })

    if not bucket:
        raise HTTPException(404, "No questions for that region.")
    return bucket



@app.get("/actions")
def get_actions(product: Optional[str] = Query(None, description="Filter by product name")):
    query = {}
    if product:
        query["product"] = product

    cursor = actions_col.find(query, {"_id": 0, "product": 1, "actions": 1})
    results = list(cursor)
    return {"data": results}