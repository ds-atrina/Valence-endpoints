from pydub import AudioSegment
import io
from pathlib import Path
from google.cloud import storage, pubsub_v1
import uuid
import os

storage_client = storage.Client()
BUCKET_NAME = os.getenv("BUCKET_NAME")

def upload_with_retries(bucket, blob_name, buffer, retries=3):
    for attempt in range(1, retries+1):
        try:
            buffer.seek(0)
            bucket.blob(blob_name).upload_from_file(buffer, content_type="audio/wav")
            return
        except Exception:
            if attempt == retries:
                raise


def convert_audio_to_wav(file):
    audio = AudioSegment.from_file(file)
    audio = audio.set_channels(1).set_sample_width(2).set_frame_rate(16000)
    buf = io.BytesIO()
    audio.export(
        buf,
        format="wav",
        parameters=["-acodec", "pcm_s16le"]  # <--- Ensures 16-bit linear PCM
    )
    buf.seek(0)

    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"{uuid.uuid4().hex}_{file.rsplit('.',1)[0]}.wav"
    upload_with_retries(bucket, blob_name, buf)



convert_audio_to_wav("C:/Users/atrn1/Downloads/Vr.m4a")

