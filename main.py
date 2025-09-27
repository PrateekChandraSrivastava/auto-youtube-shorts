#!/usr/bin/env python3
"""
Improved worker:
- Avoids repeating exact image combos (stores used signatures on Drive in used_sets.json)
- Downloads a random royalty-free music mp3 from a Drive music folder and mixes it into the video
- Generates title/description via template or OpenAI (if OPENAI_API_KEY secret present)
"""

import os
import io
import random
import time
import json
import subprocess
import hashlib
import re
import unicodedata

from pathlib import Path

# Google API imports
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request

# Optional OpenAI
import requests

# Configuration (via env)
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
DRIVE_MUSIC_FOLDER_ID = os.environ.get("DRIVE_MUSIC_FOLDER_ID")  # new secret
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")  # JSON content of service account
NUM_IMAGES = int(os.environ.get("NUM_IMAGES", "6"))

YT_CLIENT_ID = os.environ.get("YT_CLIENT_ID")
YT_CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET")
YT_REFRESH_TOKEN = os.environ.get("YT_REFRESH_TOKEN")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")  # optional

TMP_DIR = Path("/tmp/auto_youtube")
STATE_FILE = Path("state/used_sets.json")
IMAGES_DIR = TMP_DIR / "images"
OUTPUT_VIDEO = TMP_DIR / "output_short.mp4"
OUTPUT_VIDEO_WITH_AUDIO = TMP_DIR / "output_short_audio.mp4"

SAFE_MAX_TITLE = 95  # YouTube allows up to 100; keep a buffer
 
# Drive helper filenames
USED_SETS_FILENAME = "used_sets.json"  # stored in the image folder on Drive

# Scopes
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

YT_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


# --- Helpers: Drive service init -------------------------------------------------
def ensure_dirs():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

def init_drive_service():
    if not GCP_SA_KEY:
        raise RuntimeError("GCP_SA_KEY not set.")
    sa_json = json.loads(GCP_SA_KEY)
    sa_path = TMP_DIR / "sa.json"
    with open(sa_path, "w") as f:
        json.dump(sa_json, f)
    creds = service_account.Credentials.from_service_account_file(str(sa_path), scopes=DRIVE_SCOPES)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

# --- Drive file utilities -------------------------------------------------------
def list_files_in_folder(drive_service, folder_id, mime_contains=None):
    query = f"'{folder_id}' in parents and trashed = false"
    if mime_contains:
        query += f" and (mimeType contains '{mime_contains}')"
    files = []
    page_token = None
    while True:
        resp = drive_service.files().list(q=query,
                                         spaces='drive',
                                         fields="nextPageToken, files(id, name, mimeType)",
                                         pageToken=page_token,
                                         pageSize=200).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken", None)
        if not page_token:
            break
    return files

def download_file_to_path(drive_service, file_id, dest_path):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

def upload_bytes_as_file(drive_service, folder_id, name, data_bytes, mime_type="application/json"):
    media = MediaIoBaseDownload  # dummy to keep style
    # Use files().create to upload bytes
    file_metadata = {"name": name, "parents": [folder_id]}
    media_body = MediaFileUpload(io.BytesIO(data_bytes), mimetype=mime_type) if False else None
    # Because MediaFileUpload doesn't accept BytesIO directly in this environment, we'll write temp file
    tmp = TMP_DIR / f"tmp_upload_{int(time.time())}"
    with open(tmp, "wb") as f:
        f.write(data_bytes)
    media_body = MediaFileUpload(str(tmp), mimetype=mime_type)
    # if file exists, we need to update; return file id
    # First search file by name in folder
    q = f"'{folder_id}' in parents and name = '{name}' and trashed = false"
    resp = drive_service.files().list(q=q, spaces='drive', fields='files(id,name)').execute()
    if resp.get("files"):
        file_id = resp["files"][0]["id"]
        drive_service.files().update(fileId=file_id, media_body=media_body).execute()
        return file_id
    else:
        file = drive_service.files().create(body=file_metadata, media_body=media_body, fields='id').execute()
        return file.get("id")

# --- Used-set tracking ----------------------------------------------------------
def compute_signature(file_meta_list):
    # stable signature from sorted Drive file IDs
    ids = sorted([f["id"] for f in file_meta_list])
    raw = ",".join(ids).encode("utf-8")
    import hashlib
    return hashlib.sha256(raw).hexdigest()

def load_used_sets_local():
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()

def save_used_sets_local(used_set):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(list(used_set), f)

# --- Image selection (avoid repeats) -------------------------------------------
def pick_images_avoiding_repeats(drive_service, folder_id, n, max_attempts=20):
    files = list_files_in_folder(drive_service, folder_id, mime_contains="image/")
    if not files:
        raise RuntimeError("No images found in Drive folder.")
    used = load_used_sets_local()
    attempts = 0
    while attempts < max_attempts:
        chosen = random.sample(files, min(n, len(files)))
        sig = compute_signature(chosen)
        if sig not in used:
            used.add(sig)
            save_used_sets_local(used)
            return chosen
        attempts += 1
    # fallback if we exhausted combinations
    chosen = random.sample(files, min(n, len(files)))
    sig = compute_signature(chosen)
    used.add(sig)
    save_used_sets_local(used)
    return chosen


# --- Music selection ------------------------------------------------------------
def pick_random_music_and_download(drive_service, music_folder_id):
    # Try audio mime first
    files = list_files_in_folder(drive_service, music_folder_id, mime_contains="audio/")
    # Fallback: list all and filter by extension
    if not files:
        all_files = list_files_in_folder(drive_service, music_folder_id, mime_contains=None)
        files = [f for f in all_files if any(f["name"].lower().endswith(ext) for ext in (".mp3", ".wav", ".m4a"))]
    if not files:
        print("No music files found in the Drive music folder.")
        return None
    chosen = random.choice(files)
    dest = TMP_DIR / f"music_{Path(chosen['name']).stem}{Path(chosen['name']).suffix}"
    print("Selected music:", chosen["name"])
    download_file_to_path(drive_service, chosen["id"], str(dest))
    return str(dest)


# --- Video building & mixing ----------------------------------------------------
def build_video_from_images(image_paths, output_path, per_image_sec=5):
    list_txt = TMP_DIR / "list.txt"
    with open(list_txt, "w", encoding="utf-8") as f:
        for p in image_paths:
            f.write(f"file '{p}'\n")
            f.write(f"duration {per_image_sec}\n")
        f.write(f"file '{image_paths[-1]}'\n")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(list_txt),
        "-vf",
        "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
        "-r", "30", "-c:v", "libx264", "-crf", "23", "-preset", "medium",
        str(output_path)
    ]
    subprocess.check_call(cmd)

def mix_audio_into_video(video_in, audio_in, video_out):
    # Mix audio (loop if audio longer than video) and produce final mp4
    # -shortest ensures the output stops at the shorter stream (video); we use -stream_loop for audio
    cmd = [
        "ffmpeg", "-y",
        "-i", str(video_in),
        "-stream_loop", "-1", "-i", str(audio_in),
        "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
        "-map", "0:v:0", "-map", "1:a:0",
        "-shortest",
        str(video_out)
    ]
    subprocess.check_call(cmd)

# --- YouTube upload ------------------------------------------------------------
def youtube_upload(video_path, title, description, tags=None, privacy="unlisted"):
    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN):
        raise RuntimeError("YouTube OAuth credentials are missing.")
    creds = OAuthCredentials(
        token=None,
        refresh_token=YT_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        scopes=YT_SCOPES
    )
    creds.refresh(Request())
    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags or [],
            "categoryId": "22"
        },
        "status": {"privacyStatus": privacy}
    }
    media = MediaFileUpload(str(video_path), chunksize=-1, resumable=True, mimetype="video/mp4")
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status:
            print(f"Upload progress: {int(status.progress()*100)}%")
    return response.get("id")




def strip_control_and_unsupported(text: str) -> str:
    # remove control chars
    text = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C")
    # collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text

def keep_basic_chars(text: str) -> str:
    # allow letters, numbers, common punctuation, spaces, dashes, underscores, emoji removal
    # remove any char that is not in this safe set
    return re.sub(r"[^A-Za-z0-9 .,;:!?@#&()\-_/|’'\"+]", "", text)

def make_safe_title(raw: str, default="Auto Short"):
    t = strip_control_and_unsupported(raw)
    t = keep_basic_chars(t)
    t = t.strip(" .-_")
    if not t:
        t = default
    if len(t) > SAFE_MAX_TITLE:
        t = t[:SAFE_MAX_TITLE].rstrip()
    return t

# --- Title & description generation --------------------------------------------
def generate_title_and_desc_from_template(image_meta_list):
    # free template method (always available)
    keywords = []
    for f in image_meta_list:
        name = f.get("name", "")
        base = Path(name).stem
        keywords.append(base.replace("_", " ").split()[0:2])
    # Join a couple of words from filenames for context
    sample = " ".join([Path(f["name"]).stem.split()[0] for f in image_meta_list[:3]])
    ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    title = f"{sample} — Auto Short ({ts})"
    description = f"Auto-generated short using images: {', '.join([p['name'] for p in image_meta_list])}\n#shorts #autopost"
    tags = ["shorts", "auto"]
    return title, description, tags

def generate_title_and_desc_openai(image_meta_list):
    if not OPENAI_API_KEY:
        return generate_title_and_desc_from_template(image_meta_list)
    # Prepare prompt
    file_names = [p["name"] for p in image_meta_list]
    prompt = (
        f"You are a short-form video copywriter. Given these image names: {file_names}\n"
        "Write:\n1) a catchy short title <= 60 chars\n2) a 2-line description including 2-3 hashtags\n3) 6 concise hashtags (comma-separated only)\nTone: punchy and curiosity-driven, global audience."
    )
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    # Using OpenAI-compatible API (basic POST to chat completions if available)
    data = {
        "model": "gpt-4o-mini",  # change if unavailable; this is just an example
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200,
        "temperature": 0.8
    }
    try:
        resp = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data, timeout=30)
        resp.raise_for_status()
        r = resp.json()
        content = r["choices"][0]["message"]["content"].strip()
        # naive parsing: split lines
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        title = lines[0][:60] if lines else "Auto Short"
        description = ("\n".join(lines[1:3])) if len(lines) > 1 else "Auto-generated short. #shorts"
        # last line hashtags parsing
        hashtags = []
        for l in lines[3:]:
            if "#" in l or "," in l:
                parts = [t.strip().lstrip("#") for t in l.replace(",", " ").split() if t.strip()]
                hashtags.extend(["#"+p for p in parts if p])
        tags = [t.lstrip("#") for t in hashtags][:10]
        return title, description, tags or ["shorts", "auto"]
    except Exception as e:
        print("OpenAI call failed:", e)
        return generate_title_and_desc_from_template(image_meta_list)

# --- Main flow -----------------------------------------------------------------
def main():
    ensure_dirs()
    drive_service = init_drive_service()
    print("Picking images (avoid repeats)...")
    chosen_meta = pick_images_avoiding_repeats(drive_service, DRIVE_FOLDER_ID, NUM_IMAGES)
    # download images
    image_paths = []
    for i, meta in enumerate(chosen_meta, start=1):
        dest = IMAGES_DIR / f"img_{i}{Path(meta['name']).suffix}"
        print("Downloading", meta['name'])
        download_file_to_path(drive_service, meta["id"], str(dest))
        image_paths.append(str(dest))
    print("Building video...")
    build_video_from_images(image_paths, OUTPUT_VIDEO, per_image_sec=5)
    # pick music
    if DRIVE_MUSIC_FOLDER_ID:
        music_path = pick_random_music_and_download(drive_service, DRIVE_MUSIC_FOLDER_ID)
        if music_path:
            print("Mixing audio into video:", music_path)
            mix_audio_into_video(OUTPUT_VIDEO, music_path, OUTPUT_VIDEO_WITH_AUDIO)
            final_video = OUTPUT_VIDEO_WITH_AUDIO
        else:
            print("No music found; uploading video without audio.")
            final_video = OUTPUT_VIDEO
    else:
        print("DRIVE_MUSIC_FOLDER_ID not set; uploading video without audio.")
        final_video = OUTPUT_VIDEO

    # generate title & desc
    title, desc, tags = generate_title_and_desc_openai(chosen_meta)

    # sanitize description
    desc = strip_control_and_unsupported(desc)

    safe_title = make_safe_title(title, default="Auto Short")
    print("Title (raw):", title)
    print("Title (safe):", safe_title)
    print("Description:", desc)

    vid = youtube_upload(final_video, safe_title, desc, tags=tags, privacy="unlisted")
    print("Uploaded. Video ID:", vid)

if __name__ == "__main__":
    main()
