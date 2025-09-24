#!/usr/bin/env python3
"""
Minimal runnable skeleton for:
- downloading N random images from a Google Drive folder (service account)
- composing them into a vertical short using ffmpeg
- uploading the result to YouTube using OAuth refresh token

This script uses only free APIs and simple templated titles/descriptions (no paid LLM).
You will later add proper credentials as GitHub Secrets.
"""

import os
import io
import random
import time
import json
import subprocess
from pathlib import Path

# Google API imports
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request

# Configuration (via env)
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")  # JSON content of service account
NUM_IMAGES = int(os.environ.get("NUM_IMAGES", "6"))

YT_CLIENT_ID = os.environ.get("YT_CLIENT_ID")
YT_CLIENT_SECRET = os.environ.get("YT_CLIENT_SECRET")
YT_REFRESH_TOKEN = os.environ.get("YT_REFRESH_TOKEN")

TMP_DIR = Path("/tmp/auto_youtube")
IMAGES_DIR = TMP_DIR / "images"
OUTPUT_VIDEO = TMP_DIR / "output_short.mp4"

# Scopes
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
YT_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

def ensure_dirs():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    (IMAGES_DIR).mkdir(parents=True, exist_ok=True)

def init_drive_service():
    """
    Initialize Drive service using a service account JSON.
    IMPORTANT: share the Drive folder with the service account email (in the JSON 'client_email').
    """
    if not GCP_SA_KEY:
        raise RuntimeError("GCP_SA_KEY not set. Provide a service account JSON in repo secrets.")
    sa_json = json.loads(GCP_SA_KEY)
    sa_path = TMP_DIR / "sa.json"
    with open(sa_path, "w") as f:
        json.dump(sa_json, f)
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=DRIVE_SCOPES
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def list_images_in_folder(drive_service, folder_id):
    # List image files in the folder
    query = f"'{folder_id}' in parents and (mimeType contains 'image/') and trashed = false"
    files = []
    page_token = None
    while True:
        resp = drive_service.files().list(
            q=query,
            spaces='drive',
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
            pageSize=100
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken", None)
        if not page_token:
            break
    return files

def download_file(drive_service, file_id, dest_path):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()

def pick_and_download_images(drive_service, folder_id, n):
    files = list_images_in_folder(drive_service, folder_id)
    if not files:
        raise RuntimeError("No images found in the Drive folder. Make sure the service account has access.")
    chosen = random.sample(files, min(n, len(files)))
    paths = []
    # clear old images
    for f in IMAGES_DIR.glob("*"):
        f.unlink()
    for i, fmeta in enumerate(chosen, start=1):
        dest = IMAGES_DIR / f"img_{i}{Path(fmeta['name']).suffix}"
        print(f"Downloading {fmeta['name']} -> {dest}")
        download_file(drive_service, fmeta["id"], str(dest))
        paths.append(str(dest))
    return paths

def build_video_with_ffmpeg(image_paths, output_path, per_image_sec=5):
    """
    Build a vertical short using ffmpeg concat method.
    """
    list_txt = TMP_DIR / "list.txt"
    with open(list_txt, "w") as f:
        for p in image_paths:
            f.write(f"file '{p}'\n")
            f.write(f"duration {per_image_sec}\n")
        # ffmpeg concat requires last file to be repeated
        f.write(f"file '{image_paths[-1]}'\n")

    # ffmpeg command: scale images to 1080x1920 keeping aspect ratio, pad background
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(list_txt),
        "-vf",
        "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
        "-r", "30", "-c:v", "libx264", "-crf", "23", "-preset", "medium",
        str(output_path)
    ]
    print("Running ffmpeg to create video...")
    subprocess.check_call(cmd)
    print("Video created at", output_path)

def youtube_upload(video_path, title, description, tags=None, privacy="unlisted"):
    if not (YT_CLIENT_ID and YT_CLIENT_SECRET and YT_REFRESH_TOKEN):
        raise RuntimeError("YouTube OAuth credentials are missing in env (YT_CLIENT_ID, YT_CLIENT_SECRET, YT_REFRESH_TOKEN).")
    creds = OAuthCredentials(
        token=None,
        refresh_token=YT_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        scopes=YT_SCOPES
    )
    # Refresh access token
    creds.refresh(Request())

    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags or [],
            "categoryId": "22"  # People & Blogs (change if you want)
        },
        "status": {"privacyStatus": privacy}
    }

    media = MediaFileUpload(str(video_path), chunksize=-1, resumable=True, mimetype="video/mp4")
    req = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    print("Starting upload to YouTube...")
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status:
            print(f"Upload progress: {int(status.progress()*100)}%")
    print("Upload complete. Video ID:", response.get("id"))
    return response.get("id")

def generate_title_and_desc():
    ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    title = f"Auto Short — {ts}"
    description = "Auto-uploaded short video. #autopost #shorts"
    tags = ["shorts", "auto", "generated"]
    return title, description, tags

def main():
    ensure_dirs()
    print("Initializing Drive service...")
    drive_service = init_drive_service()
    print("Selecting and downloading images...")
    images = pick_and_download_images(drive_service, DRIVE_FOLDER_ID, NUM_IMAGES)
    build_video_with_ffmpeg(images, OUTPUT_VIDEO, per_image_sec=5)
    title, desc, tags = generate_title_and_desc()
    vid = youtube_upload(OUTPUT_VIDEO, title, desc, tags=tags, privacy="unlisted")
    print("Done. Uploaded video id:", vid)

if __name__ == "__main__":
    main()
