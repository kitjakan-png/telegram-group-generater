"""
Telegram Group Creator — Cloud Version
========================================
รันบน Google Cloud Run พร้อม Firebase Auth + Firestore

Environment Variables ที่ต้องตั้งใน Cloud Run:
  TG_API_ID     = 31069095
  TG_API_HASH   = 0d25832d9bbdefb403d2fd9a4ab37870
  TG_SESSION    = <session string จาก generate_session.py>
  FRONTEND_URL  = https://YOUR_PROJECT.web.app
"""

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

import firebase_admin
from firebase_admin import auth as fb_auth, credentials, firestore as fb_store
import uvicorn
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerFlood, UserPrivacyRestricted, ChatAdminRequired

# ── Firebase Admin ────────────────────────────────────────────────────────────
# ถ้ามี FIREBASE_SERVICE_ACCOUNT env var (JSON string) ใช้ credentials นั้น
# ถ้าไม่มี ใช้ Application Default Credentials (สำหรับ Cloud Run)
_sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
if _sa_json:
    _cred = credentials.Certificate(json.loads(_sa_json))
    firebase_admin.initialize_app(_cred)
else:
    firebase_admin.initialize_app()
db = fb_store.client()

# ── Telegram Config ───────────────────────────────────────────────────────────
API_ID     = int(os.environ.get("TG_API_ID", "31069095"))
API_HASH   = os.environ.get("TG_API_HASH", "0d25832d9bbdefb403d2fd9a4ab37870")
TG_SESSION = os.environ.get("TG_SESSION", "")  # StringSession string

FRONTEND_URL = os.environ.get("FRONTEND_URL", "*")

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:5000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

pyro_client: Client | None = None
jobs: dict[str, dict] = {}


# ── Auth Dependency ───────────────────────────────────────────────────────────

async def get_uid(authorization: str = Header(None)) -> str:
    """ตรวจสอบ Firebase ID Token แล้วคืน uid"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "กรุณา login ก่อนใช้งาน")
    token = authorization.split(" ", 1)[1]
    try:
        decoded = fb_auth.verify_id_token(token)
        return decoded["uid"]
    except Exception as e:
        raise HTTPException(401, f"Token ไม่ถูกต้อง: {e}")


# ── Pyrogram ──────────────────────────────────────────────────────────────────

async def get_client() -> Client:
    global pyro_client
    if pyro_client is None or not pyro_client.is_connected:
        if TG_SESSION:
            pyro_client = Client(
                name=":memory:",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=TG_SESSION,
            )
        else:
            # fallback ใช้ session file (รันในเครื่อง)
            pyro_client = Client("bulk_creator_session", api_id=API_ID, api_hash=API_HASH)
        await pyro_client.start()
    return pyro_client


async def safe_add(client: Client, chat_id: int, username: str) -> bool:
    try:
        await client.add_chat_members(chat_id, username.lstrip("@"))
        return True
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            await client.add_chat_members(chat_id, username.lstrip("@"))
            return True
        except Exception:
            return False
    except Exception:
        return False


# ── Job Runner ────────────────────────────────────────────────────────────────

async def run_job(job_id: str, uid: str, group_names: list[str], bot_username: str):
    job = jobs[job_id]
    job["status"] = "running"
    results = []

    def emit(msg: str, kind: str = "info"):
        job["events"].append({
            "type": kind,
            "message": msg,
            "time": datetime.now().strftime("%H:%M:%S"),
        })

    try:
        client = await get_client()

        for idx, name in enumerate(group_names):
            name = name.strip()
            if not name:
                continue
            emit(f"[{idx+1}/{len(group_names)}] กำลังสร้างกลุ่ม: {name}")

            try:
                chat    = await client.create_supergroup(name)
                chat_id = chat.id
                emit(f"✓ สร้าง '{name}' สำเร็จ  (ID: {chat_id})", "success")
                await asyncio.sleep(2)

                if bot_username.strip():
                    ok = await safe_add(client, chat_id, bot_username)
                    emit(
                        f"✓ เพิ่ม @{bot_username.lstrip('@')} สำเร็จ" if ok
                        else "⚠ เพิ่มบอทไม่สำเร็จ",
                        "success" if ok else "warning",
                    )
                await asyncio.sleep(1)

                link_obj    = await client.create_chat_invite_link(chat_id)
                invite_link = link_obj.invite_link
                emit(f"🔗 {invite_link}", "link")

                row = {
                    "name":        name,
                    "chat_id":     str(chat_id),
                    "invite_link": invite_link,
                    "bot_username": bot_username,
                    "status":      "success",
                    "created_at":  datetime.now(timezone.utc).isoformat(),
                }
                results.append(row)

                # ── บันทึกลง Firestore ──────────────────────────────────────
                db.collection("users").document(uid).collection("groups").add(row)

            except PeerFlood:
                emit("✗ PeerFlood — account ถูก limit หยุดรัน", "error")
                results.append({"name": name, "chat_id": "-", "invite_link": "-", "status": "error"})
                break

            except FloodWait as e:
                emit(f"⏳ FloodWait {e.value}s — รอ...", "warning")
                await asyncio.sleep(e.value)
                try:
                    chat    = await client.create_supergroup(name)
                    chat_id = chat.id
                    if bot_username.strip():
                        await safe_add(client, chat_id, bot_username)
                    link_obj = await client.create_chat_invite_link(chat_id)
                    row = {
                        "name": name, "chat_id": str(chat_id),
                        "invite_link": link_obj.invite_link,
                        "bot_username": bot_username, "status": "success",
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    results.append(row)
                    db.collection("users").document(uid).collection("groups").add(row)
                    emit(f"✓ '{name}' สำเร็จ (retry)", "success")
                except Exception as e2:
                    emit(f"✗ '{name}' ล้มเหลว: {e2}", "error")
                    results.append({"name": name, "chat_id": "-", "invite_link": "-", "status": "error"})

            except Exception as e:
                emit(f"✗ '{name}' ล้มเหลว: {e}", "error")
                results.append({"name": name, "chat_id": "-", "invite_link": "-", "status": "error"})

            if idx < len(group_names) - 1:
                emit("⏳ รอ 4 วินาที...")
                await asyncio.sleep(4)

        success = sum(1 for r in results if r["status"] == "success")
        job["status"]  = "done"
        job["results"] = results
        emit(f"🎉 เสร็จสิ้น! สำเร็จ {success}/{len(results)} กลุ่ม", "done")

        # บันทึก job summary ลง Firestore
        db.collection("users").document(uid).collection("jobs").add({
            "job_id":    job_id,
            "total":     len(results),
            "success":   success,
            "results":   results,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    except Exception as e:
        job["status"] = "error"
        job["events"].append({
            "type": "error",
            "message": f"Fatal: {e}",
            "time": datetime.now().strftime("%H:%M:%S"),
        })


# ── API Endpoints ─────────────────────────────────────────────────────────────

class CreateRequest(BaseModel):
    group_names: list[str]
    bot_username: str


@app.get("/api/status")
async def api_status(uid: str = Depends(get_uid)):
    try:
        client = await get_client()
        me = await client.get_me()
        return {"connected": True, "name": me.first_name, "username": me.username or ""}
    except Exception as e:
        return {"connected": False, "error": str(e)}


@app.post("/api/create")
async def api_create(req: CreateRequest, uid: str = Depends(get_uid)):
    names = [n.strip() for n in req.group_names if n.strip()]
    if not names:
        return JSONResponse({"error": "ไม่มีชื่อกลุ่ม"}, status_code=400)
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "pending", "results": [], "events": []}
    asyncio.create_task(run_job(job_id, uid, names, req.bot_username))
    return {"job_id": job_id}


@app.get("/api/progress/{job_id}")
async def api_progress(job_id: str, uid: str = Depends(get_uid)):
    async def stream() -> AsyncGenerator[str, None]:
        last = 0
        while True:
            if job_id not in jobs:
                break
            job    = jobs[job_id]
            events = job["events"]
            while last < len(events):
                yield f"data: {json.dumps(events[last])}\n\n"
                last += 1
            if job["status"] in ("done", "error"):
                yield f"data: {json.dumps({'type':'end','results':job['results']})}\n\n"
                break
            await asyncio.sleep(0.3)
    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/history")
async def api_history(uid: str = Depends(get_uid)):
    """ดึงประวัติกลุ่มที่สร้างไว้จาก Firestore"""
    docs = db.collection("users").document(uid).collection("groups")\
             .order_by("created_at", direction=fb_store.Query.DESCENDING)\
             .limit(100).stream()
    groups = [{"id": d.id, **d.to_dict()} for d in docs]
    return {"groups": groups}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("webapp_cloud:app", host="0.0.0.0", port=port, reload=False)
