"""Mafia LAN Pro - FastAPI + WebSocket server.

المعمارية: الهوست (جهاز المنظّم) هو من يدير منطق اللعبة، والسيرفر:
  - يحفظ آخر حالة للغرفة في الذاكرة (rooms_state)
  - يرسل SYNC_STATE فور انضمام أي عميل
  - يخفي الأدوار السرية عن اللاعبين (Redaction) قبل الإرسال
  - يمرر طلبات اللاعبين (PLAYER_JOIN_REQUEST / PLAYER_ACTION) إلى الهوست فقط
"""

import copy
import json
import re
import time
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

app = FastAPI(title="Mafia LAN Pro")

BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

MAX_MESSAGE_SIZE = 200_000  # بايت
ROOM_TTL_SECONDS = 6 * 60 * 60  # حذف الغرف المهجورة بعد 6 ساعات

# room_code -> { websocket: {"pid": str, "host": bool} }
rooms_ws: Dict[str, Dict[WebSocket, dict]] = {}
# room_code -> آخر حالة متزامنة للعبة
rooms_state: Dict[str, dict] = {}
# room_code -> معرّف الهوست
rooms_host: Dict[str, str] = {}
# room_code -> آخر نشاط
rooms_seen: Dict[str, float] = {}


# ----------------------------------------------------------------- مساعدات
def clean_pid(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "", raw or "")[:24]


def redact_for(state: Optional[dict], pid: str, is_host: bool) -> Optional[dict]:
    """نسخة من الحالة مناسبة لعميل معيّن (إخفاء أدوار الآخرين)."""
    if state is None:
        return None
    if is_host:
        return state
    s = copy.deepcopy(state)
    s.pop("_host", None)
    players = s.get("players") or []
    me = next((p for p in players if p.get("id") == pid), None)
    my_role = me.get("role") if me else None
    game_over = s.get("phase") == "gameover"
    for p in players:
        if game_over or p.get("id") == pid or not p.get("alive", True):
            continue
        if my_role == "mafia" and p.get("role") == "mafia":
            continue  # أفراد المافيا يعرفون بعضهم
        p["role"] = None
    notes = s.get("notes") or {}
    s["notes"] = {pid: notes.get(pid, [])}
    return s


async def safe_send(ws: WebSocket, data: dict) -> bool:
    try:
        await ws.send_text(json.dumps(data, ensure_ascii=False))
        return True
    except Exception:
        return False


async def broadcast_state(room: str) -> None:
    clients = rooms_ws.get(room)
    if not clients:
        return
    state = rooms_state.get(room)
    for ws, meta in list(clients.items()):
        ok = await safe_send(
            ws,
            {"type": "SYNC_STATE", "state": redact_for(state, meta["pid"], meta["host"])},
        )
        if not ok:
            clients.pop(ws, None)


def host_online(room: str) -> bool:
    return any(m["host"] for m in rooms_ws.get(room, {}).values())


async def broadcast_host_status(room: str) -> None:
    payload = {"type": "HOST_STATUS", "online": host_online(room)}
    for ws in list(rooms_ws.get(room, {}).keys()):
        await safe_send(ws, payload)


def purge_stale_rooms() -> None:
    now = time.time()
    for code in list(rooms_seen.keys()):
        if rooms_ws.get(code):
            continue
        if now - rooms_seen[code] > ROOM_TTL_SECONDS:
            rooms_seen.pop(code, None)
            rooms_state.pop(code, None)
            rooms_host.pop(code, None)
            rooms_ws.pop(code, None)


# ------------------------------------------------------------------ المسارات
@app.get("/")
async def index():
    return FileResponse(INDEX_FILE, media_type="text/html")


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "rooms": len(rooms_state)})


@app.websocket("/ws/{room_code}")
async def ws_endpoint(websocket: WebSocket, room_code: str):
    await websocket.accept()

    if not (room_code.isdigit() and len(room_code) == 4):
        await safe_send(websocket, {"type": "ERROR", "code": "BAD_ROOM"})
        await websocket.close(code=4400)
        return

    purge_stale_rooms()

    pid = clean_pid(websocket.query_params.get("pid", ""))
    if not pid:
        await safe_send(websocket, {"type": "ERROR", "code": "BAD_PID"})
        await websocket.close(code=4400)
        return
    wants_host = websocket.query_params.get("host") == "1"

    # تعارض كود الغرفة: غرفة موجودة لها هوست آخر
    if wants_host and room_code in rooms_host and rooms_host[room_code] != pid:
        if room_code in rooms_state:
            await safe_send(websocket, {"type": "ERROR", "code": "ROOM_TAKEN"})
            await websocket.close(code=4409)
            return
    if wants_host:
        rooms_host[room_code] = pid

    is_host = rooms_host.get(room_code) == pid and wants_host
    clients = rooms_ws.setdefault(room_code, {})
    # إغلاق اتصال قديم لنفس المعرّف (تبويب مكرر)
    for old_ws, meta in list(clients.items()):
        if meta["pid"] == pid:
            clients.pop(old_ws, None)
            try:
                await old_ws.close(code=4001)
            except Exception:
                pass
    clients[websocket] = {"pid": pid, "host": is_host}
    rooms_seen[room_code] = time.time()

    # أهم نقطة: إرسال آخر حالة فوراً لأي عميل جديد
    await safe_send(
        websocket,
        {"type": "SYNC_STATE", "state": redact_for(rooms_state.get(room_code), pid, is_host)},
    )
    await broadcast_host_status(room_code)

    try:
        while True:
            raw = await websocket.receive_text()
            rooms_seen[room_code] = time.time()
            if len(raw) > MAX_MESSAGE_SIZE:
                continue
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(msg, dict):
                continue

            mtype = msg.get("type")

            if mtype == "PING":
                await safe_send(websocket, {"type": "PONG"})

            elif mtype == "STATE_UPDATE" and is_host:
                state = msg.get("state")
                if isinstance(state, dict) and isinstance(state.get("players"), list):
                    rooms_state[room_code] = state
                    await broadcast_state(room_code)

            elif mtype in ("PLAYER_JOIN_REQUEST", "PLAYER_ACTION") and not is_host:
                msg["pid"] = pid  # لا نثق بالمعرّف القادم من العميل
                delivered = False
                for hws, meta in list(rooms_ws.get(room_code, {}).items()):
                    if meta["host"]:
                        delivered = await safe_send(hws, msg) or delivered
                if not delivered:
                    await safe_send(websocket, {"type": "HOST_STATUS", "online": False})

            elif mtype == "CLOSE_ROOM" and is_host:
                for ows in list(rooms_ws.get(room_code, {}).keys()):
                    if ows is not websocket:
                        await safe_send(ows, {"type": "ROOM_CLOSED"})
                rooms_state.pop(room_code, None)
                rooms_host.pop(room_code, None)
                rooms_seen.pop(room_code, None)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        room_clients = rooms_ws.get(room_code)
        if room_clients is not None:
            room_clients.pop(websocket, None)
            if not room_clients:
                rooms_ws.pop(room_code, None)
        rooms_seen[room_code] = time.time()
        if is_host:
            await broadcast_host_status(room_code)
