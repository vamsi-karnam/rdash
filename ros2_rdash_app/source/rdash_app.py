#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Author: Vamsi Karnam
# Description: RDash - Robot Information Telemetry Transport webserver application
# -----------------------------------------------------------------------------

import os, time, threading, argparse, base64, webbrowser
from pathlib import Path
from collections import defaultdict, deque
from typing import Dict, Tuple, Optional, Any, List

from flask import Flask, jsonify, request, render_template, Response, make_response
from flask_socketio import SocketIO
from werkzeug.middleware.proxy_fix import ProxyFix

DEFAULT_MAX_SAMPLES = int(os.environ.get("RDASH_MAX_SAMPLES", "2048"))
DEFAULT_MAX_IMAGE_BYTES = int(os.environ.get("RDASH_MAX_IMAGE_BYTES", str(8 * 1024 * 1024)))  # 8 MB
DEFAULT_MAX_AUDIO_BYTES = int(os.environ.get("RDASH_MAX_AUDIO_BYTES", str(16 * 1024 * 1024)))  # 16 MB
DEFAULT_MAX_TEXT_LINES = int(os.environ.get("RDASH_MAX_TEXT_LINES", "500"))

ACTIVE_SEC = 5.0      # <=5s since last update -> active
IDLE_SEC   = 60.0     # >5s and <=60s -> idle, else disconnected

# WS coalescer rate
WS_FLUSH_HZ = float(os.environ.get("RDASH_WS_FLUSH_HZ", "20"))

# WS coalescer buffer
_ws_buf_lock = threading.Lock()
_ws_buf: Dict[Tuple[str,str], Dict[str, Any]] = {}  # (robot,sensor) -> {"t": ts, "data": {...}}

def _ws_buffer(robot: str, sensor: str, ts: float, payload_num: Dict[str, float]):
    with _ws_buf_lock:
        key = (robot, sensor)
        rec = _ws_buf.get(key)
        if not rec:
            rec = {"t": ts, "data": {}}
            _ws_buf[key] = rec
        rec["t"] = ts
        data = rec["data"]
        for k, v in payload_num.items():
            data[k] = v

def _ws_flush_task(sio: SocketIO):
    interval = 1.0 / max(1.0, WS_FLUSH_HZ)
    while True:
        # cooperative sleep so eventlet/gevent servers don’t block
        sio.sleep(interval)
        batch = []
        with _ws_buf_lock:
            if _ws_buf:
                items = list(_ws_buf.items())
                _ws_buf.clear()
                batch = items
        for (robot, sensor), rec in batch:
            try:
                sio.emit('sensor_data',
                         {"robot": robot, "sensor": sensor, "t": rec["t"], "data": rec["data"]},
                         namespace="/ws")
            except Exception:
                pass

# In-memory persistence
class TimeSeriesStore:
    def __init__(self, maxlen: int):
        self.lock = threading.Lock()
        self.data: Dict[Tuple[str, str, str], deque] = defaultdict(lambda: deque(maxlen=maxlen))
        self.latest_jpeg: Dict[Tuple[str, str], bytes] = {}
        self.latest_audio: Dict[Tuple[str, str], Tuple[bytes, str]] = {}  # (bytes, mime)
        self.units: Dict[Tuple[str, str, str], str] = {}                  # (robot,sensor,metric)->unit
        self.types: Dict[Tuple[str, str], str] = {}                       # last msg type per (robot,sensor)
        self.last_ts: Dict[Tuple[str, str], float] = {}                   # last update per (robot,sensor)
        self.tf_edges: Dict[str, List[Tuple[str,str]]] = {}               # robot -> list of (parent,child)
        self.maxlen = maxlen

        # NEW: rolling text logs per (robot, sensor)
        self.texts: Dict[Tuple[str, str], deque] = defaultdict(
            lambda: deque(maxlen=DEFAULT_MAX_TEXT_LINES)
        )

    def _touch(self, robot: str, sensor: str, ts: Optional[float] = None):
        self.last_ts[(robot, sensor)] = ts if ts else time.time()

    def append_numeric(self, robot: str, sensor: str, payload: Dict[str, float], ts: float,
                       units: Optional[Dict[str,str]], typ: Optional[str]):
        with self.lock:
            for k, v in payload.items():
                self.data[(robot, sensor, k)].append((ts, float(v)))
                if units and k in units:
                    self.units[(robot, sensor, k)] = units[k]
            if typ:
                self.types[(robot, sensor)] = typ
            self._touch(robot, sensor, ts)

    def append_text(self, robot: str, sensor: str, ts: float, text: str, level: Optional[str], typ: Optional[str]):
        with self.lock:
            self.texts[(robot, sensor)].append((ts, text, level or None))
            if typ:
                self.types[(robot, sensor)] = typ
            self._touch(robot, sensor, ts)

    def get_texts(self, robot: str, sensor: str) -> List[Tuple[float, str, Optional[str]]]:
        with self.lock:
            dq = self.texts.get((robot, sensor))
            return list(dq) if dq else []

    def clear_series(self, robot: str, sensor: str):
        with self.lock:
            keys = [key for key in self.data.keys() if key[0] == robot and key[1] == sensor]
            for key in keys:
                self.data.pop(key, None)
            # units/types/last_ts left intact; they’ll refresh on next push

    def clear_text(self, robot: str, sensor: str):
        with self.lock:
            self.texts.pop((robot, sensor), None)
            # types/last_ts left intact

    def set_jpeg(self, robot: str, sensor: str, jpg: bytes, typ: Optional[str]):
        with self.lock:
            self.latest_jpeg[(robot, sensor)] = jpg
            if typ: self.types[(robot, sensor)] = typ
            self._touch(robot, sensor)

    def get_latest_jpeg(self, robot: str, sensor: str) -> Optional[bytes]:
        with self.lock:
            return self.latest_jpeg.get((robot, sensor))

    def set_audio(self, robot: str, sensor: str, audio: bytes, mime: str, typ: Optional[str]):
        with self.lock:
            self.latest_audio[(robot, sensor)] = (audio, mime)
            if typ: self.types[(robot, sensor)] = typ
            self._touch(robot, sensor)

    def get_latest_audio(self, robot: str, sensor: str) -> Optional[Tuple[bytes, str]]:
        with self.lock:
            return self.latest_audio.get((robot, sensor))

    def set_tf_edges(self, robot: str, edges: List[Tuple[str,str]]):
        with self.lock:
            self.tf_edges[robot] = edges

    def snapshot(self, robot: Optional[str] = None, sensor: Optional[str] = None):
        with self.lock:
            out: Dict[str, Any] = {}
            for (r, s, k), dq in self.data.items():
                if robot and r != robot: continue
                if sensor and s != sensor: continue
                out.setdefault(r, {}).setdefault(s, {})[k] = list(dq)
            return out

    def list_robots(self):
        with self.lock:
            robots: Dict[str, Dict[str, set]] = {}
            for (r, s, _), _dq in self.data.items():
                robots.setdefault(r, {"sensors": set(), "cameras": set(), "audios": set(), "texts": set()})
                robots[r]["sensors"].add(s)
            for (r, s) in self.latest_jpeg.keys():
                robots.setdefault(r, {"sensors": set(), "cameras": set(), "audios": set(), "texts": set()})
                robots[r]["cameras"].add(s)
            for (r, s) in self.latest_audio.keys():
                robots.setdefault(r, {"sensors": set(), "cameras": set(), "audios": set(), "texts": set()})
                robots[r]["audios"].add(s)
            for (r, s) in self.texts.keys():
                robots.setdefault(r, {"sensors": set(), "cameras": set(), "audios": set(), "texts": set()})
                robots[r]["texts"].add(s)
            return {
                r: {
                    "sensors": sorted(list(v["sensors"])),
                    "cameras": sorted(list(v["cameras"])),
                    "audios":  sorted(list(v["audios"])),
                    "texts":   sorted(list(v["texts"])),
                }
                for r, v in robots.items()
            }

    def get_units_map(self, robot: str, sensor: str) -> Dict[str, str]:
        with self.lock:
            out = {}
            for (r,s,k), unit in self.units.items():
                if r == robot and s == sensor:
                    out[k] = unit
            return out

    def get_types_for_robot(self, robot: str) -> Dict[str, str]:
        with self.lock:
            out = {}
            for (r,s), typ in self.types.items():
                if r == robot:
                    out[s] = typ
            return out

    def get_status_for_robot(self, robot: str) -> Dict[str, float]:
        with self.lock:
            return { s: self.last_ts[(r, s)] for (r,s), _ in self.last_ts.items() if r == robot }

    def get_tf(self, robot: str) -> Dict[str, Any]:
        with self.lock:
            edges = self.tf_edges.get(robot, [])
            return {"edges": edges}

# App stuff
def find_tls_in_certs_folder() -> Tuple[Optional[str], Optional[str]]:
    cdir = Path(__file__).resolve().parent / "certs"
    if not cdir.exists():
        return None, None
    for cert in sorted(cdir.glob("*.pem")):
        key_candidate = cdir / (cert.stem + "-key.pem")
        if key_candidate.exists():
            return str(cert), str(key_candidate)
    cert, key = cdir / "cert.pem", cdir / "key.pem"
    if cert.exists() and key.exists():
        return str(cert), str(key)
    return None, None

def create_app(max_samples: int, max_image_bytes: int, message_queue: Optional[str]):
    base_dir = Path(__file__).resolve().parent
    template_dir = base_dir / "templates"
    static_dir = base_dir / "static"

    app = Flask(__name__, template_folder=str(template_dir), static_folder=str(static_dir), static_url_path="/static")
    app.config["MAX_CONTENT_LENGTH"] = max_image_bytes + DEFAULT_MAX_AUDIO_BYTES
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    socketio = SocketIO(
        app,
        cors_allowed_origins="*",
        async_mode=None,  # allow eventlet in production; our tasks use sio.sleep
        logger=False,
        engineio_logger=False,
        message_queue=message_queue,
        ping_timeout=20,
        ping_interval=25,
    )

    store = TimeSeriesStore(maxlen=max_samples)

    # Start WS coalescer (non-blocking)
    socketio.start_background_task(_ws_flush_task, socketio)

    return app, socketio, store

# globals probably, too tired to remember
app: Flask = None  # type: ignore
socketio: SocketIO = None  # type: ignore
store: TimeSeriesStore = None  # type: ignore
_auth_token: Optional[str] = None
_https_enabled: bool = False

# Auth helpers
def _authorized() -> bool:
    global _auth_token
    if not _auth_token:
        return True
    hdr = request.headers.get('Authorization', '')
    if hdr.startswith('Bearer ') and hdr.split(' ', 1)[1] == _auth_token:
        return True
    ck = request.cookies.get('urdaf_token')
    if ck and ck == _auth_token:
        return True
    q = request.args.get('token')
    if q and q == _auth_token:
        return True
    return False

def _status_from_ts(ts: Optional[float]) -> str:
    if not ts: return "disconnected"
    age = time.time() - ts
    if age <= ACTIVE_SEC: return "active"
    if age <= IDLE_SEC: return "idle"
    return "disconnected"

# http handlers
def register_http_handlers(app: Flask, sio: SocketIO, store: TimeSeriesStore):
    @app.after_request
    def add_security_headers(resp):
        if request.path.startswith("/api/"):
            resp.headers["Cache-Control"] = "no-store"
        if _https_enabled:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        resp.headers["X-URDAF-Auth"] = "required" if _auth_token else "none"
        resp.headers["Access-Control-Expose-Headers"] = "X-URDAF-Auth"
        return resp

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/api/config")
    def api_config():
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        return jsonify({
            "requires_auth": bool(_auth_token),
            "max_samples": store.maxlen,
            "https": _https_enabled,
            "sqlite": False  # always false; no persistence
        })

    @app.route("/api/robots")
    def api_robots():
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        robots = store.list_robots()
        statuses = {}
        for r, meta in robots.items():
            last_list = []
            for s in meta["sensors"]:
                last_list.append(store.last_ts.get((r, s)))
            for c in meta["cameras"]:
                last_list.append(store.last_ts.get((r, c)))
            for a in meta["audios"]:
                last_list.append(store.last_ts.get((r, a)))
            for t in meta.get("texts", []):
                last_list.append(store.last_ts.get((r, t)))
            ts = max([t for t in last_list if t is not None], default=None)
            statuses[r] = _status_from_ts(ts)
        return jsonify({"robots": robots, "status": statuses, "ts": time.time()})

    @app.route("/api/history/<robot>/<path:sensor>")
    def api_history(robot, sensor):
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        snap = store.snapshot(robot=robot, sensor=sensor)
        return jsonify(snap.get(robot, {}).get(sensor, {}))

    # note: /api/history_range was persistent in previous version with sqlite db, do not add in!!.

    # units/types/status/tf
    @app.route("/api/meta/<robot>/<path:sensor>")
    def api_meta(robot, sensor):
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        units = store.get_units_map(robot, sensor)
        typ = store.get_types_for_robot(robot).get(sensor)
        last = store.get_status_for_robot(robot).get(sensor)
        return jsonify({"units": units, "type": typ, "last": last, "status": _status_from_ts(last)})

    @app.route("/api/types/<robot>")
    def api_types(robot):
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        return jsonify(store.get_types_for_robot(robot))

    @app.route("/api/status/<robot>")
    def api_status(robot):
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        m = store.get_status_for_robot(robot)
        return jsonify({ s: {"last": m.get(s), "status": _status_from_ts(m.get(s))} for s in m })

    @app.route("/api/tf/<robot>")
    def api_tf(robot):
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        return jsonify(store.get_tf(robot))

    # PUSH numeric
    @app.route("/api/push", methods=["POST"])
    def api_push():
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        data = request.get_json(silent=True) or {}
        robot = str(data.get("robot","")).strip() or "default"
        sensor = str(data.get("sensor","")).strip() or "root"
        ts = float(data.get("t", time.time()))
        payload = data.get("data", {})
        units = data.get("units") or None
        typ = data.get("type") or None
        payload_num = {k: float(v) for k,v in payload.items() if isinstance(v,(int,float))}
        if payload_num:
            store.append_numeric(robot, sensor, payload_num, ts, units, typ)
            _ws_buffer(robot, sensor, ts, payload_num)  # coalesce for WS
        return jsonify({"ok": True})

    # MEDIA PUSH: IMAGE
    @app.route("/api/push_image", methods=["POST"])
    def api_push_image():
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        ct = request.content_type or ""
        robot = request.form.get("robot") or request.args.get("robot") or "default"
        sensor = request.form.get("sensor") or request.args.get("sensor") or "camera"
        typ = request.form.get("type") or request.args.get("type")
        try:
            if "multipart/form-data" in ct:
                file = request.files.get("image")
                if not file:
                    return jsonify({"error":"no file"}), 400
                jpg = file.read()
                if len(jpg) > DEFAULT_MAX_IMAGE_BYTES:
                    return jsonify({"error":"image too large"}), 413
                store.set_jpeg(robot, sensor, jpg, typ)
                return jsonify({"ok": True})
            else:
                data = request.get_json(silent=True) or {}
                b64 = data.get("image_b64")
                if not b64:
                    return jsonify({"error":"no image_b64"}), 400
                jpg = base64.b64decode(b64)
                if len(jpg) > DEFAULT_MAX_IMAGE_BYTES:
                    return jsonify({"error":"image too large"}), 413
                if data.get("type"): typ = data.get("type")
                store.set_jpeg(robot, sensor, jpg, typ)
                return jsonify({"ok": True})
        except Exception:
            return jsonify({"error":"bad image payload"}), 400

    # MEDIA PUSH: AUDIO (retained for API compatibility)
    @app.route("/api/push_audio", methods=["POST"])
    def api_push_audio():
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        robot = request.form.get("robot") or request.args.get("robot") or "default"
        sensor = request.form.get("sensor") or request.args.get("sensor") or "audio"
        typ = request.form.get("type") or request.args.get("type")
        try:
            if request.content_type and "multipart/form-data" in request.content_type:
                file = request.files.get("audio")
                if not file:
                    return jsonify({"error":"no file"}), 400
                audio = file.read()
                mime = file.mimetype or "application/octet-stream"
                if not mime.startswith("audio/"):
                    return jsonify({"error":"bad mime"}), 415
                if len(audio) > DEFAULT_MAX_AUDIO_BYTES:
                    return jsonify({"error":"audio too large"}), 413
                store.set_audio(robot, sensor, audio, mime, typ)
                return jsonify({"ok": True})
            else:
                data = request.get_json(silent=True) or {}
                b64 = data.get("audio_b64")
                mime = (data.get("mime") or "").strip() or "audio/ogg"
                if not b64:
                    return jsonify({"error":"no audio_b64"}), 400
                if not mime.startswith("audio/"):
                    return jsonify({"error":"bad mime"}), 415
                audio = base64.b64decode(b64)
                if len(audio) > DEFAULT_MAX_AUDIO_BYTES:
                    return jsonify({"error":"audio too large"}), 413
                if data.get("type"): typ = data.get("type")
                store.set_audio(robot, sensor, audio, mime, typ)
                return jsonify({"ok": True})
        except Exception:
            return jsonify({"error":"bad audio payload"}), 400

    # PUSH: TF 
    @app.route("/api/push_tf", methods=["POST"])
    def api_push_tf():
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        data = request.get_json(silent=True) or {}
        robot = str(data.get("robot","")).strip() or "default"
        edges = data.get("edges") or []
        try:
            edges = [(str(a), str(b)) for a,b in edges]
        except Exception:
            edges = []
        store.set_tf_edges(robot, edges)
        return jsonify({"ok": True})
    
    # VIDEO STREAM
    @app.route("/video/<robot>/<path:sensor>")
    def video(robot, sensor):
        if not _authorized():
            return Response("unauthorized", status=401)
        wait = request.args.get("wait", "1")
        if wait == "0":
            if store.get_latest_jpeg(robot, sensor) is None:
                return Response("no video", status=404)
        def gen():
            while True:
                jpg = store.get_latest_jpeg(robot, sensor)
                if jpg:
                    yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + jpg + b'\r\n')
                socketio.sleep(0.05)
        resp = Response(gen(), mimetype='multipart/x-mixed-replace; boundary=frame')
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    # PUSH: text
    @app.route("/api/push_text", methods=["POST"])
    def api_push_text():
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        try:
            data = request.get_json(silent=True) or {}
            robot = str(data.get("robot","")).strip() or "default"
            sensor = str(data.get("sensor","")).strip() or "log"
            ts = float(data.get("t", time.time()))
            text = str(data.get("text",""))
            level = data.get("level")
            typ = data.get("type") or None

            if text:
                store.append_text(robot, sensor, ts, text, level, typ)
                try:
                    socketio.emit('text_data',
                             {"robot": robot, "sensor": sensor, "t": ts, "text": text, "level": level},
                             namespace="/ws")
                except Exception:
                    pass
            return jsonify({"ok": True})
        except Exception:
            return jsonify({"error":"bad text payload"}), 400

    # TEXT HISTORY 
    @app.route("/api/text_history/<robot>/<path:sensor>")
    def api_text_history(robot, sensor):
        if not _authorized():
            return jsonify({"error":"unauthorized"}), 401
        lines = store.get_texts(robot, sensor)
        out = [{"t": t, "text": txt, "level": lvl} for (t, txt, lvl) in lines]
        return jsonify(out)

    # Delete series (RAM only)
    @app.route("/api/delete_series/<robot>/<path:sensor>", methods=["POST"])
    def api_delete_series(robot, sensor):
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        store.clear_series(robot, sensor)
        return jsonify({"ok": True})

    # Delete text (RAM only)
    @app.route("/api/delete_text/<robot>/<path:sensor>", methods=["POST"])
    def api_delete_text(robot, sensor):
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        store.clear_text(robot, sensor)
        return jsonify({"ok": True})

    @socketio.on("connect", namespace="/ws")
    def ws_connect(auth=None):
        if not _auth_token:
            return True
        if isinstance(auth, dict) and auth.get("token") == _auth_token:
            return True
        if request.cookies.get("urdaf_token") == _auth_token:
            return True
        if request.args.get("token") == _auth_token:
            return True
        hdr = request.headers.get("Authorization", "")
        if hdr.startswith("Bearer ") and hdr.split(" ", 1)[1] == _auth_token:
            return True
        try:
            app.logger.warning(
                "WS auth failed (auth:%s cookie:%s qs:%s hdr:%s)",
                isinstance(auth, dict),
                bool(request.cookies.get("urdaf_token")),
                bool(request.args.get("token")),
                bool(hdr),
            )
        except Exception:
            pass
        return False

    # Graceful shutdown 
    @app.route("/api/shutdown", methods=["POST"])
    def api_shutdown():
        if not _authorized():
            return jsonify({"error": "unauthorized"}), 401
        werk_shutdown = request.environ.get("werkzeug.server.shutdown")
        def stopper():
            time.sleep(0.2)
            try:
                if werk_shutdown:
                    werk_shutdown(); return
                try: socketio.stop(); return
                except Exception: pass
            finally:
                os._exit(0)
        print("[URDAF] Shutdown requested via /api/shutdown")
        threading.Thread(target=stopper, daemon=True).start()
        return jsonify({"ok": True, "message": "Shutting down…"})

# Run
def _open_browser_later(url: str, delay: float = 1.5):
    def _t():
        time.sleep(delay)
        try: webbrowser.open(url)
        except Exception: pass
    threading.Thread(target=_t, daemon=True).start()

def main():
    global app, socketio, store, _auth_token, _https_enabled
    p = argparse.ArgumentParser(description="URDAF server (robots -> HTTPS/HTTP POST, browser <- live)")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8443)
    p.add_argument("--auth-token", help="Bearer token required by API/WebSocket")
    p.add_argument("--cert", help="Path to TLS cert.pem (optional)")
    p.add_argument("--key", help="Path to TLS key.pem (optional)")
    p.add_argument("--max-samples", type=int, default=DEFAULT_MAX_SAMPLES)
    p.add_argument("--max-image-bytes", type=int, default=DEFAULT_MAX_IMAGE_BYTES)
    p.add_argument("--message-queue", help="Optional message queue (e.g., redis://localhost:6379/0)")
    p.add_argument("--no-browser", action="store_true", help="Disable browser auto-open")
    args = p.parse_args()

    _auth_token = args.auth_token

    app, socketio, store = create_app(
        max_samples=args.max_samples,
        max_image_bytes=args.max_image_bytes,
        message_queue=args.message_queue,
    )
    register_http_handlers(app, socketio, store)

    cert, key = args.cert, args.key
    if not (cert and key):
        cert, key = find_tls_in_certs_folder()

    if cert and key:
        _https_enabled = True
        scheme = "https"
        port = args.port
        url = f"{scheme}://{'localhost' if args.host in ('0.0.0.0','::') else args.host}:{port}"
        if not args.no_browser: _open_browser_later(url)
        socketio.run(app, host=args.host, port=port, ssl_context=(cert, key))
    else:
        _https_enabled = False
        port = 8080 if args.port == 8443 else args.port
        scheme = "http"
        url = f"{scheme}://{'localhost' if args.host in ('0.0.0.0','::') else args.host}:{port}"
        if not args.no_browser: _open_browser_later(url)
        socketio.run(app, host=args.host, port=port)

if __name__ == "__main__":
    main()
