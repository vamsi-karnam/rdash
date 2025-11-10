#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Author: Vamsi Karnam
# Description: R'DASH (Robot Information Telemetry Transport Dashboard) agent application
# License: Apache 2.0
# -----------------------------------------------------------------------------
import os, re, time, argparse, math, signal, threading, struct
from typing import Optional, Dict, Any, Tuple, List

import rclpy
from rclpy.node import Node
from rclpy.executors import SingleThreadedExecutor
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
from rosidl_runtime_py.convert import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message

from sensor_msgs.msg import CompressedImage, Image, PointCloud2
from tf2_msgs.msg import TFMessage
from std_msgs.msg import UInt8MultiArray, ByteMultiArray, String

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from cv_bridge import CvBridge
    import cv2
    CV_AVAILABLE = True
except Exception:
    CV_AVAILABLE = False

def flatten(obj, prefix=''):
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            flat.update(flatten(v, f"{prefix}{k}." if prefix else f"{k}."))  # noqa
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            flat.update(flatten(v, f"{prefix}{i}."))  # noqa
    else:
        if isinstance(obj, (int, float)) and not isinstance(obj, bool):
            key = prefix[:-1] if prefix.endswith('.') else prefix
            flat[key] = float(obj)
    return flat

def parse_unit_rules(rules: List[str]) -> List[Tuple[re.Pattern, str, float]]:
    out = []
    for r in rules or []:
        part = r.strip()
        if not part or '=' not in part or ':' not in part:
            continue
        lhs, rhs = part.split('=', 1)
        unit, scale_str = rhs.rsplit(':', 1)
        try:
            pat = re.compile(lhs)
            scale = float(scale_str)
            out.append((pat, unit.strip(), scale))
        except Exception:
            continue
    return out

def pc2_summarize(msg: PointCloud2, target_points=2048) -> Dict[str, float]:
    """
    Downsample and compute min/max/mean for float/double fields
    """
    try:
        total = msg.width * msg.height
        if msg.point_step <= 0 or total <= 0:
            return {'pc2.count': 0, 'pc2.sampled': 0}
        stride = max(1, total // target_points)
        fields = [(f.name, f.offset, f.datatype, f.count) for f in msg.fields]
        # ROS2 datatypes: 7=float32, 8=float64
        acc = {}
        for (name, off, dt, _c) in fields:
            if dt in (7, 8):
                acc[f'{name}.min'] = float('inf')
                acc[f'{name}.max'] = float('-inf')
                acc[f'{name}.sum'] = 0.0
        cnt = 0
        buf = msg.data
        for idx in range(0, total, stride):
            base = idx * msg.point_step
            for (name, off, dt, _c) in fields:
                if dt not in (7, 8): continue
                if dt == 7:
                    if base+off+4 > len(buf): continue
                    val = struct.unpack_from('<f', buf, base+off)[0]
                else:
                    if base+off+8 > len(buf): continue
                    val = struct.unpack_from('<d', buf, base+off)[0]
                if val < acc[f'{name}.min']: acc[f'{name}.min'] = val
                if val > acc[f'{name}.max']: acc[f'{name}.max'] = val
                acc[f'{name}.sum'] += val
            cnt += 1
        for (name, off, dt, _c) in fields:
            if dt in (7, 8) and f'{name}.sum' in acc:
                acc[f'{name}.mean'] = acc.pop(f'{name}.sum') / max(1, cnt)
        acc['pc2.count'] = total
        acc['pc2.sampled'] = cnt
        return acc
    except Exception:
        return {'pc2.count': msg.width * msg.height, 'pc2.sampled': 0}

class UrdafAgent(Node):
    def __init__(self, robot: str, include: Optional[str], exclude: Optional[str],
                 server: str, token: Optional[str], max_hz: float,
                 verify: Optional[str] | bool, numeric_qos: str,
                 ts_pref: str, unit_rules_raw: List[str], pc2_flag: bool, max_metrics_per_push: int = 32):
        super().__init__('ritt_agent')
        self.robot = robot
        self.inc = re.compile(include) if include else None
        self.exc = re.compile(exclude) if exclude else None
        self.server = server.rstrip('/')
        self.token = token
        self.ts_pref = ts_pref  # 'header' or 'receive'
        self.unit_rules = parse_unit_rules(unit_rules_raw)
        self.pc2_flag = pc2_flag
        self.max_metrics_per_push = max(0, max_metrics_per_push)

        # HTTP with retries
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.2,
                        status_forcelist=(429, 500, 502, 503, 504),
                        allowed_methods=frozenset(['GET', 'POST']))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.verify = verify
        self.headers = {'Authorization': f'Bearer {self.token}'} if self.token else {}

        # QoS profiles
        self.qos_num_best = QoSProfile(depth=10,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST)
        self.qos_num_rel = QoSProfile(depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
            history=HistoryPolicy.KEEP_LAST)
        self.qos_img = self.qos_num_best  # cameras typically BEST_EFFORT

        self.numeric_qos = numeric_qos  # 'best' / 'reliable' / 'dual'

        self.bridge = CvBridge() if CV_AVAILABLE else None
        self.subs = {}          # topic -> list of subs
        self.known = set()
        self.max_period = 1.0 / max_hz if max_hz > 0 else 0.0
        self.last_sent: Dict[Tuple[str, str], float] = {}

        # TF aggregation
        self.tf_edges = {}  # parent -> set(children)
        self.last_tf_push = 0.0

    def _allowed(self, topic: str) -> bool:
        if self.inc and not self.inc.search(topic): return False
        if self.exc and self.exc.search(topic): return False
        return True

    def _sensor_name(self, topic: str) -> str:
        parts = [p for p in topic.split('/') if p]
        return '/'.join(parts[1:]) if len(parts) > 1 else (parts[0] if parts else 'root')

    def _pick_time(self, msg) -> float:
        if self.ts_pref == 'header':
            try:
                hdr = getattr(msg, 'header', None)
                if hdr and hdr.stamp and (hdr.stamp.sec or hdr.stamp.nanosec):
                    return float(hdr.stamp.sec) + float(hdr.stamp.nanosec) * 1e-9
            except Exception:
                pass
        return time.time()

    def _apply_units(self, f: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, str]]:
        if not self.unit_rules:
            return f, {}
        out = {}
        units = {}
        for k, v in f.items():
            scaled = v
            for pat, unit, scale in self.unit_rules:
                if pat.search(k):
                    scaled = v * scale
                    units[k] = unit
                    break
            out[k] = scaled
        return out, units

    def discover(self):
        topic_types = self.get_topic_names_and_types()
        for topic, types in topic_types:
            if not types: continue
            if not self._allowed(topic): continue
            if topic in self.known: continue
            msg_type = types[0]
            try:
                pytype = get_message(msg_type)
            except Exception:
                continue

            subs = []
            if msg_type == 'sensor_msgs/msg/CompressedImage':
                subs.append(self.create_subscription(CompressedImage, topic, lambda m, t=topic, mt=msg_type: self.on_compressed(t, mt, m), self.qos_img)) # qos_img = qos_num_best
            elif msg_type == 'sensor_msgs/msg/Image':
                subs.append(self.create_subscription(Image, topic, lambda m, t=topic, mt=msg_type: self.on_raw_image(t, mt, m), self.qos_img))
            elif msg_type in ('std_msgs/msg/UInt8MultiArray', 'std_msgs/msg/ByteMultiArray'):
                # UI tiles hidden for now; endpoint kept
                subs.append(self.create_subscription(get_message(msg_type), topic, lambda m, t=topic, mt=msg_type: self.on_audio_bytes(t, mt, m), self.qos_img))
            elif msg_type in ('tf2_msgs/msg/TFMessage',):
                subs.append(self.create_subscription(TFMessage, topic, self.on_tf, self.qos_num_best))
            elif msg_type == 'sensor_msgs/msg/PointCloud2':
                subs.append(self.create_subscription(PointCloud2, topic, lambda m, t=topic, mt=msg_type: self.on_pc2(t, mt, m), self.qos_num_best))
            elif msg_type == 'std_msgs/msg/String':
                subs.append(self.create_subscription(String, topic, lambda m, t=topic, mt=msg_type: self.on_string(t, mt, m), self.qos_num_best))
            else:
                # numeric
                if self.numeric_qos == 'best':
                    subs.append(self.create_subscription(pytype, topic,
                        lambda m, t=topic, mt=msg_type: self.on_msg(t, mt, m), self.qos_num_best))
                elif self.numeric_qos == 'reliable':
                    subs.append(self.create_subscription(pytype, topic,
                        lambda m, t=topic, mt=msg_type: self.on_msg(t, mt, m), self.qos_num_rel))
                else:  # dual
                    subs.append(self.create_subscription(pytype, topic,
                        lambda m, t=topic, mt=msg_type: self.on_msg(t, mt, m), self.qos_num_best))
                    subs.append(self.create_subscription(pytype, topic,
                        lambda m, t=topic, mt=msg_type: self.on_msg(t, mt, m), self.qos_num_rel))

            self.subs[topic] = subs
            self.known.add(topic)
            self.get_logger().info(f"Subscribed: {topic} [{msg_type}] (numeric_qos={self.numeric_qos})")

        # TF ensure
        if '/tf' not in self.subs:
            self.subs['/tf'] = [self.create_subscription(TFMessage, '/tf', self.on_tf, self.qos_num_best)]
        if '/tf_static' not in self.subs:
            self.subs['/tf_static'] = [self.create_subscription(TFMessage, '/tf_static', self.on_tf, self.qos_num_best)]

    def throttle_ok(self, sensor: str) -> bool:
        if self.max_period <= 0: return True
        now = time.time()
        last = self.last_sent.get(('num', sensor), 0.0)
        if now - last >= self.max_period:
            self.last_sent[('num', sensor)] = now
            return True
        return False

    def _post_json(self, path: str, payload: Dict[str, Any], timeout=3):
        try:
            r = self.session.post(f"{self.server}{path}",
                                  json=payload, headers=self.headers, timeout=timeout,
                                  verify=self.verify)
            r.raise_for_status()
        except Exception:
            pass

    # Handlers
    def on_msg(self, topic: str, msg_type: str, msg: Any):
        d = message_to_ordereddict(msg)
        f = flatten(d)
        if not f: return
        sensor = self._sensor_name(topic)
        if not self.throttle_ok(sensor): return

        f_scaled, units = self._apply_units(f)

        # limit metrics per push to avoid choking frontend/WS ---- new
        if self.max_metrics_per_push and len(f_scaled) > self.max_metrics_per_push:
            keep = sorted(f_scaled.keys())[:self.max_metrics_per_push]
            f_scaled = {k: f_scaled[k] for k in keep}
            if isinstance(units, dict) and units:
                units = {k: units[k] for k in keep if k in units}
#            try:
#                self.get_logger().warn(
#                    f"Trimmed {len(f) - len(f_scaled)} metrics for sensor '{sensor}' "f"({len(f_scaled)} kept)"
#                )
#            except Exception:
#                pass
        # ---------------------------------------------------------------------- new

        payload = {
            "robot": self.robot,
            "sensor": sensor,
            "t": self._pick_time(msg),
            "data": f_scaled,
            "type": msg_type,
            "units": units or None
        }
        self._post_json("/api/push", payload)

    def on_string(self, topic: str, msg_type: str, msg: String):
        try:
            sensor = self._sensor_name(topic)
            ts = self._pick_time(msg)
            payload = {
                "robot": self.robot,
                "sensor": sensor,
                "t": ts,
                "text": msg.data,
                "type": msg_type
            }
            self._post_json("/api/push_text", payload)
        except Exception:
            pass

    def on_pc2(self, topic: str, msg_type: str, msg: PointCloud2):
        if not self.pc2_flag: return
        sensor = self._sensor_name(topic)
        if not self.throttle_ok(sensor): return
        stats = pc2_summarize(msg, target_points=2048)
        payload = {
            "robot": self.robot,
            "sensor": sensor,
            "t": self._pick_time(msg),
            "data": stats,
            "type": msg_type
        }
        self._post_json("/api/push", payload)

    def on_compressed(self, topic: str, msg_type: str, msg: CompressedImage):
        sensor = self._sensor_name(topic)
        files = {'image': ('frame.jpg', bytes(msg.data), 'image/jpeg')}
        data = {'robot': self.robot, 'sensor': sensor, 'type': msg_type}
        try:
            r = self.session.post(f"{self.server}/api/push_image",
                                  files=files, data=data, headers=self.headers, timeout=5,
                                  verify=self.verify)
            r.raise_for_status()
        except Exception:
            pass

    def on_raw_image(self, topic: str, msg_type: str, msg: Image):
        if not (CV_AVAILABLE and self.bridge): return
        try:
            cv_img = self.bridge.imgmsg_to_cv2(msg, desired_encoding='bgr8')
            ok, jpeg = cv2.imencode('.jpg', cv_img)
            if not ok: return
            sensor = self._sensor_name(topic)
            files = {'image': ('frame.jpg', jpeg.tobytes(), 'image/jpeg')}
            data = {'robot': self.robot, 'sensor': sensor, 'type': msg_type}
            r = self.session.post(f"{self.server}/api/push_image",
                                  files=files, data=data, headers=self.headers, timeout=5,
                                  verify=self.verify)
            r.raise_for_status()
        except Exception:
            pass

    def on_audio_bytes(self, topic, msg_type, msg):
        try:
            data_field = getattr(msg, 'data', None)
            if not data_field:
                return
            audio = bytes(data_field) if isinstance(data_field, (bytes, bytearray)) else bytes(bytearray(data_field))
            files = {'audio': ('clip.wav', audio, 'audio/wav')}
            data = {'robot': self.robot, 'sensor': self._sensor_name(topic), 'type': msg_type}
            r = self.session.post(f"{self.server}/api/push_audio",
                                  files=files, data=data, headers=self.headers, timeout=5,
                                  verify=self.verify)
            r.raise_for_status()
        except Exception:
            pass

    def on_tf(self, msg: TFMessage):
        try:
            for t in msg.transforms:
                parent = t.header.frame_id or ''
                child = t.child_frame_id or ''
                if not parent or not child: continue
                s = self.tf_edges.setdefault(parent, set())
                s.add(child)
            now = time.time()
            if now - self.last_tf_push > 2.0:
                edges = [(p, c) for p, cs in self.tf_edges.items() for c in cs]
                self._post_json("/api/push_tf", {"robot": self.robot, "edges": edges}, timeout=2)
                self.last_tf_push = now
        except Exception:
            pass

# Graceful exit ctrl+c
_stop_event = threading.Event()

def _install_signal_handlers():
    def _handle(sig, frame):
        try: _stop_event.set()
        except Exception: pass
    for s in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(s, _handle)
        except Exception:
            pass

def main():
    ap = argparse.ArgumentParser(description="RDASH agent (ROS 2 -> HTTPS/HTTP)")
    ap.add_argument("--server", required=True, help="Base URL, e.g. https://HOST:8443 or http://HOST:8080")
    ap.add_argument("--token", help="Bearer token")
    ap.add_argument("--robot-name", required=True, help="Name this robot (e.g., testsim-001)")
    ap.add_argument("--include", help="Regex include topics")
    ap.add_argument("--exclude", default="/rosout|/parameter_events", help="Regex exclude topics")
    ap.add_argument("--max-hz", type=float, default=10.0, help="Max numeric push rate per sensor")
    ap.add_argument("--insecure-tls", action="store_true", help="Skip TLS verify (testing only)")
    ap.add_argument("--ca-bundle", help="Path to a CA bundle for TLS verification")
    ap.add_argument("--numeric-qos", choices=["best","reliable","dual"], default="best",
                    help="QoS for numeric topics (default: best)")
    ap.add_argument("--timestamp", choices=["header","receive"], default="receive",
                    help="Prefer header.stamp or receive time for t field")
    ap.add_argument("--unit", action="append",
                    help='Tag unit+scale rules; repeatable. Format: "<regex>=<UNIT>:<scale>"')
    ap.add_argument("--pc2-summarize", action="store_true", help="Summarize PointCloud2 to stats (min/max/mean)")
    ap.add_argument("--max-metrics-per-push", type=int, default=32,
                help="Hard cap on number of numeric metrics per push (default: 32, 0 = unlimited)")
    args = ap.parse_args()

    verify = args.ca_bundle if args.ca_bundle else (False if args.insecure_tls else True)

    rclpy.init(args=None)
    node = UrdafAgent(args.robot_name, args.include, args.exclude,
                      args.server, args.token, args.max_hz, verify,
                      numeric_qos=args.numeric_qos,
                      ts_pref=args.timestamp, unit_rules_raw=args.unit or [],
                      pc2_flag=args.pc2_summarize, max_metrics_per_push=args.max_metrics_per_push)

    execu = SingleThreadedExecutor()
    execu.add_node(node)
    _install_signal_handlers()
    last_discover = 0.0
    try:
        while rclpy.ok() and not _stop_event.is_set():
            now = time.time()
            if now - last_discover > 2.0:
                try:
                    node.discover()
                except Exception:
                    pass
                last_discover = now
            execu.spin_once(timeout_sec=0.05)
    finally:
        try: node.destroy_node()
        except Exception: pass
        rclpy.shutdown()

if __name__ == "__main__":
    main()