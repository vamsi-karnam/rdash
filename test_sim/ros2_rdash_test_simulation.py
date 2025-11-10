#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Author: Vamsi Karnam
# Description: Dummy robot ROS2 node simulator for smoke test
# -----------------------------------------------------------------------------
"""
Agnostic, high-coverage ROS 2 demo publisher for RDash

Robot details:
  - Numerics (10+): /robot/speed_mps, /robot/altitude_m, /robot/battery_mv,
    /robot/temperature_c, /robot/voltage_v, /robot/current_a, /robot/power_w,
    /robot/yaw_rate_dps, /robot/heading_deg, /robot/mem_used_mb, /robot/pressure_kpa
  - Text:            /robot/log  (INFO/WARN/ERROR cycling)
  - Camera (raw):    /robot/camera/image            (frames from ./test_frames/*)
  - Camera (jpeg):   /robot/camera/image/compressed (frames from ./test_frames/*)
  - LiDAR LaserScan: /robot/lidar/scan
  - LiDAR PointCloud2: /robot/lidar/points  (for --pc2-summarize)
  - TF:
      /tf_static: map->odom, base_link->camera_link, base_link->lidar_link, base_link->imu_link
      /tf:        odom->base_link (moving), camera_link slight pitch oscillation

DDS details:
  - Numeric: /foo/dds/network/latency_ms, /bar/dds/cpu/percent
  - Text:    /baz/dds/diagnostics, /baz/dds/system/diagnostics

Agnostic rule: any topic path that contains a `dds` segment appears in the DDS drawer.
"""

import math
import time
import random
import socket
import argparse
import os
import glob

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy

from std_msgs.msg import Float32, String
from sensor_msgs.msg import Image, CompressedImage, LaserScan, PointCloud2, PointField
from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage

# Optional deps (images/point clouds)
CV2 = None
NP = None
try:
    from cv_bridge import CvBridge
    BRIDGE = CvBridge()
except Exception:
    BRIDGE = None
try:
    import numpy as np
    NP = np
    import cv2
    CV2 = cv2
except Exception:
    CV2 = None


# ---------------- QoS helpers ----------------
def qos_profiles(reliable: bool):
    reliab = ReliabilityPolicy.RELIABLE if reliable else ReliabilityPolicy.BEST_EFFORT
    return QoSProfile(
        depth=10,
        reliability=reliab,
        durability=DurabilityPolicy.VOLATILE,
        history=HistoryPolicy.KEEP_LAST,
    )

def qos_tf_static():
    # TF_STATIC is usually TRANSIENT_LOCAL so late subscribers get it
    return QoSProfile(
        depth=1,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
        history=HistoryPolicy.KEEP_LAST,
    )


# ---------------- math helpers ----------------
def moving_bar(h, w, t):
    """Simple synthetic image (fallback if no test_frames found)."""
    if NP is None:
        return None
    img = NP.zeros((h, w, 3), dtype=NP.uint8)
    x = int((math.sin(t) * 0.5 + 0.5) * (w - 3))
    img[:, x:x+3, :] = 255
    return img

def make_pc2(points_xyz, frame_id="lidar"):
    if NP is None:
        return None
    assert points_xyz.ndim == 2 and points_xyz.shape[1] == 3
    msg = PointCloud2()
    msg.header.frame_id = frame_id
    msg.height = 1
    msg.width = points_xyz.shape[0]
    msg.is_bigendian = False
    msg.is_dense = True
    msg.point_step = 12  # 3 * float32
    msg.row_step = msg.point_step * msg.width
    msg.fields = [
        PointField(name='x', offset=0,  datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4,  datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8,  datatype=PointField.FLOAT32, count=1),
    ]
    msg.data = points_xyz.astype(NP.float32).tobytes()
    return msg

def quat_from_rpy(roll, pitch, yaw):
    """Return (x,y,z,w) quaternion from Euler angles (radians)."""
    cr = math.cos(roll * 0.5)
    sr = math.sin(roll * 0.5)
    cp = math.cos(pitch * 0.5)
    sp = math.sin(pitch * 0.5)
    cy = math.cos(yaw * 0.5)
    sy = math.sin(yaw * 0.5)
    qw = cr*cp*cy + sr*sp*sy
    qx = sr*cp*cy - cr*sp*sy
    qy = cr*sp*cy + sr*cp*sy
    qz = cr*cp*sy - sr*sp*cy
    return (qx, qy, qz, qw)


class AgnosticDemo(Node):
    def __init__(self, args):
        super().__init__('agnostic_demo')
        self.args = args
        qos_rel = qos_profiles(args.reliable)

        # ---------- camera frames source (from ./test_frames) ----------
        self._frame_paths = []
        self._frame_idx = 0
        self._last_bgr = None
        self._load_frame_list()

        # ---------- Regular numerics (main robot page) ----------
        self.pub_speed       = self._maybe_pub(args.enable_numeric,   Float32, '/robot/speed_mps', qos_rel)
        self.pub_altitude    = self._maybe_pub(args.enable_numeric,   Float32, '/robot/altitude_m', qos_rel)
        self.pub_batt_mv     = self._maybe_pub(args.enable_numeric,   Float32, '/robot/battery_mv', qos_rel)
        self.pub_temp_c      = self._maybe_pub(args.enable_numeric,   Float32, '/robot/temperature_c', qos_rel)
        self.pub_voltage_v   = self._maybe_pub(args.enable_numeric,   Float32, '/robot/voltage_v', qos_rel)
        self.pub_current_a   = self._maybe_pub(args.enable_numeric,   Float32, '/robot/current_a', qos_rel)
        self.pub_power_w     = self._maybe_pub(args.enable_numeric,   Float32, '/robot/power_w', qos_rel)
        self.pub_yaw_rate    = self._maybe_pub(args.enable_numeric,   Float32, '/robot/yaw_rate_dps', qos_rel)
        self.pub_heading_deg = self._maybe_pub(args.enable_numeric,   Float32, '/robot/heading_deg', qos_rel)
        self.pub_mem_mb      = self._maybe_pub(args.enable_numeric,   Float32, '/robot/mem_used_mb', qos_rel)
        self.pub_press_kpa   = self._maybe_pub(args.enable_numeric,   Float32, '/robot/pressure_kpa', qos_rel)

        # regular text
        self.pub_log = self._maybe_pub(args.enable_text, String, '/robot/log', qos_rel)
        self.pub_debug = self.create_publisher(String, '/sensor/debug_log', qos_rel)
        self.create_timer(2.0, self.tick_debug)

        # cameras
        self.camera_frame = 'camera_link'
        self.pub_img  = self._maybe_pub(args.enable_image and BRIDGE is not None, Image, '/robot/camera/image', qos_rel)
        self.pub_imgc = self._maybe_pub(args.enable_compressed and CV2 is not None, CompressedImage, '/robot/camera/image/compressed', qos_rel)

        # LiDAR: LaserScan + PointCloud2
        self.lidar_frame = 'lidar_link'
        self.pub_scan = self._maybe_pub(args.enable_lidar, LaserScan, '/robot/lidar/scan', qos_rel)
        self.pub_pc2  = self._maybe_pub(args.enable_lidar and NP is not None, PointCloud2, '/robot/lidar/points', qos_rel)

        # ---------- TF (static + dynamic) ----------
        self.pub_tf        = self.create_publisher(TFMessage, '/tf', qos_rel)
        self.pub_tf_static = self.create_publisher(TFMessage, '/tf_static', qos_tf_static())

        # Send static TF once
        self._publish_static_tf()

        # Timers for dynamic TF (odom->base_link @ 10 Hz; camera micro tilt @ 5 Hz)
        self.create_timer(0.1, self._tick_tf_base)     # 10 Hz
        self.create_timer(0.2, self._tick_tf_camera)   # 5 Hz

        # ---------- Timers ----------
        if self.pub_speed:
            self._spd = 0.0
            self.create_timer(1.0 / args.numeric_hz, self.tick_speed)

        if args.enable_numeric:
            self.create_timer(1.0 / args.numeric_hz, self.tick_other_numerics)

        if self.pub_log:
            self._level_cycle = iter(['INFO', 'WARN', 'ERROR'])
            self.create_timer(args.text_period, self.tick_log)

        # cameras (higher fps defaults)
        if self.pub_img:
            self._h, self._w = args.raw_h, args.raw_w
            self.create_timer(1.0 / args.image_hz, self.tick_raw_image)
        if self.pub_imgc:
            self._hc, self._wc = args.jpeg_h, args.jpeg_w
            self.create_timer(1.0 / args.compressed_hz, self.tick_compressed_image)

        # LiDAR
        if self.pub_scan:
            self.create_timer(1.0 / args.lidar_hz, self.tick_scan)
        if self.pub_pc2:
            self.create_timer(1.0 / args.pc2_hz, self.tick_pc2)

        # DDS numeric
        self.pub_dds_latency = self._maybe_pub(
            args.enable_dds_numeric, Float32, '/foo/dds/network/latency_ms', qos_rel
        )
        if self.pub_dds_latency:
            self.create_timer(args.dds_latency_period, self.tick_dds_latency)

        self.pub_dds_cpu = self._maybe_pub(
            args.enable_dds_numeric, Float32, '/bar/dds/cpu/percent', qos_rel
        )
        if self.pub_dds_cpu:
            self.create_timer(args.dds_cpu_period, self.tick_dds_cpu)

        # DDS text
        self.pub_dds_diag = self._maybe_pub(
            args.enable_dds_text, String, '/baz/dds/diagnostics', qos_rel
        )
        self.pub_dds_diag2 = self._maybe_pub(
            args.enable_dds_text, String, '/baz/dds/system/diagnostics', qos_rel
        )
        if self.pub_dds_diag or self.pub_dds_diag2:
            self.create_timer(args.dds_text_period, self.tick_dds_texts)

        # bursts on speed to stress charts
        if self.pub_speed and args.enable_bursts:
            self.create_timer(args.burst_every, self.tick_burst)

        self.get_logger().info("Agnostic demo ready. Put frames in ./test_frames/ to play as video. Ctrl+C to stop.")

    # ---------- frame source ----------
    def _load_frame_list(self):
        exts = ('*.jpg','*.jpeg','*.png','*.bmp')
        base = os.path.join(os.getcwd(), 'test_frames')
        paths = []
        for e in exts:
            paths.extend(glob.glob(os.path.join(base, e)))
        self._frame_paths = sorted(paths)
        self._frame_idx = 0

    def _next_frame_bgr(self):
        """Read next frame from ./test_frames; fallback to moving bar."""
        if CV2 is not None and self._frame_paths:
            p = self._frame_paths[self._frame_idx]
            self._frame_idx = (self._frame_idx + 1) % len(self._frame_paths)
            img = CV2.imread(p, CV2.IMREAD_COLOR)
            if img is not None:
                self._last_bgr = img
                return img
        # Fallback synthetic
        if NP is not None:
            img = moving_bar(360, 480, time.time())
            self._last_bgr = img
            return img
        return None

    # ---------- helpers ----------
    def _maybe_pub(self, cond, msg_type, topic, qos):
        return self.create_publisher(msg_type, topic, qos) if cond else None

    # ---------- regular numerics ----------
    def tick_speed(self):
        self._spd = max(0.0, self._spd + random.uniform(-0.25, 0.45))
        if random.random() < self.args.nan_prob:
            val = float('nan')
        elif random.random() < self.args.inf_prob:
            val = float('inf') if random.random() < 0.5 else float('-inf')
        else:
            val = self._spd
        self.pub_speed.publish(Float32(data=float(val)))

    def tick_other_numerics(self):
        t = time.time()
        if self.pub_altitude:
            alt = 12.0 + 2.0 * math.sin(t * 0.2)
            self.pub_altitude.publish(Float32(data=float(alt)))
        if self.pub_batt_mv:
            mv = 12000.0 - (t % 600) * 2.0
            self.pub_batt_mv.publish(Float32(data=float(mv)))
        if self.pub_temp_c:
            temp = 30.0 + 5.0 * math.sin(t * 0.1)
            self.pub_temp_c.publish(Float32(data=float(temp)))
        if self.pub_voltage_v:
            v = 12.0 + 0.2 * math.sin(t * 0.6)
            self.pub_voltage_v.publish(Float32(data=float(v)))
        if self.pub_current_a:
            a = 1.2 + 0.6 * (math.sin(t * 1.2) * 0.5 + 0.5)
            self.pub_current_a.publish(Float32(data=float(a)))
        if self.pub_power_w:
            v = 12.0 + 0.2 * math.sin(t * 0.6)
            a = 1.2 + 0.6 * (math.sin(t * 1.2) * 0.5 + 0.5)
            self.pub_power_w.publish(Float32(data=float(v * a)))
        if self.pub_yaw_rate:
            yaw = 45.0 * math.sin(t * 0.7)
            self.pub_yaw_rate.publish(Float32(data=float(yaw)))
        if self.pub_heading_deg:
            hdg = (t * 12.0) % 360.0
            self.pub_heading_deg.publish(Float32(data=float(hdg)))
        if self.pub_mem_mb:
            mem = 500.0 + 100.0 * (math.sin(t * 0.05) * 0.5 + 0.5)
            self.pub_mem_mb.publish(Float32(data=float(mem)))
        if self.pub_press_kpa:
            p = 101.3 + 1.2 * math.sin(t * 0.03)
            self.pub_press_kpa.publish(Float32(data=float(p)))

    # ---------- regular text ----------
    def tick_log(self):
        try:
            lvl = next(self._level_cycle)
        except StopIteration:
            self._level_cycle = iter(['INFO', 'WARN', 'ERROR'])
            lvl = next(self._level_cycle)
        msg = f"{lvl}: regular/log heartbeat spd≈{getattr(self, '_spd', 0.0):.2f}"
        self.pub_log.publish(String(data=msg))

    def tick_debug(self):
        msg = f"DEBUG: cpu={random.randint(20,90)}%"
        self.pub_debug.publish(String(data=msg))

    # ---------- images (from disk frames, looped) ----------
    def tick_raw_image(self):
        if BRIDGE is None:
            return
        frame = self._next_frame_bgr()
        if frame is None:
            return
        if self._w and self._h and (frame.shape[1] != self._w or frame.shape[0] != self._h):
            frame = CV2.resize(frame, (self._w, self._h)) if CV2 is not None else frame
        msg = BRIDGE.cv2_to_imgmsg(frame, encoding='bgr8')
        msg.header.frame_id = self.camera_frame
        self.pub_img.publish(msg)

    def tick_compressed_image(self):
        if CV2 is None:
            return
        # Reuse last loaded frame if available; else read next
        frame = self._last_bgr if self._last_bgr is not None else self._next_frame_bgr()
        if frame is None:
            return
        if self._wc and self._hc and (frame.shape[1] != self._wc or frame.shape[0] != self._hc):
            frame = CV2.resize(frame, (self._wc, self._hc))
        ok, buf = CV2.imencode('.jpg', frame, [int(CV2.IMWRITE_JPEG_QUALITY), 80])
        if not ok:
            return
        msg = CompressedImage()
        msg.format = 'jpeg'
        msg.data = buf.tobytes()
        self.pub_imgc.publish(msg)

    # ---------- LiDAR ----------
    def tick_scan(self):
        n = 360
        rng = []
        t = time.time()
        blocked_start = int((math.sin(t*0.3)*0.5+0.5) * 120)
        for i in range(n):
            base = 5.0 + 0.5 * math.sin((i/20.0) + t*0.2)
            noise = random.uniform(-0.05, 0.05)
            r = max(0.1, base + noise)
            if blocked_start <= i < blocked_start+20:
                r = 0.15
            rng.append(r)

        msg = LaserScan()
        msg.header.frame_id = self.lidar_frame
        msg.angle_min = -math.pi
        msg.angle_max = math.pi
        msg.angle_increment = (msg.angle_max - msg.angle_min) / n
        msg.time_increment = 0.0
        msg.scan_time = 1.0 / max(self.args.lidar_hz, 1.0)
        msg.range_min = 0.1
        msg.range_max = 12.0
        msg.ranges = rng
        self.pub_scan.publish(msg)

    def tick_pc2(self):
        if NP is None:
            return
        N = 800
        t = time.time()
        ang = NP.linspace(0, 2*math.pi, N, endpoint=False)
        r = 4.0 + 0.2*NP.sin(ang*4 + t*0.8)
        x = r * NP.cos(ang)
        y = r * NP.sin(ang)
        z = 0.2 * NP.sin(ang*6 + t*1.2)
        pts = NP.stack([x, y, z], axis=1).astype(NP.float32)
        msg = make_pc2(pts, frame_id=self.lidar_frame)
        if msg:
            self.pub_pc2.publish(msg)

    # ---------- DDS numerics ----------
    def tick_dds_latency(self):
        start = time.perf_counter()
        ms = -1.0
        try:
            with socket.create_connection(("8.8.8.8", 53), timeout=0.5):
                pass
            ms = (time.perf_counter() - start) * 1000.0
        except Exception:
            ms = -1.0
        self.pub_dds_latency.publish(Float32(data=float(ms)))

    def tick_dds_cpu(self):
        t = time.time()
        cpu = (math.sin(t * 0.7) * 0.5 + 0.5) * 100.0
        self.pub_dds_cpu.publish(Float32(data=float(cpu)))

    # ---------- DDS texts ----------
    def tick_dds_texts(self):
        if hasattr(self, 'pub_dds_diag') and self.pub_dds_diag:
            self.pub_dds_diag.publish(String(data="DDS OK: system nominal"))
        if hasattr(self, 'pub_dds_diag2') and self.pub_dds_diag2:
            self.pub_dds_diag2.publish(String(data="DDS/System: health optimal"))

    # ---------- bursts to stress charts ----------
    def tick_burst(self):
        for _ in range(self.args.burst_points):
            v = max(0.0, getattr(self, '_spd', 0.0) + random.uniform(-0.1, 0.1))
            self.pub_speed.publish(Float32(data=float(v)))

    # ---------- TF publishers ----------
    def _publish_static_tf(self):
        # map -> odom (identity or slight offset)
        t1 = TransformStamped()
        t1.header.frame_id = 'map'
        t1.child_frame_id  = 'odom'
        t1.transform.translation.x = 0.0
        t1.transform.translation.y = 0.0
        t1.transform.translation.z = 0.0
        q = quat_from_rpy(0, 0, 0)
        t1.transform.rotation.x, t1.transform.rotation.y, t1.transform.rotation.z, t1.transform.rotation.w = q

        # base_link -> camera_link (forward + up, slight downward pitch)
        t2 = TransformStamped()
        t2.header.frame_id = 'base_link'
        t2.child_frame_id  = 'camera_link'
        t2.transform.translation.x = 0.20
        t2.transform.translation.y = 0.00
        t2.transform.translation.z = 0.15
        q = quat_from_rpy(math.radians(-10), 0, 0)
        t2.transform.rotation.x, t2.transform.rotation.y, t2.transform.rotation.z, t2.transform.rotation.w = q

        # base_link -> lidar_link (on mast)
        t3 = TransformStamped()
        t3.header.frame_id = 'base_link'
        t3.child_frame_id  = 'lidar_link'
        t3.transform.translation.x = 0.00
        t3.transform.translation.y = 0.00
        t3.transform.translation.z = 0.25
        q = quat_from_rpy(0, 0, 0)
        t3.transform.rotation.x, t3.transform.rotation.y, t3.transform.rotation.z, t3.transform.rotation.w = q

        # base_link -> imu_link (co-located)
        t4 = TransformStamped()
        t4.header.frame_id = 'base_link'
        t4.child_frame_id  = 'imu_link'
        t4.transform.translation.x = 0.00
        t4.transform.translation.y = 0.00
        t4.transform.translation.z = 0.00
        q = quat_from_rpy(0, 0, 0)
        t4.transform.rotation.x, t4.transform.rotation.y, t4.transform.rotation.z, t4.transform.rotation.w = q

        msg = TFMessage(transforms=[t1, t2, t3, t4])
        # static transforms use TRANSIENT_LOCAL; publish once (or a couple times)
        self.pub_tf_static.publish(msg)

    def _tick_tf_base(self):
        # odom -> base_link : robot moves in a small circle; yaw increases
        t = time.time()
        x = 1.5 * math.cos(t * 0.2)
        y = 1.5 * math.sin(t * 0.2)
        yaw = (t * 0.2) % (2*math.pi)

        ts = TransformStamped()
        ts.header.frame_id = 'odom'
        ts.child_frame_id  = 'base_link'
        ts.transform.translation.x = x
        ts.transform.translation.y = y
        ts.transform.translation.z = 0.0
        q = quat_from_rpy(0.0, 0.0, yaw)
        ts.transform.rotation.x, ts.transform.rotation.y, ts.transform.rotation.z, ts.transform.rotation.w = q

        self.pub_tf.publish(TFMessage(transforms=[ts]))

    def _tick_tf_camera(self):
        # Tiny oscillation on camera_link pitch (just to show dynamic child motion)
        t = time.time()
        pitch = math.radians(-10 + 2.0*math.sin(t*0.7))

        ts = TransformStamped()
        ts.header.frame_id = 'base_link'
        ts.child_frame_id  = 'camera_link'
        ts.transform.translation.x = 0.20
        ts.transform.translation.y = 0.00
        ts.transform.translation.z = 0.15
        q = quat_from_rpy(pitch, 0.0, 0.0)
        ts.transform.rotation.x, ts.transform.rotation.y, ts.transform.rotation.z, ts.transform.rotation.w = q

        self.pub_tf.publish(TFMessage(transforms=[ts]))


# ---------------- Argparse + main ----------------
def parse_args():
    p = argparse.ArgumentParser(description="Agnostic RDash ROS2 demo (expanded, TF + video frames)")
    p.add_argument('--reliable', action='store_true', help='Use RELIABLE QoS (default BEST_EFFORT)')
    # toggles
    p.add_argument('--enable-numeric',    action='store_true', default=True)
    p.add_argument('--enable-text',       action='store_true', default=True)
    p.add_argument('--enable-image',      action='store_true', default=True)
    p.add_argument('--enable-compressed', action='store_true', default=True)
    p.add_argument('--enable-lidar',      action='store_true', default=True)
    p.add_argument('--enable-dds-numeric', action='store_true', default=True)
    p.add_argument('--enable-dds-text',    action='store_true', default=True)
    p.add_argument('--enable-bursts',     action='store_true', default=True)
    # rates/periods
    p.add_argument('--numeric-hz', type=float, default=5.0)
    p.add_argument('--image-hz', type=float, default=15.0)      # raw camera FPS
    p.add_argument('--compressed-hz', type=float, default=8.0)  # jpeg FPS
    p.add_argument('--text-period', type=float, default=2.0)
    p.add_argument('--lidar-hz', type=float, default=7.0)       # LaserScan
    p.add_argument('--pc2-hz', type=float, default=5.0)         # PointCloud2
    p.add_argument('--dds-latency-period', type=float, default=2.0)
    p.add_argument('--dds-cpu-period', type=float, default=1.0)
    p.add_argument('--dds-text-period', type=float, default=3.0)
    # image sizes
    p.add_argument('--raw-h', type=int, default=360)
    p.add_argument('--raw-w', type=int, default=480)
    p.add_argument('--jpeg-h', type=int, default=360)
    p.add_argument('--jpeg-w', type=int, default=480)
    # bursts / edge injection
    p.add_argument('--burst-every', type=float, default=5.0, help='Seconds between speed bursts')
    p.add_argument('--burst-points', type=int, default=60,   help='Points per burst')
    p.add_argument('--nan-prob', type=float, default=0.02,   help='Probability of NaN injection on speed')
    p.add_argument('--inf-prob', type=float, default=0.01,   help='Probability of +/-Inf injection on speed')
    return p.parse_args()

def main():
    args = parse_args()
    rclpy.init()
    node = AgnosticDemo(args)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
