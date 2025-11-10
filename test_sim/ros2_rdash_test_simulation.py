#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
  - Camera (raw):    /robot/camera/image
  - Camera (jpeg):   /robot/camera/image/compressed
  - LiDAR LaserScan: /robot/lidar/scan
  - LiDAR PointCloud2: /robot/lidar/points  (for --pc2-summarize)

DDS (drawer):
  - Numeric: /foo/dds/network/latency_ms, /bar/dds/cpu/percent
  - Text:    /baz/dds/diagnostics, /baz/dds/system/diagnostics

Agnostic rule: any topic path that contains a `dds` segment appears in the DDS drawer.
"""

import math
import time
import random
import socket
import argparse
import struct

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy

from std_msgs.msg import Float32, String
from sensor_msgs.msg import Image, CompressedImage, LaserScan, PointCloud2, PointField

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

def qos_profiles(reliable: bool):
    reliab = ReliabilityPolicy.RELIABLE if reliable else ReliabilityPolicy.BEST_EFFORT
    return QoSProfile(
        depth=10,
        reliability=reliab,
        durability=DurabilityPolicy.VOLATILE,
        history=HistoryPolicy.KEEP_LAST,
    )

def moving_bar(h, w, t):
    """Simple synthetic image with a moving white bar."""
    if NP is None:
        return None
    img = NP.zeros((h, w, 3), dtype=NP.uint8)
    x = int((math.sin(t) * 0.5 + 0.5) * (w - 3))
    img[:, x:x+3, :] = 255
    return img

def make_pc2(points_xyz, frame_id="lidar"):
    """
    points_xyz: numpy array shape (N,3) float32 (x,y,z)
    """
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

class AgnosticDemo(Node):
    def __init__(self, args):
        super().__init__('agnostic_demo')
        self.args = args
        qos_rel = qos_profiles(args.reliable)

        # -------- Regular numerics (main robot page) --------
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

        # cameras
        self.pub_img  = self._maybe_pub(args.enable_image and BRIDGE is not None, Image, '/robot/camera/image', qos_rel)
        self.pub_imgc = self._maybe_pub(args.enable_compressed and CV2 is not None, CompressedImage, '/robot/camera/image/compressed', qos_rel)

        # LiDAR: LaserScan + PointCloud2
        self.pub_scan = self._maybe_pub(args.enable_lidar, LaserScan, '/robot/lidar/scan', qos_rel)
        self.pub_pc2  = self._maybe_pub(args.enable_lidar and NP is not None, PointCloud2, '/robot/lidar/points', qos_rel)

        # -------- DDS topics (drawer) --------
        self.pub_dds_latency = self._maybe_pub(args.enable_dds_numeric, Float32, '/foo/dds/network/latency_ms', qos_rel)
        self.pub_dds_cpu     = self._maybe_pub(args.enable_dds_numeric, Float32, '/bar/dds/cpu/percent', qos_rel)
        self.pub_dds_diag    = self._maybe_pub(args.enable_dds_text,    String,  '/baz/dds/diagnostics', qos_rel)
        self.pub_dds_diag2   = self._maybe_pub(args.enable_dds_text,    String,  '/baz/dds/system/diagnostics', qos_rel)

        # -------- Timers --------
        # numeric backbone
        if self.pub_speed:
            self._spd = 0.0
            self.create_timer(1.0 / args.numeric_hz, self.tick_speed)

        # other numerics (tie to same rate for simplicity)
        if args.enable_numeric:
            self.create_timer(1.0 / args.numeric_hz, self.tick_other_numerics)

        # text (levels cycle)
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
        if self.pub_dds_latency:
            self.create_timer(args.dds_latency_period, self.tick_dds_latency)
        if self.pub_dds_cpu:
            self.create_timer(args.dds_cpu_period, self.tick_dds_cpu)
        # DDS text
        if self.pub_dds_diag or self.pub_dds_diag2:
            self.create_timer(args.dds_text_period, self.tick_dds_texts)

        # bursts on speed to stress charts
        if self.pub_speed and args.enable_bursts:
            self.create_timer(args.burst_every, self.tick_burst)

        self.get_logger().info("Agnostic demo ready. Press Ctrl+C to stop.")

    # helpers 
    def _maybe_pub(self, cond, msg_type, topic, qos):
        return self.create_publisher(msg_type, topic, qos) if cond else None

    # regular numerics 
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
        # altitude (slow sine)
        if self.pub_altitude:
            alt = 12.0 + 2.0 * math.sin(t * 0.2)
            self.pub_altitude.publish(Float32(data=float(alt)))
        # battery (sawtooth down)
        if self.pub_batt_mv:
            mv = 12000.0 - (t % 600) * 2.0  # drops 2 mV/s
            self.pub_batt_mv.publish(Float32(data=float(mv)))
        # temp
        if self.pub_temp_c:
            temp = 30.0 + 5.0 * math.sin(t * 0.1)
            self.pub_temp_c.publish(Float32(data=float(temp)))
        # electrical
        if self.pub_voltage_v:
            v = 12.0 + 0.2 * math.sin(t * 0.6)
            self.pub_voltage_v.publish(Float32(data=float(v)))
        if self.pub_current_a:
            a = 1.2 + 0.6 * (math.sin(t * 1.2) * 0.5 + 0.5)
            self.pub_current_a.publish(Float32(data=float(a)))
        if self.pub_power_w:
            # crude P = V * A
            v = 12.0 + 0.2 * math.sin(t * 0.6)
            a = 1.2 + 0.6 * (math.sin(t * 1.2) * 0.5 + 0.5)
            self.pub_power_w.publish(Float32(data=float(v * a)))
        # dynamics
        if self.pub_yaw_rate:
            yaw = 45.0 * math.sin(t * 0.7)
            self.pub_yaw_rate.publish(Float32(data=float(yaw)))
        if self.pub_heading_deg:
            hdg = (t * 12.0) % 360.0
            self.pub_heading_deg.publish(Float32(data=float(hdg)))
        # system
        if self.pub_mem_mb:
            mem = 500.0 + 100.0 * (math.sin(t * 0.05) * 0.5 + 0.5)
            self.pub_mem_mb.publish(Float32(data=float(mem)))
        if self.pub_press_kpa:
            p = 101.3 + 1.2 * math.sin(t * 0.03)
            self.pub_press_kpa.publish(Float32(data=float(p)))

    # regular text (level-tagged)
    def tick_log(self):
        try:
            lvl = next(self._level_cycle)
        except StopIteration:
            self._level_cycle = iter(['INFO', 'WARN', 'ERROR'])
            lvl = next(self._level_cycle)
        msg = f"{lvl}: regular/log heartbeat spd≈{getattr(self, '_spd', 0.0):.2f}"
        self.pub_log.publish(String(data=msg))

    # images
    def tick_raw_image(self):
        if BRIDGE is None or NP is None:
            return
        img = moving_bar(self._h, self._w, time.time())
        if img is None:
            return
        msg = BRIDGE.cv2_to_imgmsg(img, encoding='bgr8')
        self.pub_img.publish(msg)

    def tick_compressed_image(self):
        if CV2 is None or NP is None:
            return
        img = moving_bar(self._hc, self._wc, time.time())
        if img is None:
            return
        ok, buf = CV2.imencode('.jpg', img, [int(CV2.IMWRITE_JPEG_QUALITY), 80])
        if not ok:
            return
        msg = CompressedImage()
        msg.format = 'jpeg'
        msg.data = buf.tobytes()
        self.pub_imgc.publish(msg)

    # LiDAR
    def tick_scan(self):
        # 360 beams, ~5m average with some noise & a blocked sector
        n = 360
        rng = []
        t = time.time()
        blocked_start = int((math.sin(t*0.3)*0.5+0.5) * 120)  # moving blocked sector
        for i in range(n):
            base = 5.0 + 0.5 * math.sin((i/20.0) + t*0.2)
            noise = random.uniform(-0.05, 0.05)
            r = max(0.1, base + noise)
            if blocked_start <= i < blocked_start+20:
                r = 0.15  # near obstacle
            rng.append(r)

        msg = LaserScan()
        msg.header.frame_id = 'lidar'
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
        # ring of points with slight vertical ripple; ~800 points
        N = 800
        t = time.time()
        ang = NP.linspace(0, 2*math.pi, N, endpoint=False)
        r = 4.0 + 0.2*NP.sin(ang*4 + t*0.8)
        x = r * NP.cos(ang)
        y = r * NP.sin(ang)
        z = 0.2 * NP.sin(ang*6 + t*1.2)
        pts = NP.stack([x, y, z], axis=1).astype(NP.float32)
        msg = make_pc2(pts, frame_id='lidar')
        if msg:
            self.pub_pc2.publish(msg)

    # DDS numerics
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

    # DDS texts
    def tick_dds_texts(self):
        if self.pub_dds_diag:
            self.pub_dds_diag.publish(String(data="DDS OK: system nominal"))
        if self.pub_dds_diag2:
            self.pub_dds_diag2.publish(String(data="DDS/System: health optimal"))

    # bursts to stress charts
    def tick_burst(self):
        for _ in range(self.args.burst_points):
            v = max(0.0, getattr(self, '_spd', 0.0) + random.uniform(-0.1, 0.1))
            self.pub_speed.publish(Float32(data=float(v)))

def parse_args():
    p = argparse.ArgumentParser(description="Agnostic RDash ROS2 demo (expanded)")
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