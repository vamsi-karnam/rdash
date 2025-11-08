# R'DASH - Robot Information Telemetry Transport Dashboard (v1.0)
**Author:** Vamsi Karnam
**License:** Apache License 2.0

*“ R’DASH (Robot Information Telemetry Transport Dashboard) is an open-source, cross-platform dashboard for real-time robot data visualization - developed and maintained by [Vamsi Karnam](https://www.linkedin.com/in/saivamsikarnam/) - Coalescing robot data made easy ”*

## Screenshots

<img width="1911" height="1107" alt="Screenshot 2025-11-08 003408" src="https://github.com/user-attachments/assets/fce6498f-4c2e-4bf2-a3e1-7efc51322229" />

<table>
<tr> 
<td><img width="1910" height="1106" alt="Screenshot 2025-11-08 003714" src="https://github.com/user-attachments/assets/33a11b09-e0de-4d66-b895-cc58526ee08d" /></td>
<td><img width="1912" height="1100" alt="Screenshot 2025-11-08 003650" src="https://github.com/user-attachments/assets/d5167fd5-fab4-4991-a446-992ca72e39df" /></td>
<td><img width="1906" height="1102" alt="Screenshot 2025-11-07 232332" src="https://github.com/user-attachments/assets/60da1a2a-5c23-42ea-a3aa-ea11a70a2f18" /></td>
<td><img width="1912" height="1106" alt="Screenshot 2025-11-07 232312" src="https://github.com/user-attachments/assets/ed0f6b09-d7f6-491c-b81c-40b117e56a53" /></td>
<td><img width="1910" height="1105" alt="Screenshot 2025-11-07 232148" src="https://github.com/user-attachments/assets/734715c8-fea0-4c1a-acf4-517859f4e321" /></td>
</tr> 
</table>

## TL;DR

> For folks who already have ROS2 working and just want to see data flow end-to-end.
> Check supported data types from [supported input types](#supported-data) and [prerequisites](#prerequisites)

1. **Clone the repo:**

```bash
git clone https://github.com/vamsi-karnam/rdash.git
```

2. **(Optional) Put TLS certs** in `ros2_rdash_app/source/certs/` as `cert.pem` + `key.pem` (self-signed at your own risk).

3. **Run the server:** *HTTPS is optional*

```bash
cd main/ros2_rdash_app/source
python3 -m venv .venv && source .venv/bin/activate (optional)
pip install -r requirements.txt
```
```bash
python3 rdash_app.py --host 0.0.0.0 --port 8443 --auth-token "<SECRET>"
# If no certs found in certs folder, the server auto-falls back to HTTP on port 8080 unless you pass --cert/--key.
```

4. **Run the agent on your ROS2 box:**

```bash
cd main/ros2_rdash_agent/source
python3 -m venv .venv && source .venv/bin/activate (optional)
pip install -r requirements.txt
```
```bash
python3 rdash_agent.py \
  --server https://<SERVER-IP>:8443 \
  --token "<SECRET>" \
  --robot-name mybot-01 \
  --include ".*" \
  --max-hz 10 \
  --max-metrics-per-push 32 \
  --pc2-summarize
```

5. **Open the dashboard:**
   If the browser didn’t auto-open, visit `https://<SERVER-IP>:8443/` (or `http://<SERVER-IP>:8080/` if running without TLS) on any device on the same LAN.
   Enter your token if prompted. You should see your robot(s) appear and live data streaming.


## Table of contents

* [Intro & description](#intro--description)
* [Features](#features)
* [Supported data](#supported-data)
* [Core design principles](#core-design-principles)
* [Architecture](#architecture)
* [Directory structure](#dirctory-structure)
* [Usage](#usage)
  * [Prerequisites](#prerequisites)
  * [Server (rdash App)](#server-rdash-app)
  * [Agent (rdash Agent)](#agent-rdash-agent)
  * [Preferred deployment topology](#preferred-deployment-topology)
  * [Test simulation](#test-simulation)
  * [API docs](#api-docs)
* [Front-end behavior & controls](#dashboard-behavior)
* [Data flow](#data-flow)
* [Performance & tuning](#performance--tuning)
* [Warnings & security notes](#warnings--security-notes)
* [License / Author / Contact](#license--author--contact--citation)
* [Acknowledgments](#acknowledgments)
* [Appendix I - Flags and Env vars](#appendix-i---full-flags--env-vars)
* [Appendix II - CLI usage examples](#appendix-ii---full-cli-examples)

## Intro & description
**R'DASH** is a minimal, production-leaning data telemetry pipe for robots:
* **Server (`rdash_app.py`)**: Flask + Socket.IO web app that ingests telemetry, keeps it in RAM, and broadcasts live updates to browsers.
* **Agent (`rdash_agent.py`)**: A ROS 2 node that discovers topics, flattens structured messages into numeric key/value pairs, pushes images/video frames, and optionally summarizes point clouds.

No DB, no schema migrations, no exotic drivers. You get **fast setup** and **transparent behavior**, plus opt-in controls to keep the browser responsive on heavy streams (lidar-like topics).

## Features
* **OS:** Linux/macOS/Windows (anything that runs Python 3 + Flask + rclpy; ROS typically on Linux). As a bonus the http/https host website can also be opened on a mobile device connected to the same network.
* **Robot / sensors:** Fully topic-agnostic for numeric streams (any message that flattens to numbers). Images supported via `sensor_msgs/Image` or `sensor_msgs/CompressedImage`. TF visualized as a simple frame tree.
* **ROS 2 distros:** Smoke-tested with **ROS 2 Jazzy**; other distros should work as long as `rclpy` is available.
* **Transport:** Pure HTTP(S) + WebSocket - no ROS in the server/web app, so you can place it anywhere (edge, admin PC, cloud with a tunnel, etc.).
* **Topic Schema**: Custom ROS2 topics are fully supported - no fixed naming scheme is required.

## Supported Data
**Supported ROS2 input types:**
* Numeric topics (anything flattenable into numbers) - **topic agnostic**.
* Images/Video Frames via `sensor_msgs/Image` or `sensor_msgs/CompressedImage`.
* Text/Logs via `std_msgs/String` (visualized in a rolling, console-style panel).
* TF via `tf2_msgs/TFMessage`.
* *(Audio endpoint is present for API compatibility but not visualised yet.)*
* *(PointCloud2 is not visualised; use `--pc2-summarize` to publish stats as numeric.)*
> Note: R’DASH agent automatically discovers all ROS2 topics - including custom ones. Any topic that publishes numeric, image, string, or PointCloud2 data will be captured and pushed to the server. You can use any topic name (for example, mybot/barometer01/pressure_mb). Similarly, Non-ROS runtimes can push data directly to the R'DASH server via the REST API (see [API Docs](#api-docs)) using arbitrary sensor names as long as the topic or source is publishing data.

## Core design principles
* **Agnostic:** no assumptions about robot, OS, sensor taxonomy, or data schema.
* **Simple to operate:** one server + one agent per ROS box.
* **Transparent & observable:** deterministic trimming; optional UI badges for capping (if enabled in your build).
* **No DB:** in-memory, ring-buffered, low-latency path to the browser.
* **Minimal deps:** plain Flask/Socket.IO on the server; rclpy on the agent.

## Architecture

```mermaid
flowchart TD
  subgraph "Robot / ROS 2 Network"
    A1["ROS 2 Nodes (sensors, nav, etc.)"]
    A2["TF / Images / Numeric topics / Text / Audio / etc"]
  end

  A1 --> A2
  A2 --> B["RDASH Agent (rclpy)"]
  B --> Z["HTTP/HTTPS (REST API): /api/push, /api/push_image, /api/push_tf, etc"] --> C["RDASH App (Flask + Socket.IO)"]

  subgraph "Non-ROS Runtimes"
    X1["Custom scripts / services / IoT Devices"]
    X2["Images / Video / Numeric topics / Text / etc"]
  end

  X1 --> X2
  X2 --> Y["HTTP clients (curl, Python requests, etc.)"]
  Y --> Z

  C -- "WebSocket" --> D["Browser Dashboard (JS ECharts)"]

  subgraph Browser
    D1["Charts / Graphs / Panels"]
  end

  D -->  D1
```

## Dirctory structure

```
main/
├── ros2_rdash_agent/
│   └── source/
│       ├── rdash_agent.py
│       └── requirements.txt
├── ros2_rdash_app/
│   └── source/
│       ├── rdash_app.py
│       ├── requirements.txt
│       ├── certs/               
│       ├── static/
│       │   ├── fonts/            
│       │   ├── js/
│       │   │   └── main.js
│       │   └── styles/
│       │       └── styles.css
│       └── templates/
│           └── index.html
├── test_sim/
│   ├── setup.txt                 
│   └── ros2_rdash_test_simulation.py
└── README.md
└── LICENSE
└── NOTICE                    
```


## Usage

### Prerequisites

* A working **ROS 2** environment (Jazzy recommended).
  *[link to official ROS 2 Jazzy docs](https://docs.ros.org/en/jazzy/index.html)*
* Python 3.10+ recommended.
* If trying to connect multiple robots - Network access setup (VPN/DNS Port Forwarding/LAN) between your ROS2 machine(s) and the server.

You can quickly verify your ROS2 environment:

```bash
ros2 topic list
```

### Preferred deployment topology

* **Run the rdash_app.py** on an **admin PC** (or an edge server) reachable by all robots/devices on the LAN.
* **Run a rdash_agent.py** on **each ROS2 machine** or for **each ROS2 node** (robot PCs, sensor PCs), configured with the server URL and token.

### Server

**Install:**

```bash
cd main/ros2_rdash_app/source
python3 -m venv .venv && source .venv/bin/activate (optional)
pip install -r requirements.txt
```

**TLS (optional but recommended when running outside LAN):**

* Generate and sign or use your own certificates.
* Put `cert.pem` and `key.pem` in `./certs/` **or** provide `--cert`/`--key`.

**Run:**

```bash
python3 rdash_app.py --host 0.0.0.0 --port 8443 --auth-token "SET-YOUR-SECRET-KEY"
```

> If no cert is found, rdash_app.py falls back to HTTP on port **8080** (unless you pass `--cert/--key`).

**Access the dashboard**
Open: `https://<HOST-IP>:8443/` (or `http://<HOST-IP>:8080/`) on a browser if the browser does not auto open.

**Alternative LAN access:**
Make sure host firewalls allow the chosen port. From another machine on your LAN:
`https://<SERVER-IP>:8443/` (or `http://<SERVER-IP>:8080/`)

#### Command flags & Env variables
**Command flags (rdash_app.py):**
* `--host` *(default: 0.0.0.0)*: Bind address.
* `--port` *(default: 8443)*: HTTPS port. (Falls back to `8080` for HTTP if no certs are available in certs directory.)
* `--auth-token`: Enables Bearer-token auth for all endpoints and WebSocket.
* `--cert`, `--key`: Paths to TLS cert/key (PEM). If omitted, rdash auto-scans `./certs`.
* `--max-samples` *(env: `rdash_MAX_SAMPLES`, default: 2048)*: Per-metric  buffer in RAM.
* `--max-image-bytes` *(env: `rdash_MAX_IMAGE_BYTES`, default: 8 MB)*: Rejects oversized images.
* `--message-queue`: Optional message queue URL (e.g., `redis://...`) if you scale out processes.
* `--no-browser`: Don’t auto-open the default browser.

**Environment variables:**
* `RDASH_MAX_SAMPLES` - same as `--max-samples`.
* `RDASH_MAX_IMAGE_BYTES` - same as `--max-image-bytes`.
* `RDASH_MAX_AUDIO_BYTES` - limit for audio push endpoint (kept for API compatibility).
* `RDASH_WS_FLUSH_HZ` - WebSocket coalescer rate (default **20 Hz**).



### Agent
> On your ROS2 machine

**Install:**

```bash
cd main/ros2_rdash_agent/source
python3 -m venv .venv && source .venv/bin/activate (optional)
pip install -r requirements.txt
```

**Run:**

```bash
python3 rdash_agent.py \
  --server http://<HOST-IP>:<PORT> \
  --token "USE-YOUR-SECRET-KEY" \
  --robot-name mybot-01 \
  --include ".*" \
  --exclude "/rosout|/parameter_events" \
  --numeric-qos best \
  --timestamp receive \
  --max-hz 10 \
  --max-metrics-per-push 32 \
  --pc2-summarize
```

#### Command flags
**Command flags (rdash_agent.py):**

* `--server` **(required)**: Base URL for the app (e.g., `https://HOST:8443` or `http://HOST:8080`).
* `--token`: Must match the server’s `--auth-token` if auth is enabled.
* `--robot-name` **(required)**: Friendly display name (e.g., `testsim-001`).
* `--include`: Regex to include topics (default: include all).
* `--exclude` *(default `/rosout|/parameter_events`)*: Regex to drop noisy topics.
* `--numeric-qos` *(best|reliable|dual, default: best)*: QoS for numeric topics.
* `--timestamp` *(header|receive, default: receive)*: Choose timestamping method per sample.
* `--unit "<regex>=<UNIT>:<scale>"` *(repeatable)*: Tag units & scale values (e.g., `vel.*=m/s:1`).
* `--pc2-summarize`: Summarize `PointCloud2` into min/max/mean stats (helps keep UI fast).
* `--max-hz` *(default 10)*: Per-sensor numeric push rate cap.
* `--max-metrics-per-push` *(default 32, 0 = unlimited)*: **Trims** a large numeric dict to the first N keys (sorted) to avoid choking the UI/WS path.
  * **Important:** This **does not change numeric values**, only **limits the number of keys per push**.
  * If you need exact full fidelity for a specific topic, increase this value depending on your hardware.

**Tip:** You can run **multiple agents** (one per robot/device/ROS2 node) pointed at the same server.

This keeps ROS2 traffic local, and the server only speaks HTTP/HTTPS + WebSocket.


### Test simulation

For a quick smoke test without real robots:
> Follow the [Server](#server) and [Agent](#agent) setup steps
> Open a new terminal on the ROS2 machine (where the Agent is already running)

```bash
cd main/test_sim
# Follow setup hints in setup.txt if needed (ROS 2 environment, sourcing, etc.)
python3 ros2_rdash_test_simulation.py
```

> Open the dashboard on a browser using `http://HOST:8080` or `https://HOST:8443` and watch dummy sensors stream.

### API Docs

> API runs independently on REST endpoints, no requirement of ROS2 runtime.

1. Everything is an HTTP API. The server accepts JSON/multipart payloads and serves lightweight JSON for status, history, and metadata.
2. Auth: Use Authorization: Bearer <token> for all /api/* endpoints and the WebSocket. The video endpoint also accepts ?token=... as a query parameter for simple embed tests.
3. State is RAM-only. Deleting series/logs clears in-memory buffers. Restarting the server clears all state.
4. Streaming: Numeric data is coalesced and broadcast over WebSocket; camera frames are exposed as MJPEG at /video/<robot>/<sensor>.
5. Topic-agnostic: The agent flattens any ROS 2 message into numeric fields and pushes to /api/push. Units/scales are attachable via --unit rules.
6. Find the documentation here: *[api_documentation.pdf](https://github.com/user-attachments/files/23427688/api_documentation.pdf)*


## Dashboard behavior

* **Live numeric charts** (ECharts) with:
  * Sliding time window (e.g., last 10 s) and slight lead so the latest sample isn’t glued to the right edge.
  * **Pause / Resume** per chart (buffers are capped to prevent memory blow-ups).
  * **Delete Data** per chart (resets the in-memory series for that sensor).
  * Responsive axis labels; devicePixelRatio aware.

* **Images** render from `sensor_msgs/Image` or `sensor_msgs/CompressedImage` (MJPEG stream).

* **TF** renders as a compact frame tree.

* **Text/Logs panel**: A collapsible panel (listed after other panels) that streams lines from `std_msgs/String` topics. Behaves like a console:
  * Live appending with session-scoped scrollback
  * **Pause / Resume** (buffers capped to avoid memory blow-ups)
  * **Delete** clears the in-memory log for that sensor


## Data flow

* **Numeric topics** → Agent flattens message into a nested key map (e.g., `field.subfield.0`) with **float values only**. Units can be tagged via `--unit` rules (e.g., `rpm -> rad/s`).
* Agent **throttles** per sensor via `--max-hz`.
* **Optional cap** via `--max-metrics-per-push` to keep pushes sane (first N keys, sorted, deterministic).
* Server receives via `/api/push`, stores in per-metric ring buffers (`RDASH_MAX_SAMPLES`), and **coalesces** latest values for WebSocket broadcast at `RDASH_WS_FLUSH_HZ`.
* Browser subscribes to `/ws` and draws live charts.


## Performance & tuning

Heavy topics (e.g., lidar-like numeric explosions) can choke the browser running the dashboard. You have three options:

1. **Limit frequency** at the agent:
   `--max-hz 5` or `--max-hz 2` for very heavy sensors.

2. **Limit keys per push** at the agent:
   `--max-metrics-per-push 32` (default).

   * Keeps only the first N keys in alphabetical order for that push.
   * **Does not alter actual numeric values** of kept keys.
   * If you truly need more metrics, raise this value - **your machine, your call**.

3. **Keep windows reasonable** at the front-end:
   The UI keeps a fixed sliding window and decimates buffered points while paused. Long pauses won’t explode memory.

**If your users have powerful workstations**:
They can crank `--max-hz` and `--max-metrics-per-push` up; the same frontend will happily handle higher loads (until you find the browser limit).

**ROS2 Publisher-side best practice:**
Publishing **only the metrics you intend to visualize** is always better than dumping enormous structs at high Hz.


## Warnings & security notes

* **Security:** If you expose the server over the open internet, use **TLS** and **auth tokens**, and consider a VPN or a reverse proxy. Avoid simple port forwarding without security hardening.
* **Resource risk:** Pushing very large images or lots of metrics at high Hz can exhaust CPU/RAM/network bandwidth. Start small, observe, then scale up.
* **Agnostic by design:** rdash doesn’t guess your intent - **filter topics** (`--include/--exclude`), pick sensible **`--max-hz`**, and **cap metrics per push** for heavy streams.
* **Persistence:** All data is in **RAM only** - restarting the server clears state.


## License / Author / Contact / Citation

R'DASH (Robot Information Telemetry Transport Dashboard)  
Developed by **Vamsi Karnam**, 2025.  
Released under the **Apache 2.0 License**.

If you use this software in any of your works, please cite:
> Karnam, S. V. (2025). *R'DASH: An Open Robot Information Telemetry Transport and Visualization Framework.*

For contact, collaborations, bug reports, or business, reach out via saivamsi.karnam@gmail.com.

Author Social: [LinkedIn](https://www.linkedin.com/in/saivamsikarnam/)

## Acknowledgments

* Built with: **Flask**, **Flask-SocketIO**, **ECharts**, **rclpy**.
* Compatible with **ROS 2** (smoke-tested on **Ubuntu 24.04** and **ROS2 Jazzy**).
* Fonts: Orbitron / Rajdhani (included under `static/fonts/`).


## Appendix I - Full flags & env vars

### `rdash_app.py` (server)

**Flags**

* `--host` *(str, default: 0.0.0.0)*
* `--port` *(int, default: 8443)*
* `--auth-token` *(str)*
* `--cert` *(path to PEM cert)*
* `--key` *(path to PEM key)*
* `--max-samples` *(int, default from `RDASH_MAX_SAMPLES` or 2048)*
* `--max-image-bytes` *(int, default from `RDASH_MAX_IMAGE_BYTES` or 8MB per image)*
* `--message-queue` *(str, optional)*
* `--no-browser` *(flag; default None)*

**Environment**

* `RDASH_MAX_SAMPLES`
* `RDASH_MAX_IMAGE_BYTES`
* `RDASH_MAX_AUDIO_BYTES`
* `RDASH_WS_FLUSH_HZ`

### `rdash_agent.py` (agent)

**Flags**

* `--server` *(required)*
* `--token`
* `--robot-name` *(required)*
* `--include`, `--exclude`
* `--numeric-qos` *(best|reliable|dual; default best)*
* `--timestamp` *(header|receive; default receive)*
* `--unit "<regex>=<UNIT>:<scale>"` *(repeatable)*
* `--pc2-summarize`
* `--max-hz` *(float; default 10)*
* `--max-metrics-per-push` *(int; default 32; 0 = unlimited)*
* `--ca-bundle` *(Path to a CA bundle for TLS verification; default None)*
* `--insecure-tls` *(Skip TLS verification; default None)*

**Behavior notes**

* Metric trimming is **deterministic**: sorts keys alphabetically and keeps the first N.
* Values are **not modified** (beyond optional `--unit` scaling you define).
* Prefer publishing only what you’ll visualize.

## Appendix II - Full CLI Examples

### A. `rdash_app.py` (server)

#### A1) Production-style HTTPS + Redis + Auth Token

```bash
# --- Optional tuning via environment (server reads RDASH_* envs) ---
export RDASH_MAX_SAMPLES=4096
export RDASH_MAX_IMAGE_BYTES=$((12*1024*1024))   # 12 MB/image
export RDASH_MAX_AUDIO_BYTES=$((24*1024*1024))   # 24 MB/audio
export RDASH_WS_FLUSH_HZ=30                      # WS coalescer flush Hz

# --- Launch (HTTPS with cert/key, auth, Redis message queue) ---
python3 rdash_app.py \
  --host 0.0.0.0 \
  --port 8443 \
  --auth-token 'rdash_secret_token' \
  --cert ./path/to/certificate \
  --key  ./path/to/key \
  --max-samples 4096 \
  --max-image-bytes $((12*1024*1024)) \
  --message-queue 'redis://localhost:6379/0'
```

What this does:

* Serves **HTTPS** on `:8443` (with provided TLS cert/key).
* Requires clients (browser & agent) to use `Authorization: Bearer rdash_secret_token_123`.
* Uses Redis for Socket.IO message queue.
* Increases in-RAM history & image/audio size limits.
* Faster WS flush cadence for snappier plots.

#### A2) Local dev over HTTP + explicit port + no auto-open

```bash
python3 rdash_app.py \
  --host 127.0.0.1 \
  --port 8080 \
  --auth-token 'rdash_dev_token' \
  --max-samples 1024 \
  --max-image-bytes $((8*1024*1024)) \
  --no-browser
```

What this does:

* Runs **HTTP** on `http://127.0.0.1:8080` without TLS.
* Keeps an auth token.
* No browser auto-open; handy when running inside tmux/containers.

---

### B. `rdash_agent.py` (agent)

> **Tip on `--unit` rules:**
> Rules match **flattened field names**, not topic names.
> Examples of flattened keys you’ll commonly see:
>
> * `twist.linear.x`, `twist.angular.z` (from `geometry_msgs/Twist`)
> * `imu.linear_acceleration.x` (from `sensor_msgs/Imu`)
> * `range` (from `sensor_msgs/Range`)
> * `data` (from `std_msgs/Float32`, `Int32`, etc.)

#### B1) Secure deployment (HTTPS, CA bundle, dual QoS, header stamps, many units)

```bash
python3 rdash_agent.py \
  --server https://dash.myrobot.net:8443 \
  --token 'rdash_secret_token_123' \
  --robot-name testsim-001 \
  --include '^(?:/drone/|/node2/|/robot/)' \
  --exclude '/rosout|/parameter_events|/debug' \
  --numeric-qos dual \
  --timestamp header \
  --unit '^twist\.linear\.(x|y|z)$=km/h:3.6' \
  --unit '^twist\.angular\.(x|y|z)$=deg/s:57.2958' \
  --unit '^imu\.linear_acceleration\.(x|y|z)$=m/s^2:1.0' \
  --unit '^range$=m:1.0' \
  --unit '^battery\.voltage$=V:1.0' \
  --pc2-summarize \
  --max-hz 15 \
  --max-metrics-per-push 64 \
  --ca-bundle /etc/ssl/certs/ca-bundle.crt
```

What this does:

* Connects to the **HTTPS** server and validates TLS using the provided CA bundle.
* Authenticates with the same bearer token as the server.
* Subscribes to only `/drone/*`, `/node2/*`, and `/robot/*`; excludes noisy ROS infra topics.
* Uses **dual** QoS (best-effort + reliable) for numeric topics to maximize reception.
* Uses **message header timestamps** for the `t` field (great for synchronized data).
* Applies **multiple unit transforms** in one command:

  * Linear velocity to **km/h** (`× 3.6`)
  * Angular velocity to **deg/s** (`× 57.2958`)
  * IMU linear acceleration to **m/s²** (`× 1.0`, i.e., just tagging the unit)
  * Sonar/lidar **range** in meters
  * Battery voltage in volts
* Summarizes `PointCloud2` to min/max/mean stats.
* Caps outgoing numeric rate (per sensor) at 15 Hz.
* Allows up to 64 metrics per push to avoid choking frontend/WS.

#### B2) Local dev (HTTP, insecure, broad include, practical unit rules, header=receive)

```bash
python3 rdash_agent.py \
  --server http://127.0.0.1:8080 \
  --token 'rdash_dev_token' \
  --robot-name testsim \
  --include '.*' \
  --exclude '/rosout|/parameter_events' \
  --numeric-qos best \
  --timestamp receive \
  --unit '^data$=m/s:1.0' \
  --unit '^range$=m:1.0' \
  --unit '^imu\.linear_acceleration\.(x|y|z)$=m/s^2:1.0' \
  --pc2-summarize \
  --max-hz 10 \
  --max-metrics-per-push 32 \
  --insecure-tls
```

What this does:

* Connects to the **HTTP** server, uses no TLS.
* Authenticates with the same bearer token as the server.
* Subscribes to all topics.
* Uses **best** QoS (best-effort).
* Uses **time received timestamps**.
* Applies units.
* Summarizes `PointCloud2` to min/max/mean stats.
* Caps outgoing numeric rate (per sensor) at 10 Hz.
* Allows up to 32 metrics per push to avoid choking frontend/WS.

---
*"Data should empower, not overwhelm"*
