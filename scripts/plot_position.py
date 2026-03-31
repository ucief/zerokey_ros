import json
import math
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timezone

import requests
import matplotlib.pyplot as plt
from signalrcore.hub_connection_builder import HubConnectionBuilder

# eventHubUrl = "http://10.42.0.1:33001/hubs/eventHub"
eventHubUrl = "http://192.168.50.87:33001/hubs/eventHub"
# apiUrl = "http://10.42.0.1:5000/v3/"
apiUrl = "http://192.168.50.87:5000/v3/"

AUTH_ID = "Admin"
AUTH_SECRET = "ZeroKey_Admin1"

# How many recent points to keep in the plot
MAX_POINTS = 500
FPS_WINDOW_SECONDS = 5.0

PRINT_RAW_EVENT = False
PRINT_PARSED_EVENT = False
PRINT_STATUS = True

# Shared runtime state
state_lock = threading.Lock()
latest_state = {
    "position": None,
    "velocity": None,
    "orientation_quat": None,
    "orientation_yaw_deg": None,
    "event_timestamp": None,
    "local_timestamp": None,
    "received_at": None,
    "sequence": None,
    "category": None,
    "event_type": None,
    "source_mac": None,
    "gateway_uri": None,
    "total_events": 0,
    "event_timestamp_delta_ms": None,
    "fps_last_5s": 0.0,
    "last_error": None,
}

trajectory = deque(maxlen=MAX_POINTS)
recent_event_receipts = deque()


def log_info(message):
    print(f"[INFO] {message}")


def log_warn(message):
    print(f"[WARNING] {message}")


def log_error(message):
    print(f"[ERROR] {message}")


def parse_iso_utc(ts):
    if not ts:
        return None

    try:
        ts = ts.strip()

        # "2026-03-30T21:00:05.5238Z" -> aware UTC
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))

        dt = datetime.fromisoformat(ts)

        # If timestamp has no timezone, assume UTC
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)

        # Normalize any timezone to UTC
        return dt.astimezone(timezone.utc)

    except Exception:
        return None


def utc_now():
    return datetime.now(timezone.utc)


def update_event_metrics(received_at, event_timestamp):
    cutoff_ts = received_at.timestamp() - FPS_WINDOW_SECONDS

    while recent_event_receipts and recent_event_receipts[0] < cutoff_ts:
        recent_event_receipts.popleft()

    recent_event_receipts.append(received_at.timestamp())

    event_timestamp_delta_ms = None
    previous_event_timestamp = latest_state.get("event_timestamp")
    if previous_event_timestamp is not None and event_timestamp is not None:
        event_timestamp_delta_ms = (
            event_timestamp - previous_event_timestamp
        ).total_seconds() * 1000.0

    fps_last_5s = len(recent_event_receipts) / FPS_WINDOW_SECONDS
    return event_timestamp_delta_ms, fps_last_5s


def format_vector(vec, precision=3):
    if not isinstance(vec, (list, tuple)) or len(vec) < 3:
        return "n/a"
    return f"({vec[0]:.{precision}f}, {vec[1]:.{precision}f}, {vec[2]:.{precision}f})"


def format_quaternion(quat, precision=4):
    if not isinstance(quat, (list, tuple)) or len(quat) < 4:
        return "n/a"
    return f"({quat[0]:.{precision}f}, {quat[1]:.{precision}f}, {quat[2]:.{precision}f}, {quat[3]:.{precision}f})"


def quaternion_to_yaw_deg(quat):
    """
    Convert quaternion [w, x, y, z] to yaw (rotation about Z) in degrees.

    Returns:
        yaw_deg in range [-180, 180], or None if invalid.

    Assumes the tag sends quaternion as [w, x, y, z].
    """
    if not isinstance(quat, (list, tuple)) or len(quat) < 4:
        return None

    try:
        w, x, y, z = [float(v) for v in quat[:4]]

        # Optional normalization for robustness
        norm = math.sqrt(w*w + x*x + y*y + z*z)
        if norm < 1e-12:
            return None

        w /= norm
        x /= norm
        y /= norm
        z /= norm

        # Standard yaw extraction from quaternion [w, x, y, z]
        yaw_rad = math.atan2(
            2.0 * (w * z + x * y),
            1.0 - 2.0 * (y * y + z * z)
        )
        return math.degrees(yaw_rad)

    except Exception:
        return None


def pretty_print_json(data, prefix="[DATA]"):
    try:
        print(f"{prefix} {json.dumps(data, indent=2, sort_keys=True)}")
    except Exception:
        print(f"{prefix} {data}")


def request_with_logging(method, url, headers=None, body=None, timeout=10):
    log_info(f"Sending {method.upper()} request to: {url}")

    if headers:
        pretty_print_json(headers, prefix="[REQUEST HEADERS]")

    if body is not None:
        try:
            parsed = json.loads(body) if isinstance(body, str) else body
            pretty_print_json(parsed, prefix="[REQUEST BODY]")
        except Exception:
            print(f"[REQUEST BODY] {body}")

    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        data=body,
        timeout=timeout,
    )

    log_info(f"Received HTTP {response.status_code} from {url}")

    if not response.ok:
        log_warn(f"Request to {url} returned a non-success status code.")
        print("[RESPONSE TEXT]")
        print(response.text)

    return response


def authenticateConnection():
    log_info("Starting authentication flow.")

    token_headers = {
        "Content-Type": "application/json"
    }
    token_body = json.dumps({
        "grant_type": "client_credentials",
        "auth_id": AUTH_ID,
        "auth_secret": AUTH_SECRET
    })

    auth_response = request_with_logging(
        method="post",
        url=apiUrl + "auth/token",
        headers=token_headers,
        body=token_body
    )

    if not auth_response.ok:
        raise RuntimeError("Authentication failed while requesting access token.")

    auth_json = auth_response.json()
    auth_token = auth_json.get("access_token")
    if not auth_token:
        raise RuntimeError("Authentication response did not include access_token.")

    log_info("Bearer token retrieved successfully.")

    endpoint_headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + auth_token
    }
    endpoint_body = json.dumps({
        "QualityOfService": {
            "MaxUpdateRate": 20,
            "MaxThroughput": 10240,
            "AutotuneConnectionParameters": False
        },
        "Mode": "read",
        "Filters": [{"FilterTemplate": "position_events"}]
    })

    endpoint_response = request_with_logging(
        method="post",
        url=apiUrl + "events/connections",
        headers=endpoint_headers,
        body=endpoint_body
    )

    if not endpoint_response.ok:
        raise RuntimeError("Failed to create event connection / obtain EndpointID.")

    endpoint_json = endpoint_response.json()
    endpoint_id = endpoint_json.get("EndpointID")
    if not endpoint_id:
        raise RuntimeError("Endpoint response did not include EndpointID.")

    log_info(f"EndpointID retrieved successfully: {endpoint_id}")
    return endpoint_id


def on_open():
    log_info("Connection opened and SignalR handshake completed.")


def on_close():
    log_warn("Connection closed.")


def on_error(error):
    log_error(f"SignalR error: {error}")


def decode_payload(data):
    payload = data

    if isinstance(payload, list):
        if len(payload) == 0:
            raise ValueError("Received empty list payload.")
        if len(payload) == 1:
            payload = payload[0]

    if isinstance(payload, str):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected payload type: {type(payload)}")

    return payload


def on_event_received(data):
    received_at = utc_now()

    try:
        payload = decode_payload(data)
        if PRINT_RAW_EVENT:
            print("[RAW EVENT RECEIVED]")
            print(data)
            pretty_print_json(payload, prefix="[PARSED EVENT]")

        content = payload.get("Content", {})
        source = payload.get("Source", {})

        position = content.get("Position")
        velocity = content.get("Velocity")
        orientation_quat = content.get("Orientation")
        orientation_yaw_deg = quaternion_to_yaw_deg(orientation_quat)

        event_timestamp = parse_iso_utc(payload.get("Timestamp"))
        local_timestamp = parse_iso_utc(payload.get("LocalTimestamp"))

        with state_lock:
            event_timestamp_delta_ms, fps_last_5s = update_event_metrics(
                received_at,
                event_timestamp,
            )

            latest_state["position"] = position
            latest_state["velocity"] = velocity
            latest_state["orientation_quat"] = orientation_quat
            latest_state["orientation_yaw_deg"] = orientation_yaw_deg
            latest_state["event_timestamp"] = event_timestamp
            latest_state["local_timestamp"] = local_timestamp
            latest_state["received_at"] = received_at
            latest_state["sequence"] = content.get("Sequence")
            latest_state["category"] = payload.get("Category")
            latest_state["event_type"] = payload.get("Type")
            latest_state["source_mac"] = source.get("MAC")
            latest_state["gateway_uri"] = source.get("GatewayURI")
            latest_state["total_events"] += 1
            latest_state["event_timestamp_delta_ms"] = event_timestamp_delta_ms
            latest_state["fps_last_5s"] = fps_last_5s
            latest_state["last_error"] = None

            if isinstance(position, (list, tuple)) and len(position) >= 3:
                trajectory.append((position[0], position[1], position[2]))

    except Exception as exc:
        with state_lock:
            latest_state["last_error"] = str(exc)
        log_warn(f"Failed to process received event: {exc}")
        traceback.print_exc()


def status_printer_loop():
    while True:
        time.sleep(1)

        with state_lock:
            snapshot = dict(latest_state)

        if snapshot["last_error"]:
            print(f"[WARNING] Last event processing error: {snapshot['last_error']}")

        if snapshot["position"] is None:
            print("[STATUS] No position received yet.")
            continue

        now = utc_now()

        event_age_ms = None
        local_event_age_ms = None
        receive_gap_ms = None

        if snapshot["event_timestamp"] is not None:
            event_age_ms = (now - snapshot["event_timestamp"]).total_seconds() * 1000.0

        if snapshot["local_timestamp"] is not None:
            local_event_age_ms = (now - snapshot["local_timestamp"]).total_seconds() * 1000.0

        if snapshot["event_timestamp"] is not None and snapshot["received_at"] is not None:
            receive_gap_ms = (snapshot["received_at"] - snapshot["event_timestamp"]).total_seconds() * 1000.0

        yaw_text = (
            f"{snapshot['orientation_yaw_deg']:.1f} deg"
            if snapshot["orientation_yaw_deg"] is not None else "n/a"
        )

        event_age_text = f"{event_age_ms:.1f} ms" if event_age_ms is not None else "n/a"
        local_age_text = f"{local_event_age_ms:.1f} ms" if local_event_age_ms is not None else "n/a"
        receive_gap_text = f"{receive_gap_ms:.1f} ms" if receive_gap_ms is not None else "n/a"
        event_ts_delta_text = (
            f"{snapshot['event_timestamp_delta_ms']:.1f} ms"
            if snapshot["event_timestamp_delta_ms"] is not None else "n/a"
        )
        fps_text = f"{snapshot['fps_last_5s']:.2f}"

        print(
            "[STATUS] "
            f"time={now.isoformat()} | "
            f"event_ts={snapshot['event_timestamp'].isoformat() if snapshot['event_timestamp'] else 'n/a'} | "
            f"pos={format_vector(snapshot['position'])} | "
            f"vel={format_vector(snapshot['velocity'])} | "
            f"quat={format_quaternion(snapshot['orientation_quat'])} | "
            f"yaw={yaw_text} | "
            f"age_from_event_ts={event_age_text} | "
            f"age_from_local_ts={local_age_text} | "
            f"receive_latency={receive_gap_text} | "
            f"event_ts_delta={event_ts_delta_text} | "
            f"fps_last_5s={fps_text} | "
            f"seq={snapshot['sequence']} | "
            f"events={snapshot['total_events']}"
        )


def set_equal_axes_3d(ax, xs, ys, zs, padding_ratio=0.2):
    """
    Make all 3 axes use the same scale so distances look correct in meters.
    """
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    z_min, z_max = min(zs), max(zs)

    x_center = (x_min + x_max) / 2.0
    y_center = (y_min + y_max) / 2.0
    z_center = (z_min + z_max) / 2.0

    span_x = x_max - x_min
    span_y = y_max - y_min
    span_z = z_max - z_min

    max_span = max(span_x, span_y, span_z, 1.0)
    half_range = (max_span * (1.0 + padding_ratio)) / 2.0

    ax.set_xlim(x_center - half_range, x_center + half_range)
    ax.set_ylim(y_center - half_range, y_center + half_range)
    ax.set_zlim(z_center - half_range, z_center + half_range)

    try:
        ax.set_box_aspect((1, 1, 1))
    except Exception:
        pass


def plotting_loop():
    plt.ion()
    fig = plt.figure("Live 3D Position")
    ax = fig.add_subplot(111, projection="3d")

    line, = ax.plot([], [], [], label="Trajectory")
    point, = ax.plot([], [], [], marker="o", linestyle="None", label="Current Position")

    ax.set_xlabel("X (m)")
    ax.set_ylabel("Y (m)")
    ax.set_zlabel("Z (m)")
    ax.set_title("Real-Time Position")
    ax.legend()

    while True:
        with state_lock:
            points = list(trajectory)
            current_position = latest_state["position"]

        if points:
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            zs = [p[2] for p in points]

            line.set_data(xs, ys)
            line.set_3d_properties(zs)

            if current_position and len(current_position) >= 3:
                point.set_data([current_position[0]], [current_position[1]])
                point.set_3d_properties([current_position[2]])

            set_equal_axes_3d(ax, xs, ys, zs, padding_ratio=0.2)

        plt.pause(0.001)


def main():
    log_info("Building SignalR hub connection.")

    try:
        hub_connection = (
            HubConnectionBuilder()
            .with_url(
                eventHubUrl,
                options={
                    "access_token_factory": authenticateConnection
                }
            )
            .build()
        )
    except Exception as exc:
        log_error(f"Failed to build SignalR hub connection: {exc}")
        traceback.print_exc()
        return

    hub_connection.on_open(on_open)
    hub_connection.on_close(on_close)

    try:
        hub_connection.on_error(on_error)
    except Exception:
        log_warn("This signalrcore version does not support 'on_error' callback registration.")

    hub_connection.on("Event", on_event_received)

    printer_thread = threading.Thread(target=status_printer_loop, daemon=True)
    printer_thread.start()

    plot_thread = threading.Thread(target=plotting_loop, daemon=True)
    plot_thread.start()

    try:
        log_info(f"Starting connection to hub: {eventHubUrl}")
        hub_connection.start()
        log_info("Hub connection start requested.")
        log_info("Listening for events. Press Ctrl+C to stop.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log_warn("Keyboard interrupt received. Stopping connection.")
    except Exception as exc:
        log_error(f"Unexpected runtime error: {exc}")
        traceback.print_exc()
    finally:
        try:
            hub_connection.stop()
            log_info("Hub connection stopped cleanly.")
        except Exception as exc:
            log_warn(f"Failed to stop hub connection cleanly: {exc}")


if __name__ == "__main__":
    main()
