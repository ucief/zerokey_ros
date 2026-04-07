import json
import math
import re
import threading
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional, Tuple

import requests
import rclpy
from geometry_msgs.msg import TransformStamped
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time
from tf2_ros import TransformBroadcaster

try:
    from signalrcore.hub_connection_builder import HubConnectionBuilder
except ModuleNotFoundError as exc:
    if exc.name != "signalrcore":
        raise
    raise ModuleNotFoundError(
        "signalrcore is required by zerokey_ros2. Install package dependencies "
        "with `python3 -m pip install -r src/zerokey_ros2/requirements.txt` "
        "before running the node."
    ) from exc


def parse_iso_utc(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None

    try:
        value = ts.strip()
        match = re.fullmatch(
            r"(?P<date>\d{4}-\d{2}-\d{2})T"
            r"(?P<time>\d{2}:\d{2}:\d{2})"
            r"(?P<fraction>\.\d+)?"
            r"(?P<tz>Z|[+-]\d{2}:\d{2})?",
            value,
        )
        if not match:
            return None

        fraction = match.group("fraction") or ""
        tz_suffix = match.group("tz") or ""

        if fraction:
            # Python 3.10 rejects some valid ISO8601 widths such as 4-digit fractions.
            fraction_digits = fraction[1:]
            fraction = "." + (fraction_digits[:6].ljust(6, "0"))

        normalized = f"{match.group('date')}T{match.group('time')}{fraction}"
        if tz_suffix == "Z":
            normalized += "+00:00"
        else:
            normalized += tz_suffix

        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def sanitize_name(value: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value).strip("_").lower()
    return sanitized or "unknown_tag"


def datetime_to_ros_time(dt: Optional[datetime]) -> Time:
    if dt is None:
        return Time()
    return Time(nanoseconds=int(dt.timestamp() * 1e9))


def make_identity_quaternion() -> Tuple[float, float, float, float]:
    return (1.0, 0.0, 0.0, 0.0)


def covariance_diagonal_to_matrix(
    values: Any,
    fallback: Tuple[float, float, float, float, float, float],
) -> list[float]:
    matrix = [0.0] * 36

    if not isinstance(values, (list, tuple)) or len(values) != 6:
        values = fallback

    sanitized = []
    for default_value, value in zip(fallback, values):
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            numeric_value = float(default_value)

        if not math.isfinite(numeric_value) or numeric_value < 0.0:
            numeric_value = float(default_value)
        sanitized.append(numeric_value)

    for index, diagonal_value in enumerate(sanitized):
        matrix[index * 6 + index] = diagonal_value

    return matrix


def sanitize_quaternion(value: Any) -> Tuple[float, float, float, float]:
    if not isinstance(value, (list, tuple)) or len(value) < 4:
        return make_identity_quaternion()

    try:
        quaternion = tuple(float(component) for component in value[:4])
    except (TypeError, ValueError):
        return make_identity_quaternion()

    if not all(math.isfinite(component) for component in quaternion):
        return make_identity_quaternion()

    norm = math.sqrt(sum(component * component for component in quaternion))
    if norm <= 1e-9:
        return make_identity_quaternion()

    return tuple(component / norm for component in quaternion)


class ZeroKeyNode(Node):
    def __init__(self) -> None:
        super().__init__("zerokey_node")

        self.declare_parameter("ip", "10.42.0.1")
        self.declare_parameter("api_port", 5000)
        self.declare_parameter("event_hub_port", 33001)
        self.declare_parameter("auth_id", "LocalAdmin")
        self.declare_parameter("auth_secret", "DefaultSecret")
        self.declare_parameter("world_frame", "zerokey_world")
        self.declare_parameter("topic_prefix", "zerokey/tags")
        self.declare_parameter("filter_template", "position_events")
        self.declare_parameter("max_update_rate", 20)
        self.declare_parameter("max_throughput", 10240)
        self.declare_parameter("reconnect_delay_sec", 5.0)
        self.declare_parameter("publish_tf", False)
        self.declare_parameter(
            "pose_covariance_diagonal",
            [0.008, 0.008, 0.10, 1.0e6, 1.0e6, 1.0e6],
        )
        self.declare_parameter(
            "twist_covariance_diagonal",
            [0.25, 0.25, 0.25, 1.0e6, 1.0e6, 1.0e6],
        )

        self._lock = threading.Lock()
        self._pending_events: Deque[Dict[str, Any]] = deque()
        self._odom_publishers: Dict[str, Odometry] = {}
        self._tag_frames: Dict[str, str] = {}
        self._unknown_counter = 0

        self._hub_connection = None
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._tf_broadcaster = TransformBroadcaster(self)
        self._publisher_qos = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=1,
            reliability=QoSReliabilityPolicy.RELIABLE,
        )
        self._world_frame = str(self._parameter_value("world_frame"))
        self._topic_prefix = str(self._parameter_value("topic_prefix"))
        self._publish_tf = bool(self._parameter_value("publish_tf"))
        self._pose_covariance = covariance_diagonal_to_matrix(
            self._parameter_value("pose_covariance_diagonal"),
            (0.008, 0.008, 0.10, 1.0e6, 1.0e6, 1.0e6),
        )
        self._twist_covariance = covariance_diagonal_to_matrix(
            self._parameter_value("twist_covariance_diagonal"),
            (0.25, 0.25, 0.25, 1.0e6, 1.0e6, 1.0e6),
        )
        self._event_guard = self.create_guard_condition(self._process_pending_events)

        self._connection_thread = threading.Thread(
            target=self._connection_worker,
            daemon=True,
        )
        self._connection_thread.start()

    def destroy_node(self) -> bool:
        self._stop_event.set()
        self._reconnect_event.set()
        self._stop_hub_connection()
        return super().destroy_node()

    def _parameter_value(self, name: str) -> Any:
        return self.get_parameter(name).value

    def _base_url(self, port_param: str) -> str:
        return f"http://{self._parameter_value('ip')}:{self._parameter_value(port_param)}"

    def _api_url(self) -> str:
        return f"{self._base_url('api_port')}/v3/"

    def _event_hub_url(self) -> str:
        return f"{self._base_url('event_hub_port')}/hubs/eventHub"

    def _request(self, method: str, url: str, headers=None, body=None, timeout: int = 10):
        return requests.request(
            method=method,
            url=url,
            headers=headers,
            data=body,
            timeout=timeout,
        )

    def _authenticate_connection(self) -> str:
        auth_body = json.dumps(
            {
                "grant_type": "client_credentials",
                "auth_id": self._parameter_value("auth_id"),
                "auth_secret": self._parameter_value("auth_secret"),
            }
        )
        auth_response = self._request(
            method="post",
            url=self._api_url() + "auth/token",
            headers={"Content-Type": "application/json"},
            body=auth_body,
        )
        auth_response.raise_for_status()

        auth_token = auth_response.json().get("access_token")
        if not auth_token:
            raise RuntimeError("Missing access_token in authentication response.")

        endpoint_body = json.dumps(
            {
                "QualityOfService": {
                    "MaxUpdateRate": int(self._parameter_value("max_update_rate")),
                    "MaxThroughput": int(self._parameter_value("max_throughput")),
                    "AutotuneConnectionParameters": False,
                },
                "Mode": "read",
                "Filters": [
                    {
                        "FilterTemplate": self._parameter_value("filter_template"),
                    }
                ],
            }
        )
        endpoint_response = self._request(
            method="post",
            url=self._api_url() + "events/connections",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {auth_token}",
            },
            body=endpoint_body,
        )
        endpoint_response.raise_for_status()

        endpoint_id = endpoint_response.json().get("EndpointID")
        if not endpoint_id:
            raise RuntimeError("Missing EndpointID in endpoint response.")
        return endpoint_id

    def _decode_payload(self, data: Any) -> Dict[str, Any]:
        payload = data
        if isinstance(payload, list):
            if not payload:
                raise ValueError("Received empty list payload.")
            if len(payload) == 1:
                payload = payload[0]

        if isinstance(payload, str):
            payload = json.loads(payload)

        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected payload type: {type(payload)}")
        return payload

    def _on_open(self):
        self.get_logger().info(f"Connected to ZeroKey hub at {self._event_hub_url()}.")
        self._reconnect_event.clear()

    def _on_close(self):
        self.get_logger().warning("ZeroKey hub connection closed.")
        self._reconnect_event.set()

    def _on_error(self, error):
        self.get_logger().error(f"ZeroKey hub error: {error}")
        self._reconnect_event.set()

    def _on_event_received(self, data: Any):
        try:
            payload = self._decode_payload(data)
            with self._lock:
                self._pending_events.append(payload)
            self._event_guard.trigger()
        except Exception as exc:
            self.get_logger().warning(f"Failed to decode event payload: {exc}")
            self.get_logger().debug(traceback.format_exc())

    def _stop_hub_connection(self) -> None:
        if self._hub_connection is None:
            return

        try:
            self._hub_connection.stop()
        except Exception as exc:
            self.get_logger().warning(f"Failed to stop ZeroKey hub cleanly: {exc}")
        finally:
            self._hub_connection = None

    def _build_hub_connection(self):
        hub_connection = (
            HubConnectionBuilder()
            .with_url(
                self._event_hub_url(),
                options={"access_token_factory": self._authenticate_connection},
            )
            .build()
        )
        hub_connection.on_open(self._on_open)
        hub_connection.on_close(self._on_close)

        try:
            hub_connection.on_error(self._on_error)
        except Exception:
            self.get_logger().warning(
                "This signalrcore version does not support on_error callback registration."
            )

        hub_connection.on("Event", self._on_event_received)
        return hub_connection

    def _connection_worker(self) -> None:
        while rclpy.ok() and not self._stop_event.is_set():
            try:
                self._hub_connection = self._build_hub_connection()
                self.get_logger().info(f"Starting ZeroKey connection to {self._event_hub_url()}.")
                self._hub_connection.start()
            except Exception as exc:
                self.get_logger().error(f"Failed to connect to ZeroKey: {exc}")
                self.get_logger().debug(traceback.format_exc())
                self._stop_hub_connection()
                if self._stop_event.wait(float(self._parameter_value("reconnect_delay_sec"))):
                    break
                continue

            while rclpy.ok() and not self._stop_event.is_set():
                if self._reconnect_event.wait(timeout=0.5):
                    self._reconnect_event.clear()
                    break

            self._stop_hub_connection()

            if self._stop_event.wait(float(self._parameter_value("reconnect_delay_sec"))):
                break

    def _extract_tag_key(self, payload: Dict[str, Any]) -> str:
        source = payload.get("Source") or {}
        content = payload.get("Content") or {}

        candidates = [
            source.get("MAC"),
            source.get("TagMAC"),
            source.get("TagID"),
            content.get("TagID"),
            content.get("TagName"),
            content.get("Name"),
        ]
        for candidate in candidates:
            if candidate:
                return str(candidate)

        self._unknown_counter += 1
        return f"tag_{self._unknown_counter}"

    def _get_publisher_for_tag(self, tag_key: str):
        sanitized = sanitize_name(tag_key)
        if sanitized not in self._odom_publishers:
            topic = f"{self._topic_prefix}/{sanitized}/odom"
            self._odom_publishers[sanitized] = self.create_publisher(
                Odometry,
                topic,
                self._publisher_qos,
            )
            self._tag_frames[sanitized] = f"zerokey_tag_{sanitized}"
            self.get_logger().info(
                f"Created ROS interfaces for tag '{tag_key}' on topic '{topic}'."
            )
        return self._odom_publishers[sanitized], self._tag_frames[sanitized], sanitized

    def _process_pending_events(self) -> None:
        pending = []
        with self._lock:
            while self._pending_events:
                pending.append(self._pending_events.popleft())

        for payload in pending:
            try:
                self._publish_event(payload)
            except Exception as exc:
                self.get_logger().warning(f"Failed to publish ZeroKey event: {exc}")
                self.get_logger().debug(traceback.format_exc())

    def _publish_event(self, payload: Dict[str, Any]) -> None:
        content = payload.get("Content") or {}
        position = content.get("Position")
        if not isinstance(position, (list, tuple)) or len(position) < 3:
            return

        velocity = content.get("Velocity")
        raw_orientation = content.get("Orientation")
        orientation = sanitize_quaternion(raw_orientation)

        event_time = parse_iso_utc(payload.get("Timestamp"))
        stamp = datetime_to_ros_time(event_time)
        if stamp.nanoseconds == 0:
            stamp = self.get_clock().now()

        tag_key = self._extract_tag_key(payload)
        publisher, child_frame_id, sanitized_tag = self._get_publisher_for_tag(tag_key)

        # For details regarding the mapping of ZeroKey event data to ROS messages, see:
        # https://infocentre.zerokey.com/articles/utilizing-location-raw-update-events-in-python#UtilizingLocationRawUpdateEventsinPython-InterpretingtheOutput
        odom_msg = Odometry()
        odom_msg.header.stamp = stamp.to_msg()
        odom_msg.header.frame_id = self._world_frame
        odom_msg.child_frame_id = child_frame_id
        odom_msg.pose.pose.position.x = float(position[0])
        odom_msg.pose.pose.position.y = float(position[1])
        odom_msg.pose.pose.position.z = float(position[2])
        odom_msg.pose.pose.orientation.w = float(orientation[0])
        odom_msg.pose.pose.orientation.x = float(orientation[1])
        odom_msg.pose.pose.orientation.y = float(orientation[2])
        odom_msg.pose.pose.orientation.z = float(orientation[3])
        odom_msg.pose.covariance = self._pose_covariance

        if isinstance(velocity, (list, tuple)) and len(velocity) >= 3:
            odom_msg.twist.twist.linear.x = float(velocity[0])
            odom_msg.twist.twist.linear.y = float(velocity[1])
            odom_msg.twist.twist.linear.z = float(velocity[2])
        odom_msg.twist.covariance = self._twist_covariance

        publisher.publish(odom_msg)

        if self._publish_tf:
            transform = TransformStamped()
            transform.header.stamp = odom_msg.header.stamp
            transform.header.frame_id = self._world_frame
            transform.child_frame_id = child_frame_id
            transform.transform.translation.x = odom_msg.pose.pose.position.x
            transform.transform.translation.y = odom_msg.pose.pose.position.y
            transform.transform.translation.z = odom_msg.pose.pose.position.z
            transform.transform.rotation = odom_msg.pose.pose.orientation
            self._tf_broadcaster.sendTransform(transform)

        self.get_logger().debug(
            f"Published tag '{sanitized_tag}' sequence={content.get('Sequence')}."
        )


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ZeroKeyNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()
