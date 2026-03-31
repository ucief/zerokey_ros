import json
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
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        dt = datetime.fromisoformat(value)
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
    return (0.0, 0.0, 0.0, 1.0)


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

        self._lock = threading.Lock()
        self._pending_events: Deque[Dict[str, Any]] = deque()
        self._odom_publishers: Dict[str, Odometry] = {}
        self._tag_frames: Dict[str, str] = {}
        self._unknown_counter = 0

        self._hub_connection = None
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._tf_broadcaster = TransformBroadcaster(self)

        self._process_timer = self.create_timer(0.05, self._process_pending_events)
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
            topic = f"{self._parameter_value('topic_prefix')}/{sanitized}/odom"
            self._odom_publishers[sanitized] = self.create_publisher(Odometry, topic, 10)
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
        orientation = content.get("Orientation")
        if not isinstance(orientation, (list, tuple)) or len(orientation) < 4:
            orientation = make_identity_quaternion()

        event_time = parse_iso_utc(payload.get("Timestamp"))
        stamp = datetime_to_ros_time(event_time)
        if stamp.nanoseconds == 0:
            stamp = self.get_clock().now()

        tag_key = self._extract_tag_key(payload)
        publisher, child_frame_id, sanitized_tag = self._get_publisher_for_tag(tag_key)
        world_frame = self._parameter_value("world_frame")

        # For details regarding the mapping of ZeroKey event data to ROS messages, see:
        # https://infocentre.zerokey.com/articles/utilizing-location-raw-update-events-in-python#UtilizingLocationRawUpdateEventsinPython-InterpretingtheOutput
        odom_msg = Odometry()
        odom_msg.header.stamp = stamp.to_msg()
        odom_msg.header.frame_id = world_frame
        odom_msg.child_frame_id = child_frame_id
        odom_msg.pose.pose.position.x = float(position[0])
        odom_msg.pose.pose.position.y = float(position[1])
        odom_msg.pose.pose.position.z = float(position[2])
        odom_msg.pose.pose.orientation.w = float(orientation[0])
        odom_msg.pose.pose.orientation.x = float(orientation[1])
        odom_msg.pose.pose.orientation.y = float(orientation[2])
        odom_msg.pose.pose.orientation.z = float(orientation[3])

        if isinstance(velocity, (list, tuple)) and len(velocity) >= 3:
            odom_msg.twist.twist.linear.x = float(velocity[0])
            odom_msg.twist.twist.linear.y = float(velocity[1])
            odom_msg.twist.twist.linear.z = float(velocity[2])

        publisher.publish(odom_msg)

        transform = TransformStamped()
        transform.header.stamp = odom_msg.header.stamp
        transform.header.frame_id = world_frame
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
