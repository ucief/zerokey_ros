from __future__ import annotations

import heapq
import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

import numpy as np
import rclpy
from geometry_msgs.msg import TransformStamped
from nav_msgs.msg import Odometry
from rclpy.node import Node
from rclpy.qos import QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.time import Time
from tf2_ros import TransformBroadcaster


PX, PY, VX, VY, PSIW, OMEGA, OTX, OTY, OYAW = range(9)


@dataclass
class MeasurementEvent:
    time_sec: float
    kind: str
    z: np.ndarray
    r: Optional[np.ndarray] = None


@dataclass
class ZeroKeySample:
    time_sec: float
    x: float
    y: float
    vx: float
    vy: float
    wz: float


@dataclass
class OdomSample:
    time_sec: float
    x: float
    y: float
    yaw: float
    yaw_cont: float
    vx: float
    wz: float


@dataclass
class MotionYawSample:
    time_sec: float
    psi_wrapped: float
    psi_cont: float
    psi_var: float


def stamp_to_seconds(stamp) -> float:
    return float(stamp.sec) + 1.0e-9 * float(stamp.nanosec)


def yaw_from_quaternion(q) -> float:
    x = float(q.x)
    y = float(q.y)
    z = float(q.z)
    w = float(q.w)
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def quaternion_from_yaw(yaw: float) -> tuple[float, float, float, float]:
    half_yaw = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half_yaw), math.cos(half_yaw))


def rot2(theta: float) -> np.ndarray:
    c = math.cos(theta)
    s = math.sin(theta)
    return np.array([[c, -s], [s, c]], dtype=float)


def wrap_angle(theta: float) -> float:
    return math.atan2(math.sin(theta), math.cos(theta))


def unwrap_to_reference(theta: float, reference: float) -> float:
    return float(reference + wrap_angle(theta - reference))


def circular_mean(values: np.ndarray) -> float:
    if len(values) == 0:
        return 0.0
    values = values.astype(float)
    return float(math.atan2(np.sin(values).mean(), np.cos(values).mean()))


class ZeroKeyFusionNode(Node):
    def __init__(self) -> None:
        super().__init__("zerokey_fusion_node")

        self.declare_parameter("zerokey_odom_topic", "/zerokey/tags/ed_e5_83_3f_e3_75/odom")
        self.declare_parameter("odom_topic", "/a300_00010/platform/odom/filtered")
        self.declare_parameter("world_frame", "zerokey_world")
        self.declare_parameter("odom_frame", "odom")
        self.declare_parameter("transform_topic", "zerokey/odom_transform")
        self.declare_parameter("publish_tf", True)
        self.declare_parameter("processing_delay_sec", 0.25)
        self.declare_parameter("sync_tolerance_sec", 0.10)
        self.declare_parameter("zerokey_pos_time_offset_sec", -0.2)
        self.declare_parameter("heading_lookback_sec", 0.75)
        self.declare_parameter("heading_min_displacement_m", 0.20)
        self.declare_parameter("odom_reverse_speed_threshold_mps", 0.05)
        self.declare_parameter("history_duration_sec", 60.0)
        self.declare_parameter("initial_transform_min_samples", 5)
        self.declare_parameter("zerokey_pos_std_m", 0.04)
        self.declare_parameter("zerokey_vel_std_mps", 0.10)
        self.declare_parameter("zerokey_wz_std_radps", math.radians(2.5))
        self.declare_parameter("odom_pos_std_m", 0.40)
        self.declare_parameter("odom_heading_std_rad", math.radians(7.5))
        self.declare_parameter("odom_speed_std_mps", 0.04)
        self.declare_parameter("odom_wz_std_radps", math.radians(3.0))
        self.declare_parameter("q_pos", 1.0e-4)
        self.declare_parameter("q_vel", 5.0e-2)
        self.declare_parameter("q_psi_w", 1.0e-3)
        self.declare_parameter("q_omega", 5.0e-3)
        self.declare_parameter("q_odom_offset", 5.0e-6)
        self.declare_parameter("q_odom_yaw", 1.0e-5)
        self.declare_parameter("unobserved_covariance", 1.0e6)

        self._zerokey_odom_topic = str(self.get_parameter("zerokey_odom_topic").value)
        self._odom_topic = str(self.get_parameter("odom_topic").value)
        self._world_frame = str(self.get_parameter("world_frame").value)
        self._odom_frame = str(self.get_parameter("odom_frame").value)
        self._transform_topic = str(self.get_parameter("transform_topic").value)
        self._publish_tf = bool(self.get_parameter("publish_tf").value)
        self._processing_delay_sec = float(self.get_parameter("processing_delay_sec").value)
        self._sync_tolerance_sec = float(self.get_parameter("sync_tolerance_sec").value)
        self._zerokey_pos_time_offset_sec = float(self.get_parameter("zerokey_pos_time_offset_sec").value)
        self._heading_lookback_sec = float(self.get_parameter("heading_lookback_sec").value)
        self._heading_min_displacement_m = float(self.get_parameter("heading_min_displacement_m").value)
        self._odom_reverse_speed_threshold_mps = float(
            self.get_parameter("odom_reverse_speed_threshold_mps").value
        )
        self._history_duration_sec = float(self.get_parameter("history_duration_sec").value)
        self._initial_transform_min_samples = int(self.get_parameter("initial_transform_min_samples").value)
        self._unobserved_covariance = float(self.get_parameter("unobserved_covariance").value)

        zerokey_pos_std = float(self.get_parameter("zerokey_pos_std_m").value)
        zerokey_vel_std = float(self.get_parameter("zerokey_vel_std_mps").value)
        zerokey_wz_std = float(self.get_parameter("zerokey_wz_std_radps").value)
        odom_pos_std = float(self.get_parameter("odom_pos_std_m").value)
        odom_heading_std = float(self.get_parameter("odom_heading_std_rad").value)
        odom_speed_std = float(self.get_parameter("odom_speed_std_mps").value)
        odom_wz_std = float(self.get_parameter("odom_wz_std_radps").value)

        self._q_pos = float(self.get_parameter("q_pos").value)
        self._q_vel = float(self.get_parameter("q_vel").value)
        self._q_psi_w = float(self.get_parameter("q_psi_w").value)
        self._q_omega = float(self.get_parameter("q_omega").value)
        self._q_odom_offset = float(self.get_parameter("q_odom_offset").value)
        self._q_odom_yaw = float(self.get_parameter("q_odom_yaw").value)

        self._r_zerokey_pos = np.diag([zerokey_pos_std**2, zerokey_pos_std**2])
        self._r_zerokey_vel = np.diag([zerokey_vel_std**2, zerokey_vel_std**2])
        self._r_zerokey_wz = np.array([[zerokey_wz_std**2]], dtype=float)
        self._r_odom_pos = np.diag([odom_pos_std**2, odom_pos_std**2])
        self._r_odom_heading = np.array([[odom_heading_std**2]], dtype=float)
        self._r_odom_speed = np.array([[odom_speed_std**2]], dtype=float)
        self._r_odom_wz = np.array([[odom_wz_std**2]], dtype=float)

        self._h_zerokey_pos = np.zeros((2, 9), dtype=float)
        self._h_zerokey_pos[:, [PX, PY]] = np.eye(2)
        self._h_world_vel = np.zeros((2, 9), dtype=float)
        self._h_world_vel[:, [VX, VY]] = np.eye(2)
        self._h_transform_yaw = np.zeros((1, 9), dtype=float)
        self._h_transform_yaw[0, OYAW] = 1.0
        self._h_omega = np.zeros((1, 9), dtype=float)
        self._h_omega[0, OMEGA] = 1.0
        self._h_odom_heading = np.zeros((1, 9), dtype=float)
        self._h_odom_heading[0, PSIW] = 1.0
        self._h_odom_heading[0, OYAW] = -1.0

        qos = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=50,
            reliability=QoSReliabilityPolicy.RELIABLE,
        )

        self._zerokey_history: Deque[ZeroKeySample] = deque()
        self._odom_history: Deque[OdomSample] = deque()
        self._motion_yaw_history: Deque[MotionYawSample] = deque()
        self._event_queue: list[tuple[float, int, MeasurementEvent]] = []
        self._event_counter = 0
        self._max_observed_time_sec = float("-inf")
        self._last_odom_yaw_raw: Optional[float] = None
        self._last_odom_yaw_cont: Optional[float] = None
        self._last_motion_yaw_cont: Optional[float] = None
        self._last_motion_yaw_event_time_sec: Optional[float] = None

        self._x: Optional[np.ndarray] = None
        self._p: Optional[np.ndarray] = None
        self._last_processed_time_sec: Optional[float] = None
        self._last_publish_time_sec: Optional[float] = None
        self._initialized = False
        self._initialization_logged = False

        self._tf_broadcaster = TransformBroadcaster(self)
        self._transform_publisher = self.create_publisher(Odometry, self._transform_topic, qos)
        self.create_subscription(Odometry, self._zerokey_odom_topic, self._handle_zerokey_odom, qos)
        self.create_subscription(Odometry, self._odom_topic, self._handle_odom, qos)
        self.create_timer(0.02, self._process_events)

        self.get_logger().info(
            f"ZeroKey fusion node listening to '{self._zerokey_odom_topic}' and '{self._odom_topic}'."
        )

    def _handle_zerokey_odom(self, msg: Odometry) -> None:
        stamp_sec = self._message_time_sec(msg)
        self._max_observed_time_sec = max(self._max_observed_time_sec, stamp_sec)

        sample = ZeroKeySample(
            time_sec=stamp_sec + self._zerokey_pos_time_offset_sec,
            x=float(msg.pose.pose.position.x),
            y=float(msg.pose.pose.position.y),
            vx=float(msg.twist.twist.linear.x),
            vy=float(msg.twist.twist.linear.y),
            wz=float(msg.twist.twist.angular.z),
        )
        self._zerokey_history.append(sample)
        self._enqueue_event(
            MeasurementEvent(
                time_sec=sample.time_sec,
                kind="zerokey_pos",
                z=np.array([sample.x, sample.y], dtype=float),
            )
        )
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="zerokey_vel",
                z=np.array([sample.vx, sample.vy], dtype=float),
            )
        )
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="zerokey_wz",
                z=np.array([sample.wz], dtype=float),
            )
        )

        self._maybe_enqueue_motion_yaw_for_sample(sample)

        self._trim_histories(reference_time_sec=self._max_observed_time_sec)
        self._maybe_initialize_filter()

    def _handle_odom(self, msg: Odometry) -> None:
        stamp_sec = self._message_time_sec(msg)
        self._max_observed_time_sec = max(self._max_observed_time_sec, stamp_sec)

        raw_yaw = yaw_from_quaternion(msg.pose.pose.orientation)
        if self._last_odom_yaw_raw is None or self._last_odom_yaw_cont is None:
            yaw_cont = raw_yaw
        else:
            yaw_cont = unwrap_to_reference(raw_yaw, self._last_odom_yaw_cont)
        self._last_odom_yaw_raw = raw_yaw
        self._last_odom_yaw_cont = yaw_cont

        sample = OdomSample(
            time_sec=stamp_sec,
            x=float(msg.pose.pose.position.x),
            y=float(msg.pose.pose.position.y),
            yaw=raw_yaw,
            yaw_cont=yaw_cont,
            vx=float(msg.twist.twist.linear.x),
            wz=float(msg.twist.twist.angular.z),
        )
        self._odom_history.append(sample)
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="odom_pos",
                z=np.array([sample.x, sample.y], dtype=float),
            )
        )
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="odom_heading",
                z=np.array([sample.yaw_cont], dtype=float),
            )
        )
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="odom_speed",
                z=np.array([sample.vx], dtype=float),
            )
        )
        self._enqueue_event(
            MeasurementEvent(
                time_sec=stamp_sec,
                kind="odom_wz",
                z=np.array([sample.wz], dtype=float),
            )
        )

        if self._zerokey_history:
            self._maybe_enqueue_motion_yaw_for_sample(self._zerokey_history[-1])

        self._trim_histories(reference_time_sec=self._max_observed_time_sec)
        self._maybe_initialize_filter()

    def _message_time_sec(self, msg: Odometry) -> float:
        stamp_sec = stamp_to_seconds(msg.header.stamp)
        if stamp_sec <= 0.0:
            return self.get_clock().now().nanoseconds * 1.0e-9
        return stamp_sec

    def _enqueue_event(self, event: MeasurementEvent) -> None:
        heapq.heappush(self._event_queue, (event.time_sec, self._event_counter, event))
        self._event_counter += 1

    def _trim_histories(self, reference_time_sec: float) -> None:
        min_time = reference_time_sec - self._history_duration_sec
        while self._zerokey_history and self._zerokey_history[0].time_sec < min_time:
            self._zerokey_history.popleft()
        while self._odom_history and self._odom_history[0].time_sec < min_time:
            self._odom_history.popleft()
        while self._motion_yaw_history and self._motion_yaw_history[0].time_sec < min_time:
            self._motion_yaw_history.popleft()
        while self._event_queue and self._event_queue[0][0] < min_time:
            heapq.heappop(self._event_queue)

    def _build_motion_yaw_event(self, current_sample: ZeroKeySample) -> Optional[MeasurementEvent]:
        best_prev: Optional[ZeroKeySample] = None
        best_distance = -1.0
        for candidate in reversed(self._zerokey_history):
            if candidate is current_sample:
                continue
            dt = current_sample.time_sec - candidate.time_sec
            if dt <= 0.0:
                continue
            if dt > self._heading_lookback_sec:
                break
            dx = current_sample.x - candidate.x
            dy = current_sample.y - candidate.y
            distance = math.hypot(dx, dy)
            if distance >= self._heading_min_displacement_m and distance > best_distance:
                best_prev = candidate
                best_distance = distance

        if best_prev is None:
            return None

        odom_sample = self._nearest_odom_sample(current_sample.time_sec)
        if odom_sample is None:
            return None
        if abs(odom_sample.time_sec - current_sample.time_sec) > self._sync_tolerance_sec:
            return None

        dx = current_sample.x - best_prev.x
        dy = current_sample.y - best_prev.y
        displacement_norm_sq = dx * dx + dy * dy
        if displacement_norm_sq <= 1.0e-9:
            return None

        reverse_offset = math.pi if odom_sample.vx < -self._odom_reverse_speed_threshold_mps else 0.0
        body_heading_world = wrap_angle(math.atan2(dy, dx) + reverse_offset)
        psi_wrapped = wrap_angle(body_heading_world - odom_sample.yaw)
        if self._last_motion_yaw_cont is None:
            psi_cont = psi_wrapped
        else:
            psi_cont = unwrap_to_reference(psi_wrapped, self._last_motion_yaw_cont)
        self._last_motion_yaw_cont = psi_cont

        jacobian = np.array([[-dy / displacement_norm_sq, dx / displacement_norm_sq]], dtype=float)
        heading_var = float((jacobian @ (2.0 * self._r_zerokey_pos) @ jacobian.T)[0, 0])
        psi_var = float(heading_var + self._r_odom_heading[0, 0])
        self._motion_yaw_history.append(
            MotionYawSample(
                time_sec=current_sample.time_sec,
                psi_wrapped=psi_wrapped,
                psi_cont=psi_cont,
                psi_var=psi_var,
            )
        )
        return MeasurementEvent(
            time_sec=current_sample.time_sec,
            kind="motion_yaw",
            z=np.array([psi_cont], dtype=float),
            r=np.array([[psi_var]], dtype=float),
        )

    def _maybe_enqueue_motion_yaw_for_sample(self, sample: ZeroKeySample) -> None:
        if self._last_motion_yaw_event_time_sec is not None:
            if sample.time_sec <= self._last_motion_yaw_event_time_sec + 1.0e-6:
                return
        motion_event = self._build_motion_yaw_event(sample)
        if motion_event is None:
            return
        self._enqueue_event(motion_event)
        self._last_motion_yaw_event_time_sec = sample.time_sec

    def _nearest_odom_sample(self, time_sec: float) -> Optional[OdomSample]:
        if not self._odom_history:
            return None
        best_sample = None
        best_dt = float("inf")
        for sample in reversed(self._odom_history):
            dt = abs(sample.time_sec - time_sec)
            if dt < best_dt:
                best_dt = dt
                best_sample = sample
            if sample.time_sec <= time_sec and dt > best_dt:
                break
        return best_sample

    def _maybe_initialize_filter(self) -> None:
        if self._initialized:
            return
        if len(self._motion_yaw_history) < self._initial_transform_min_samples:
            if not self._initialization_logged:
                self.get_logger().info("Waiting for enough motion to initialize ZeroKey fusion.")
                self._initialization_logged = True
            return
        if not self._zerokey_history or not self._odom_history or not self._event_queue:
            return

        yaw_guess = circular_mean(
            np.array([sample.psi_wrapped for sample in self._motion_yaw_history], dtype=float)
        )

        translation_samples = []
        for zerokey_sample in self._zerokey_history:
            odom_sample = self._nearest_odom_sample(zerokey_sample.time_sec)
            if odom_sample is None:
                continue
            if abs(odom_sample.time_sec - zerokey_sample.time_sec) > self._sync_tolerance_sec:
                continue
            world_xy = np.array([zerokey_sample.x, zerokey_sample.y], dtype=float)
            odom_xy = np.array([odom_sample.x, odom_sample.y], dtype=float)
            translation_samples.append(world_xy - rot2(yaw_guess) @ odom_xy)

        if not translation_samples:
            return

        initial_zerokey = self._zerokey_history[0]
        initial_odom = self._nearest_odom_sample(initial_zerokey.time_sec)
        if initial_odom is None:
            return

        translation_guess = np.median(np.vstack(translation_samples), axis=0)
        initial_heading_world = float(initial_odom.yaw_cont + yaw_guess)

        self._x = np.zeros(9, dtype=float)
        self._x[[PX, PY]] = np.array([initial_zerokey.x, initial_zerokey.y], dtype=float)
        self._x[[VX, VY]] = np.array([initial_zerokey.vx, initial_zerokey.vy], dtype=float)
        self._x[PSIW] = initial_heading_world
        self._x[OMEGA] = float(initial_zerokey.wz)
        self._x[[OTX, OTY]] = translation_guess
        self._x[OYAW] = yaw_guess

        self._p = np.diag(
            [
                0.5**2,
                0.5**2,
                0.5**2,
                0.5**2,
                math.radians(30.0) ** 2,
                math.radians(20.0) ** 2,
                1.5**2,
                1.5**2,
                math.radians(45.0) ** 2,
            ]
        )
        self._last_processed_time_sec = initial_zerokey.time_sec
        while self._event_queue and self._event_queue[0][0] < self._last_processed_time_sec:
            heapq.heappop(self._event_queue)
        self._initialized = True
        self.get_logger().info(
            "Initialized ZeroKey fusion with "
            f"{len(self._motion_yaw_history)} motion-yaw samples, "
            f"translation = [{translation_guess[0]:.3f}, {translation_guess[1]:.3f}] m, "
            f"yaw = {math.degrees(yaw_guess):.2f} deg."
        )

    def _process_events(self) -> None:
        if not self._initialized:
            self._maybe_initialize_filter()
            return
        if self._x is None or self._p is None or self._last_processed_time_sec is None:
            return

        watermark = self._max_observed_time_sec - self._processing_delay_sec
        if not math.isfinite(watermark):
            return

        while self._event_queue and self._event_queue[0][0] <= watermark:
            _, _, event = heapq.heappop(self._event_queue)
            dt = max(event.time_sec - self._last_processed_time_sec, 0.0)
            self._predict_step(dt)
            self._apply_measurement(event)
            self._last_processed_time_sec = max(self._last_processed_time_sec, event.time_sec)
            self._publish_transform(event.time_sec)

    def _predict_step(self, dt: float) -> None:
        assert self._x is not None
        assert self._p is not None
        if dt <= 0.0:
            return

        f = np.eye(9, dtype=float)
        f[PX, VX] = dt
        f[PY, VY] = dt
        f[PSIW, OMEGA] = dt

        self._x = f @ self._x

        q = np.diag(
            [
                self._q_pos * dt,
                self._q_pos * dt,
                self._q_vel * dt,
                self._q_vel * dt,
                self._q_psi_w * dt,
                self._q_omega * dt,
                self._q_odom_offset * dt,
                self._q_odom_offset * dt,
                self._q_odom_yaw * dt,
            ]
        )
        self._p = f @ self._p @ f.T + q

    def _apply_measurement(self, event: MeasurementEvent) -> None:
        assert self._x is not None
        assert self._p is not None

        if event.kind == "zerokey_pos":
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_zerokey_pos, self._r_zerokey_pos)
        elif event.kind == "zerokey_vel":
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_world_vel, self._r_zerokey_vel)
        elif event.kind == "motion_yaw":
            measurement_r = event.r if event.r is not None else self._r_odom_heading
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_transform_yaw, measurement_r)
        elif event.kind == "zerokey_wz":
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_omega, self._r_zerokey_wz)
        elif event.kind == "odom_pos":
            self._x, self._p = self._kf_update_nonlinear(
                self._x,
                self._p,
                event.z,
                self._odom_pos_model,
                self._odom_pos_jacobian,
                self._r_odom_pos,
            )
        elif event.kind == "odom_heading":
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_odom_heading, self._r_odom_heading)
        elif event.kind == "odom_speed":
            self._x, self._p = self._kf_update_nonlinear(
                self._x,
                self._p,
                event.z,
                self._odom_forward_speed_model,
                self._odom_forward_speed_jacobian,
                self._r_odom_speed,
            )
        elif event.kind == "odom_wz":
            self._x, self._p = self._kf_update(self._x, self._p, event.z, self._h_omega, self._r_odom_wz)

    def _kf_update(
        self,
        x: np.ndarray,
        p: np.ndarray,
        z: np.ndarray,
        h: np.ndarray,
        r: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        innovation = z - h @ x
        s = h @ p @ h.T + r
        k = p @ h.T @ np.linalg.inv(s)
        x_new = x + k @ innovation
        identity = np.eye(len(x))
        p_new = (identity - k @ h) @ p @ (identity - k @ h).T + k @ r @ k.T
        return x_new, p_new

    def _kf_update_nonlinear(
        self,
        x: np.ndarray,
        p: np.ndarray,
        z: np.ndarray,
        h_fn,
        h_jacobian_fn,
        r: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        h = h_jacobian_fn(x)
        innovation = z - h_fn(x)
        s = h @ p @ h.T + r
        k = p @ h.T @ np.linalg.inv(s)
        x_new = x + k @ innovation
        identity = np.eye(len(x))
        p_new = (identity - k @ h) @ p @ (identity - k @ h).T + k @ r @ k.T
        return x_new, p_new

    def _odom_pos_model(self, x: np.ndarray) -> np.ndarray:
        return rot2(float(x[OYAW])).T @ (x[[PX, PY]] - x[[OTX, OTY]])

    def _odom_pos_jacobian(self, x: np.ndarray) -> np.ndarray:
        yaw = float(x[OYAW])
        c = math.cos(yaw)
        s = math.sin(yaw)
        dx = float(x[PX] - x[OTX])
        dy = float(x[PY] - x[OTY])
        h = np.zeros((2, 9), dtype=float)
        h[0, PX] = c
        h[0, PY] = s
        h[1, PX] = -s
        h[1, PY] = c
        h[0, OTX] = -c
        h[0, OTY] = -s
        h[1, OTX] = s
        h[1, OTY] = -c
        h[0, OYAW] = -s * dx + c * dy
        h[1, OYAW] = -c * dx - s * dy
        return h

    def _odom_forward_speed_model(self, x: np.ndarray) -> np.ndarray:
        psi_w = float(x[PSIW])
        return np.array([math.cos(psi_w) * x[VX] + math.sin(psi_w) * x[VY]], dtype=float)

    def _odom_forward_speed_jacobian(self, x: np.ndarray) -> np.ndarray:
        psi_w = float(x[PSIW])
        c = math.cos(psi_w)
        s = math.sin(psi_w)
        h = np.zeros((1, 9), dtype=float)
        h[0, VX] = c
        h[0, VY] = s
        h[0, PSIW] = -s * x[VX] + c * x[VY]
        return h

    def _publish_transform(self, time_sec: float) -> None:
        if self._x is None or self._p is None:
            return

        stamp = Time(nanoseconds=int(time_sec * 1.0e9)).to_msg()
        quat_x, quat_y, quat_z, quat_w = quaternion_from_yaw(float(self._x[OYAW]))

        transform_msg = TransformStamped()
        transform_msg.header.stamp = stamp
        transform_msg.header.frame_id = self._world_frame
        transform_msg.child_frame_id = self._odom_frame
        transform_msg.transform.translation.x = float(self._x[OTX])
        transform_msg.transform.translation.y = float(self._x[OTY])
        transform_msg.transform.translation.z = 0.0
        transform_msg.transform.rotation.x = quat_x
        transform_msg.transform.rotation.y = quat_y
        transform_msg.transform.rotation.z = quat_z
        transform_msg.transform.rotation.w = quat_w
        if self._publish_tf:
            self._tf_broadcaster.sendTransform(transform_msg)

        odom_msg = Odometry()
        odom_msg.header.stamp = stamp
        odom_msg.header.frame_id = self._world_frame
        odom_msg.child_frame_id = self._odom_frame
        odom_msg.pose.pose.position.x = float(self._x[OTX])
        odom_msg.pose.pose.position.y = float(self._x[OTY])
        odom_msg.pose.pose.position.z = 0.0
        odom_msg.pose.pose.orientation.x = quat_x
        odom_msg.pose.pose.orientation.y = quat_y
        odom_msg.pose.pose.orientation.z = quat_z
        odom_msg.pose.pose.orientation.w = quat_w
        odom_msg.pose.covariance = self._transform_pose_covariance().tolist()
        odom_msg.twist.covariance = self._unknown_twist_covariance().tolist()
        self._transform_publisher.publish(odom_msg)
        self._last_publish_time_sec = time_sec

    def _transform_pose_covariance(self) -> np.ndarray:
        assert self._p is not None
        covariance = np.zeros(36, dtype=float)
        for index in (2, 3, 4):
            covariance[index * 6 + index] = self._unobserved_covariance

        covariance[0 * 6 + 0] = float(self._p[OTX, OTX])
        covariance[0 * 6 + 1] = float(self._p[OTX, OTY])
        covariance[0 * 6 + 5] = float(self._p[OTX, OYAW])
        covariance[1 * 6 + 0] = float(self._p[OTY, OTX])
        covariance[1 * 6 + 1] = float(self._p[OTY, OTY])
        covariance[1 * 6 + 5] = float(self._p[OTY, OYAW])
        covariance[5 * 6 + 0] = float(self._p[OYAW, OTX])
        covariance[5 * 6 + 1] = float(self._p[OYAW, OTY])
        covariance[5 * 6 + 5] = float(self._p[OYAW, OYAW])
        return covariance

    def _unknown_twist_covariance(self) -> np.ndarray:
        covariance = np.zeros(36, dtype=float)
        for index in range(6):
            covariance[index * 6 + index] = self._unobserved_covariance
        return covariance


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ZeroKeyFusionNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()
