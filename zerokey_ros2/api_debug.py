#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import threading
import time
import traceback
from typing import Any, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import requests

try:
    from signalrcore.hub_connection_builder import HubConnectionBuilder
except ModuleNotFoundError as exc:
    if exc.name != "signalrcore":
        raise
    raise ModuleNotFoundError(
        "signalrcore is required by zerokey_ros2.api_debug. Install package dependencies "
        "with `python3 -m pip install -r requirements.txt` before running the script."
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Direct ZeroKey API/EventHub debug logger. No ROS subscription layer is used.",
    )
    parser.add_argument("--ip", default="10.42.0.1", help="ZeroKey server IP address.")
    parser.add_argument("--api-port", type=int, default=5000, help="ZeroKey REST API port.")
    parser.add_argument("--event-hub-port", type=int, default=33001, help="ZeroKey EventHub port.")
    parser.add_argument("--auth-id", default="AuthID", help="ZeroKey auth_id.")
    parser.add_argument("--auth-secret", default="DefaultSecret", help="ZeroKey auth_secret.")
    parser.add_argument("--filter-template", default="position_events", help="ZeroKey event filter template.")
    parser.add_argument("--max-update-rate", type=int, default=20, help="Requested MaxUpdateRate in the EventHub connection.")
    parser.add_argument("--max-throughput", type=int, default=10240, help="Requested MaxThroughput in the EventHub connection.")
    parser.add_argument(
        "--output-directory",
        default=str(Path.cwd() / "zerokey_api_debug"),
        help="Directory for the CSV and PNG outputs.",
    )
    parser.add_argument(
        "--file-prefix",
        default="zerokey_api_debug",
        help="Prefix for the generated CSV and PNG output files.",
    )
    parser.add_argument(
        "--rate-window-sec",
        type=float,
        default=2.0,
        help="Trailing averaging window in seconds for the plotted arrival rate.",
    )
    return parser.parse_args()


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


def format_ns_as_local_time(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(timestamp_ns * 1e-9, tz=timezone.utc).astimezone().isoformat(timespec="milliseconds")


def datetime_to_ns(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return int(dt.timestamp() * 1e9)


def decode_payload(data: Any) -> dict[str, Any]:
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


def pretty_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


@dataclass
class TimingSample:
    index: int
    receive_ns: int
    message_ns: Optional[int]


class ZeroKeyApiDebug:
    def __init__(self, args: argparse.Namespace) -> None:
        self._args = args
        self._hub_connection = None
        self._lock = threading.Lock()
        self._sample_count = 0
        self._timing_samples: list[TimingSample] = []

        timestamp_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_directory = Path(args.output_directory).expanduser()
        output_directory.mkdir(parents=True, exist_ok=True)
        file_prefix = args.file_prefix.strip() or "zerokey_api_debug"
        self._csv_path = output_directory / f"{file_prefix}_{timestamp_tag}.csv"
        self._plot_path = output_directory / f"{file_prefix}_{timestamp_tag}.png"

        self._csv_file = self._csv_path.open("w", encoding="utf-8", newline="")
        self._csv_writer = csv.DictWriter(
            self._csv_file,
            fieldnames=[
                "sample_index",
                "receive_time_iso",
                "receive_time_ns",
                "receive_time_sec",
                "zerokey_timestamp_iso",
                "zerokey_timestamp_ns",
                "zerokey_timestamp_sec",
                "local_timestamp_iso",
                "local_timestamp_ns",
                "arrival_minus_zerokey_sec",
                "category",
                "type",
                "tag_mac",
                "tag_id",
                "tag_name",
                "sequence",
                "position_x",
                "position_y",
                "position_z",
                "velocity_x",
                "velocity_y",
                "velocity_z",
                "payload_json",
            ],
        )
        self._csv_writer.writeheader()
        self._csv_file.flush()

    def api_url(self) -> str:
        return f"http://{self._args.ip}:{self._args.api_port}/v3/"

    def event_hub_url(self) -> str:
        return f"http://{self._args.ip}:{self._args.event_hub_port}/hubs/eventHub"

    def log(self, level: str, message: str) -> None:
        timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
        print(f"[{level}] {timestamp} {message}", flush=True)

    def request(self, method: str, url: str, headers=None, body=None, timeout: int = 10):
        return requests.request(
            method=method,
            url=url,
            headers=headers,
            data=body,
            timeout=timeout,
        )

    def authenticate_connection(self) -> str:
        auth_body = json.dumps(
            {
                "grant_type": "client_credentials",
                "auth_id": self._args.auth_id,
                "auth_secret": self._args.auth_secret,
            }
        )
        auth_response = self.request(
            method="post",
            url=self.api_url() + "auth/token",
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
                    "MaxUpdateRate": self._args.max_update_rate,
                    "MaxThroughput": self._args.max_throughput,
                    "AutotuneConnectionParameters": False,
                },
                "Mode": "read",
                "Filters": [
                    {
                        "FilterTemplate": self._args.filter_template,
                    }
                ],
            }
        )
        endpoint_response = self.request(
            method="post",
            url=self.api_url() + "events/connections",
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

    def on_open(self):
        self.log("INFO", f"Connected to ZeroKey EventHub at {self.event_hub_url()}")

    def on_close(self):
        self.log("WARN", "ZeroKey EventHub connection closed.")

    def on_error(self, error):
        self.log("ERROR", f"ZeroKey EventHub error: {error}")

    def on_event_received(self, data: Any):
        receive_ns = time.time_ns()
        try:
            payload = decode_payload(data)
            self._record_payload(payload, receive_ns)
        except Exception as exc:
            self.log("WARN", f"Failed to decode/process event payload: {exc}")
            traceback.print_exc()

    def _record_payload(self, payload: dict[str, Any], receive_ns: int) -> None:
        content = payload.get("Content") or {}
        source = payload.get("Source") or {}

        zerokey_dt = parse_iso_utc(payload.get("Timestamp"))
        local_dt = parse_iso_utc(payload.get("LocalTimestamp"))
        zerokey_ns = datetime_to_ns(zerokey_dt)
        local_ns = datetime_to_ns(local_dt)
        arrival_minus_zerokey = None if zerokey_ns is None else (receive_ns - zerokey_ns) * 1e-9

        position = content.get("Position")
        if not isinstance(position, (list, tuple)):
            position = [None, None, None]
        velocity = content.get("Velocity")
        if not isinstance(velocity, (list, tuple)):
            velocity = [None, None, None]

        with self._lock:
            self._sample_count += 1
            self._timing_samples.append(
                TimingSample(
                    index=self._sample_count,
                    receive_ns=receive_ns,
                    message_ns=zerokey_ns,
                )
            )

            self._csv_writer.writerow(
                {
                    "sample_index": self._sample_count,
                    "receive_time_iso": format_ns_as_local_time(receive_ns),
                    "receive_time_ns": receive_ns,
                    "receive_time_sec": f"{receive_ns * 1e-9:.9f}",
                    "zerokey_timestamp_iso": zerokey_dt.isoformat() if zerokey_dt else "",
                    "zerokey_timestamp_ns": zerokey_ns if zerokey_ns is not None else "",
                    "zerokey_timestamp_sec": f"{zerokey_ns * 1e-9:.9f}" if zerokey_ns is not None else "",
                    "local_timestamp_iso": local_dt.isoformat() if local_dt else "",
                    "local_timestamp_ns": local_ns if local_ns is not None else "",
                    "arrival_minus_zerokey_sec": f"{arrival_minus_zerokey:.9f}" if arrival_minus_zerokey is not None else "",
                    "category": payload.get("Category", ""),
                    "type": payload.get("Type", ""),
                    "tag_mac": source.get("MAC", ""),
                    "tag_id": source.get("TagID", "") or content.get("TagID", ""),
                    "tag_name": content.get("TagName", "") or content.get("Name", ""),
                    "sequence": content.get("Sequence", ""),
                    "position_x": position[0] if len(position) > 0 else "",
                    "position_y": position[1] if len(position) > 1 else "",
                    "position_z": position[2] if len(position) > 2 else "",
                    "velocity_x": velocity[0] if len(velocity) > 0 else "",
                    "velocity_y": velocity[1] if len(velocity) > 1 else "",
                    "velocity_z": velocity[2] if len(velocity) > 2 else "",
                    "payload_json": pretty_json(payload),
                }
            )
            self._csv_file.flush()

        lag_text = f"{arrival_minus_zerokey:.3f}s" if arrival_minus_zerokey is not None else "n/a"
        self.log(
            "INFO",
            f"sample={self._sample_count} recv={format_ns_as_local_time(receive_ns)} "
            f"zerokey_ts={payload.get('Timestamp', '')} lag={lag_text} "
            f"sequence={content.get('Sequence', '')} position={position}"
        )
        print(pretty_json(payload), flush=True)

    def build_hub_connection(self):
        hub_connection = (
            HubConnectionBuilder()
            .with_url(
                self.event_hub_url(),
                options={"access_token_factory": self.authenticate_connection},
            )
            .build()
        )
        hub_connection.on_open(self.on_open)
        hub_connection.on_close(self.on_close)

        try:
            hub_connection.on_error(self.on_error)
        except Exception:
            self.log("WARN", "This signalrcore version does not support on_error callback registration.")

        hub_connection.on("Event", self.on_event_received)
        return hub_connection

    def run(self) -> None:
        self.log("INFO", f"Writing CSV to {self._csv_path}")
        self.log("INFO", f"Plot will be written to {self._plot_path} on shutdown")
        self._hub_connection = self.build_hub_connection()
        self.log("INFO", f"Starting ZeroKey connection to {self.event_hub_url()}")
        self._hub_connection.start()
        self.log("INFO", "Listening for ZeroKey API events. Press Ctrl-C to stop.")

        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            self.log("WARN", "Keyboard interrupt received. Stopping connection.")
        finally:
            if self._hub_connection is not None:
                try:
                    self._hub_connection.stop()
                except Exception as exc:
                    self.log("WARN", f"Failed to stop hub connection cleanly: {exc}")
            self.finalize()

    def finalize(self) -> None:
        with self._lock:
            samples = list(self._timing_samples)
        self._csv_file.flush()
        self._csv_file.close()

        if not samples:
            self.log("WARN", f"No ZeroKey events received. CSV header written to {self._csv_path}. Plot skipped.")
            return

        self._create_plot(samples)
        self.log("INFO", f"Saved {len(samples)} samples to {self._csv_path}")
        self.log("INFO", f"Saved timing plot to {self._plot_path}")

    def _compute_trailing_rate_hz(
        self,
        receive_ns: np.ndarray,
    ) -> tuple[np.ndarray, float]:
        window_sec = max(float(self._args.rate_window_sec), 1.0e-3)
        window_ns = int(window_sec * 1e9)
        window_start_indices = np.searchsorted(receive_ns, receive_ns - window_ns, side="left")
        sample_indices = np.arange(receive_ns.size, dtype=float)
        counts = sample_indices - window_start_indices.astype(float) + 1.0
        return counts / window_sec, window_sec

    def _create_plot(self, samples: list[TimingSample]) -> None:
        receive_ns = np.asarray([sample.receive_ns for sample in samples], dtype=np.int64)

        valid_message_samples = [sample for sample in samples if sample.message_ns is not None]
        if valid_message_samples:
            message_receive_ns = np.asarray([sample.receive_ns for sample in valid_message_samples], dtype=np.int64)
            message_ns = np.asarray([sample.message_ns for sample in valid_message_samples], dtype=np.int64)
            zero_ns = min(int(receive_ns[0]), int(np.min(message_ns)))
        else:
            message_receive_ns = np.asarray([], dtype=np.int64)
            message_ns = np.asarray([], dtype=np.int64)
            zero_ns = int(receive_ns[0])

        receive_s = (receive_ns - zero_ns) * 1e-9
        message_receive_s = (message_receive_ns - zero_ns) * 1e-9
        message_s = (message_ns - zero_ns) * 1e-9
        rate_hz, rate_window_sec = self._compute_trailing_rate_hz(receive_ns)

        fig, (ax_times, ax_lag, ax_rate) = plt.subplots(
            3,
            1,
            figsize=(12, 10),
            sharex=True,
            gridspec_kw={"height_ratios": [2.0, 1.2, 1.2]},
        )

        ax_times.plot(
            receive_s,
            receive_s,
            label="Arrival timestamp",
            linewidth=1.8,
            color="#d62728",
        )
        if len(message_receive_s) > 0:
            ax_times.plot(
                message_receive_s,
                message_s,
                label="Message timestamp",
                linewidth=1.8,
                color="#1f77b4",
            )
        ax_times.set_ylabel("Timestamp [s]")
        ax_times.set_title("ZeroKey API timing and rate")
        ax_times.grid(True, alpha=0.3)
        ax_times.legend()

        if len(message_receive_s) > 0:
            lag_s = (message_receive_ns - message_ns) * 1e-9
            ax_lag.plot(message_receive_s, lag_s, linewidth=1.8, color="#2ca02c")
            lag_mean = float(np.mean(lag_s))
            lag_p95 = float(np.quantile(lag_s, 0.95))
            lag_max = float(np.max(lag_s))
            summary_text = (
                f"samples={len(samples)}  timed_samples={len(message_ns)}  "
                f"mean_lag={lag_mean:.3f}s  p95_lag={lag_p95:.3f}s  max_lag={lag_max:.3f}s"
            )
        else:
            ax_lag.text(
                0.5,
                0.5,
                "No valid ZeroKey message timestamps in received payloads.",
                ha="center",
                va="center",
                transform=ax_lag.transAxes,
            )
            summary_text = f"samples={len(samples)}  timed_samples=0"

        ax_lag.set_ylabel("Arrival - message [s]")
        ax_lag.grid(True, alpha=0.3)

        ax_rate.plot(receive_s, rate_hz, linewidth=1.8, color="#9467bd")
        ax_rate.set_xlabel("Arrival time since start [s]")
        ax_rate.set_ylabel(f"Rate [{rate_window_sec:.1f}s avg] [Hz]")
        ax_rate.grid(True, alpha=0.3)

        fig.suptitle(f"ZeroKey API Debug Timing\n{summary_text}")
        fig.tight_layout()
        fig.savefig(self._plot_path, dpi=150, bbox_inches="tight")
        plt.close(fig)


def main() -> None:
    args = parse_args()
    debugger = ZeroKeyApiDebug(args)
    debugger.run()


if __name__ == "__main__":
    main()
