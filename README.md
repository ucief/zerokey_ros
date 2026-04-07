# ZeroKey ROS 2 Node

This repo contains a ROS 2 Jazzy Python package named `zerokey_ros2`.

It connects to the ZeroKey EventHub API, subscribes to `position_events`, and publishes:

- `nav_msgs/msg/Odometry` on `zerokey/tags/<tag_id>/odom`
- `tf2` transforms from `zerokey_world` to `zerokey_tag_<tag_id>`

The package also provides a fusion node that subscribes to one ZeroKey odometry topic plus one robot odometry topic, runs the ZeroKey/odom EKF online, and publishes:

- `tf2` transform from `zerokey_world` to `odom`
- `nav_msgs/msg/Odometry` on `zerokey/odom_transform` carrying the estimated transform pose and covariance

Tag interfaces are created dynamically, so the node can handle all available mobile tags without hardcoding a fixed number.

## Parameters
- `ip`: ZeroKey server IP address
- `api_port`: REST API port, default `5000`
- `event_hub_port`: SignalR EventHub port, default `33001`
- `auth_id`: authentication ID
- `auth_secret`: authentication secret
- `world_frame`: parent TF frame, default `zerokey_world`
- `topic_prefix`: topic prefix, default `zerokey/tags`
- `filter_template`: default `position_events`
- `max_update_rate`: default `20`
- `max_throughput`: default `10240`
- `reconnect_delay_sec`: reconnect delay after failures, default `5.0`

## Build

```bash
source /opt/ros/jazzy/setup.bash
python3 -m pip install -r src/zerokey_ros2/requirements.txt
colcon build --packages-select zerokey_ros2
source install/setup.bash
```

`signalrcore` is required by the node and is installed from PyPI because there is
currently no `rosdep` key for it.

## Run

```bash
ros2 run zerokey_ros2 zerokey_node --ros-args -p ip:=192.168.50.87
```

Or with launch:

```bash
ros2 launch zerokey_ros2 zerokey.launch.py ip:=192.168.50.87
```

## Fusion Node

Run the fusion node directly:

```bash
ros2 run zerokey_ros2 zerokey_fusion_node --ros-args \
  -p zerokey_odom_topic:=/zerokey/tags/ed_e5_83_3f_e3_75/odom \
  -p odom_topic:=/a300_00010/platform/odom/filtered
```

Useful fusion parameters:

- `zerokey_odom_topic`: ZeroKey odometry topic to fuse
- `odom_topic`: robot odometry topic to fuse against ZeroKey
- `transform_topic`: output topic for the estimated `zerokey_world -> odom` transform pose, default `zerokey/odom_transform`
- `publish_tf`: whether to publish the fused TF, default `true`
- `zerokey_pos_time_offset_sec`: applies the ZeroKey position timing offset used in the notebook, default `-0.2`
- `heading_lookback_sec`: sliding window used for displacement-derived heading, default `0.75`
- `heading_min_displacement_m`: minimum ZeroKey displacement required before generating a yaw update, default `0.20`
- `processing_delay_sec`: queueing delay so out-of-order and time-offset measurements can still be processed in timestamp order, default `0.25`

Or start both nodes from launch:

```bash
ros2 launch zerokey_ros2 zerokey.launch.py \
  ip:=192.168.50.87 \
  start_fusion:=true \
  zerokey_odom_topic:=/zerokey/tags/ed_e5_83_3f_e3_75/odom \
  odom_topic:=/a300_00010/platform/odom/filtered
```

## Direct API Logger

If you want to inspect the ZeroKey stream without going through ROS topics, run:

```bash
ros2 run zerokey_ros2 api_debug --ip 192.168.50.87
```

This connects directly to the ZeroKey REST API and EventHub, logs every received payload to stdout, and writes:

- a CSV with receive timestamps, message timestamps, tag metadata, and the raw payload JSON
- a PNG timing plot showing arrival time, the timestamp carried in each message, arrival-minus-message lag, and the arrival rate averaged over a trailing 2 s window

Useful options:

- `--output-directory <dir>` to choose where the CSV and PNG are saved
- `--file-prefix <prefix>` to customize the output filenames
- `--rate-window-sec <sec>` to change the averaging window for the rate plot
