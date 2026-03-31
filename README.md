# ZeroKey ROS 2 Node

This repo contains a ROS 2 Jazzy Python package named `zerokey_ros2`.

It connects to the ZeroKey EventHub API, subscribes to `position_events`, and publishes:

- `nav_msgs/msg/Odometry` on `zerokey/tags/<tag_id>/odom`
- `tf2` transforms from `zerokey_world` to `zerokey_tag_<tag_id>`

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
