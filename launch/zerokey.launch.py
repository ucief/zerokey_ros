from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    ip = LaunchConfiguration("ip")
    publish_tf = LaunchConfiguration("publish_tf")

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "ip",
                default_value="192.168.50.87",
                description="ZeroKey server IP address",
            ),
            DeclareLaunchArgument(
                "publish_tf",
                default_value="false",
                description="Publish raw zerokey_world -> zerokey_tag TF.",
            ),
            Node(
                package="zerokey_ros2",
                executable="zerokey_node",
                name="zerokey_node",
                output="screen",
                parameters=[
                    {
                        "ip": ip,
                        "publish_tf": publish_tf,
                    }
                ],
            )
        ]
    )
