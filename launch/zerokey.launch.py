from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    ip = LaunchConfiguration("ip")

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "ip",
                default_value="192.168.50.87",
                description="ZeroKey server IP address",
            ),
            Node(
                package="zerokey_ros2",
                executable="zerokey_node",
                name="zerokey_node",
                output="screen",
                parameters=[
                    {
                        "ip": ip,
                    }
                ],
            )
        ]
    )
