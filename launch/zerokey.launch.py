from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.conditions import IfCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    ip = LaunchConfiguration("ip")
    publish_tf = LaunchConfiguration("publish_tf")
    start_fusion = LaunchConfiguration("start_fusion")
    zerokey_odom_topic = LaunchConfiguration("zerokey_odom_topic")
    odom_topic = LaunchConfiguration("odom_topic")
    fusion_publish_tf = LaunchConfiguration("fusion_publish_tf")
    odom_frame = LaunchConfiguration("odom_frame")

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
            DeclareLaunchArgument(
                "start_fusion",
                default_value="false",
                description="Start the ZeroKey/odom fusion node.",
            ),
            DeclareLaunchArgument(
                "zerokey_odom_topic",
                default_value="/zerokey/tags/ed_e5_83_3f_e3_75/odom",
                description="ZeroKey odometry topic used by the fusion node.",
            ),
            DeclareLaunchArgument(
                "odom_topic",
                default_value="/a300_00010/platform/odom/filtered",
                description="Robot odometry topic used by the fusion node.",
            ),
            DeclareLaunchArgument(
                "fusion_publish_tf",
                default_value="true",
                description="Publish the fused zerokey_world -> odom TF.",
            ),
            DeclareLaunchArgument(
                "odom_frame",
                default_value="odom",
                description="Child frame name for the fused transform output.",
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
            ),
            Node(
                package="zerokey_ros2",
                executable="zerokey_fusion_node",
                name="zerokey_fusion_node",
                output="screen",
                condition=IfCondition(start_fusion),
                parameters=[
                    {
                        "zerokey_odom_topic": zerokey_odom_topic,
                        "odom_topic": odom_topic,
                        "publish_tf": fusion_publish_tf,
                        "odom_frame": odom_frame,
                    }
                ],
            ),
        ]
    )
