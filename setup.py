from pathlib import Path

from setuptools import find_packages, setup


package_name = "zerokey_ros2"
requirements_path = Path(__file__).with_name("requirements.txt")


def load_requirements() -> list[str]:
    if not requirements_path.exists():
        return ["setuptools", "requests", "signalrcore"]

    return [
        line.strip()
        for line in requirements_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


setup(
    name=package_name,
    version="0.1.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", [f"resource/{package_name}"]),
        (f"share/{package_name}", ["package.xml"]),
        (f"share/{package_name}/launch", ["launch/zerokey.launch.py"]),
    ],
    install_requires=load_requirements(),
    zip_safe=True,
    maintainer="matthias",
    maintainer_email="matthias@example.com",
    description="ROS 2 Jazzy node for ZeroKey position events.",
    license="MIT",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "zerokey_node = zerokey_ros2.zerokey_node:main",
        ],
    },
)
