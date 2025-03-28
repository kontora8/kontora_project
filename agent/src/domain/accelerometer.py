from dataclasses import dataclass


@dataclass
class Accelerometer:
    x: int
    y: int
    z: int
    air: int
    noise: int
