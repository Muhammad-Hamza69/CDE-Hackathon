"""geoLocation device template (Task 1.2): device_id, latitude, longitude, timestamp.

Seeded near the O2 Arena, London (51.5030 N, 0.0032 E), matching the PDF's
"O2 Arena London" IoT Device Simulator template. Each tick nudges the
device's position by a small random walk so devices plausibly wander around
the venue instead of teleporting.
"""
import random
from datetime import datetime, timezone

O2_ARENA_LAT = 51.5030
O2_ARENA_LON = 0.0032

# Keeps the random walk within roughly a few hundred meters of the venue.
STEP_DEGREES = 0.0006


class GeoLocationDevice:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.latitude = O2_ARENA_LAT + random.uniform(-0.001, 0.001)
        self.longitude = O2_ARENA_LON + random.uniform(-0.001, 0.001)

    def next_reading(self) -> dict:
        self.latitude += random.uniform(-STEP_DEGREES, STEP_DEGREES)
        self.longitude += random.uniform(-STEP_DEGREES, STEP_DEGREES)
        return {
            "device_id": self.device_id,
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
