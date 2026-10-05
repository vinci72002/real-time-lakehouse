"""V2 aircraft telemetry messages and a correlated flight generator.

Kafka records are flat JSON. aircraft_id and flight_id are the join keys
a later analytics project can use. This module does not build dimensions,
aggregates, or BI tables.

Normal samples follow a flight phase. Altitude, speed, and vertical speed
move together. Position advances along the route. Fuel remaining falls as
fuel flow accumulates. Engine temperature and oil pressure stay in a normal
band unless the caller injects an anomaly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


def iso_z(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


@dataclass(frozen=True)
class Phase:
    name: str
    duration_s: float
    altitude_ft: tuple[float, float]
    ground_speed_kts: tuple[float, float]
    fuel_flow_kg_h: float
    engine_temp_c: float
    # Share of the route distance covered during this phase.
    distance_share: float


PHASES: tuple[Phase, ...] = (
    Phase("taxi", 180, (0, 0), (12, 22), 620, 430, 0.01),
    Phase("takeoff", 50, (0, 1500), (22, 165), 3600, 860, 0.02),
    Phase("climb", 780, (1500, 35000), (165, 430), 3100, 790, 0.20),
    Phase("cruise", 2400, (35000, 35000), (450, 460), 2400, 680, 0.55),
    Phase("descent", 900, (35000, 4000), (430, 210), 1100, 560, 0.17),
    Phase("approach", 180, (4000, 800), (190, 140), 900, 520, 0.04),
    Phase("landing", 50, (800, 0), (140, 20), 700, 450, 0.01),
)

FLIGHT_DURATION_S = sum(phase.duration_s for phase in PHASES)


def _lerp(start: float, end: float, fraction: float) -> float:
    return start + (end - start) * fraction


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import atan2, cos, radians, sin, degrees

    phi1, phi2 = radians(lat1), radians(lat2)
    dlon = radians(lon2 - lon1)
    y = sin(dlon) * cos(phi2)
    x = cos(phi1) * sin(phi2) - sin(phi1) * cos(phi2) * cos(dlon)
    return degrees(atan2(y, x)) % 360


def _locate(elapsed_s: float) -> tuple[Phase, float, float]:
    """Return the phase, fraction inside it, and route fraction from 0 to 1."""
    elapsed = min(max(elapsed_s, 0.0), FLIGHT_DURATION_S)
    covered = 0.0
    route = 0.0
    for phase in PHASES:
        if elapsed <= covered + phase.duration_s or phase is PHASES[-1]:
            inside = 0.0 if phase.duration_s == 0 else (elapsed - covered) / phase.duration_s
            inside = min(max(inside, 0.0), 1.0)
            return phase, inside, min(route + phase.distance_share * inside, 1.0)
        covered += phase.duration_s
        route += phase.distance_share
    last = PHASES[-1]
    return last, 1.0, 1.0


def _fuel_remaining_kg(elapsed_s: float, fuel_start_kg: float) -> float:
    elapsed = min(max(elapsed_s, 0.0), FLIGHT_DURATION_S)
    covered = 0.0
    burned = 0.0
    for phase in PHASES:
        if elapsed <= covered + phase.duration_s:
            burned += phase.fuel_flow_kg_h * ((elapsed - covered) / 3600)
            break
        burned += phase.fuel_flow_kg_h * (phase.duration_s / 3600)
        covered += phase.duration_s
    return round(max(fuel_start_kg - burned, 0.0), 1)


def sample_message(
    *,
    event_id: str,
    aircraft_id: str,
    flight_id: str,
    event_time: datetime,
    elapsed_s: float,
    origin: tuple[float, float],
    dest: tuple[float, float],
    fuel_start_kg: float = 16800.0,
    engine_temp_c: float | None = None,
) -> dict:
    """One realistic sample. event_time is independent of elapsed_s so tests can shift it."""
    phase, inside, route = _locate(elapsed_s)
    altitude = _lerp(*phase.altitude_ft, inside)
    speed = _lerp(*phase.ground_speed_kts, inside)
    if phase.duration_s == 0:
        vertical_speed = 0.0
    else:
        vertical_speed = (
            (phase.altitude_ft[1] - phase.altitude_ft[0]) / phase.duration_s
        ) * 60
    latitude = _lerp(origin[0], dest[0], route)
    longitude = _lerp(origin[1], dest[1], route)
    temp = phase.engine_temp_c if engine_temp_c is None else engine_temp_c
    return {
        "event_id": event_id,
        "aircraft_id": aircraft_id,
        "flight_id": flight_id,
        "event_time": iso_z(event_time),
        "latitude": round(latitude, 5),
        "longitude": round(longitude, 5),
        "altitude_ft": round(altitude, 1),
        "ground_speed_kts": round(speed, 1),
        "vertical_speed_fpm": round(vertical_speed, 1),
        "heading_deg": round(
            _bearing_deg(latitude, longitude, dest[0], dest[1]), 1
        ),
        "engine_temp_c": round(temp, 1),
        "oil_pressure_psi": round(62 - (altitude / 35000) * 8, 1),
        "fuel_flow_kg_h": round(phase.fuel_flow_kg_h, 1),
        "fuel_remaining_kg": _fuel_remaining_kg(elapsed_s, fuel_start_kg),
        "outside_air_temp_c": round(15 - (altitude / 1000) * 1.98, 1),
    }


def message(
    event_id: str | None,
    aircraft_id: str | None,
    flight_id: str | None,
    event_time: datetime | None,
    *,
    altitude_ft: float = 35000,
    ground_speed_kts: float = 455,
    vertical_speed_fpm: float = 0,
    heading_deg: float = 180,
    engine_temp_c: float | None = 680,
    oil_pressure_psi: float = 58,
    fuel_flow_kg_h: float = 2400,
    fuel_remaining_kg: float = 12000,
    latitude: float = 31.2,
    longitude: float = 121.5,
    outside_air_temp_c: float = -54.3,
) -> dict:
    """Explicit V2 record for tests and scripted producers."""
    return {
        "event_id": event_id,
        "aircraft_id": aircraft_id,
        "flight_id": flight_id,
        "event_time": None if event_time is None else iso_z(event_time),
        "latitude": latitude,
        "longitude": longitude,
        "altitude_ft": altitude_ft,
        "ground_speed_kts": ground_speed_kts,
        "vertical_speed_fpm": vertical_speed_fpm,
        "heading_deg": heading_deg,
        "engine_temp_c": engine_temp_c,
        "oil_pressure_psi": oil_pressure_psi,
        "fuel_flow_kg_h": fuel_flow_kg_h,
        "fuel_remaining_kg": fuel_remaining_kg,
        "outside_air_temp_c": outside_air_temp_c,
    }


@dataclass
class FlightTrack:
    """One aircraft flying origin to dest, then starting another leg."""

    aircraft_id: str
    origin: tuple[float, float]
    dest: tuple[float, float]
    started: datetime
    elapsed_s: float = 0.0
    leg: int = 1
    fuel_start_kg: float = 16800.0
    last_message: dict | None = None

    @property
    def flight_id(self) -> str:
        return f"{self.aircraft_id}-{self.started.strftime('%Y%m%d')}-{self.leg:02d}"

    def advance(self, dt_s: float) -> dict:
        self.elapsed_s += dt_s
        if self.elapsed_s >= FLIGHT_DURATION_S:
            self.leg += 1
            self.started = datetime.now(timezone.utc)
            self.elapsed_s = 0.0
            self.fuel_start_kg = 16800.0
        event_time = self.started + timedelta(seconds=self.elapsed_s)
        msg = sample_message(
            event_id=_new_id(),
            aircraft_id=self.aircraft_id,
            flight_id=self.flight_id,
            event_time=event_time,
            elapsed_s=self.elapsed_s,
            origin=self.origin,
            dest=self.dest,
            fuel_start_kg=self.fuel_start_kg,
        )
        self.last_message = msg
        return msg


def _new_id() -> str:
    import uuid

    return str(uuid.uuid4())
