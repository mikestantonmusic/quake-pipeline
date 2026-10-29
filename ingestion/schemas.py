"""Validation for USGS GeoJSON earthquake features.

Only the fields the pipeline depends on are checked; any other properties are kept as-is.
Invalid features are reported, not raised, so one bad record doesn't fail a whole batch.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class Geometry(BaseModel):
    type: str
    coordinates: list[float | None]

    @field_validator("coordinates")
    @classmethod
    def lon_lat_depth(cls, v: list[float | None]) -> list[float | None]:
        if len(v) != 3:
            raise ValueError(f"expected [lon, lat, depth], got {len(v)} values")
        lon, lat, _depth = v
        if lon is None or not -180 <= lon <= 180:
            raise ValueError(f"longitude out of range: {lon}")
        if lat is None or not -90 <= lat <= 90:
            raise ValueError(f"latitude out of range: {lat}")
        return v


class Properties(BaseModel):
    model_config = ConfigDict(extra="allow")

    time: int  # epoch milliseconds
    updated: int  # epoch milliseconds; increases each time USGS revises the event
    mag: float | None = None
    status: str | None = None
    type: str | None = None


class Feature(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(min_length=1)
    geometry: Geometry
    properties: Properties


@dataclass
class ValidationResult:
    valid: list[dict] = field(default_factory=list)
    invalid: list[tuple[str | None, str]] = field(default_factory=list)  # (event id, error)


def validate_features(features: list[dict]) -> ValidationResult:
    """Split raw GeoJSON features into valid ones and (id, error) pairs for invalid ones."""
    result = ValidationResult()
    for feature in features:
        try:
            Feature.model_validate(feature)
        except ValidationError as exc:
            event_id = feature.get("id") if isinstance(feature, dict) else None
            errors = "; ".join(
                f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
            )
            result.invalid.append((event_id, errors))
        else:
            result.valid.append(feature)
    return result
