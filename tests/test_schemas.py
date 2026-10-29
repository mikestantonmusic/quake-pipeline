import pytest

from ingestion.schemas import validate_features


def test_real_features_are_valid(feed_sample):
    result = validate_features(feed_sample["features"])

    assert len(result.valid) == 3
    assert result.invalid == []


def test_null_magnitude_is_allowed(valid_feature):
    valid_feature["properties"]["mag"] = None

    assert len(validate_features([valid_feature]).valid) == 1


def test_unknown_properties_are_kept(valid_feature):
    valid_feature["properties"]["new_usgs_field"] = "x"

    [kept] = validate_features([valid_feature]).valid

    assert kept["properties"]["new_usgs_field"] == "x"


@pytest.mark.parametrize(
    "mutate, expected_error",
    [
        (lambda f: f.pop("id"), "id"),
        (lambda f: f["properties"].pop("time"), "properties.time"),
        (lambda f: f["properties"].update(updated="yesterday"), "properties.updated"),
        (lambda f: f["geometry"].update(coordinates=[10.0, 95.0, 5.0]), "latitude out of range"),
        (lambda f: f["geometry"].update(coordinates=[200.0, 10.0, 5.0]), "longitude out of range"),
        (lambda f: f["geometry"].update(coordinates=[10.0, 10.0]), "expected [lon, lat, depth]"),
    ],
)
def test_invalid_features_are_reported(valid_feature, mutate, expected_error):
    mutate(valid_feature)

    result = validate_features([valid_feature])

    assert result.valid == []
    [(_, error)] = result.invalid
    assert expected_error in error


def test_one_bad_feature_does_not_reject_the_batch(feed_sample):
    feed_sample["features"][1]["geometry"] = None

    result = validate_features(feed_sample["features"])

    assert len(result.valid) == 2
    assert [event_id for event_id, _ in result.invalid] == ["ci41568496"]
