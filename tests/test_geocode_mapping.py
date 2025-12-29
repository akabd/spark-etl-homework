import etl.geocode as geo


def test_build_missing_coords_mapping_monkeypatch(spark, monkeypatch):
    def fake_geocode(query, api_key, country_code=None, timeout=10):
        return (10.0, 20.0)

    monkeypatch.setattr(geo, "opencage_geocode", fake_geocode)

    df = spark.createDataFrame(
        [(1, "IT", "Milan", None, None),
         (2, "IT", "Milan", None, None),
         (3, "IT", "Rome", 41.9, 12.5)],
        ["id", "country", "city", "lat", "lng"]
    )

    mapping = geo.build_missing_coords_mapping(df, api_key="x")
    assert mapping[("IT", "Milan")] == (10.0, 20.0)
    assert ("IT", "Rome") not in mapping
