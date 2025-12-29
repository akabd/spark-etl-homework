import time
import requests


def is_invalid_coord(lat, lng) -> bool:
    if lat is None or lng is None:
        return True
    try:
        lat = float(lat)
        lng = float(lng)
        return not (-90 <= lat <= 90 and -180 <= lng <= 180)
    except Exception:
        return True


def opencage_geocode(query: str, api_key: str, country_code: str | None = None, timeout=10):
    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {"q": query, "key": api_key, "limit": 1, "no_annotations": 1}
    if country_code:
        params["countrycode"] = country_code

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        return None
    geom = results[0].get("geometry")
    if not geom:
        return None
    return float(geom["lat"]), float(geom["lng"])


def build_missing_coords_mapping(df, api_key: str, country_code: str | None = None, rate_limit_sec=1.1):

    it = (
        df.select("country", "city", "lat", "lng")
          .dropna(subset=["country", "city"])
          .distinct()
          .toLocalIterator()
    )

    mapping = {}
    last_call = 0.0

    for row in it:
        ctr, city, lat, lng = row["country"], row["city"], row["lat"], row["lng"]
        if not is_invalid_coord(lat, lng):
            continue

        key = (ctr, city)
        if key in mapping:
            continue

        # simple rate limiting
        now = time.time()
        wait = max(0.0, rate_limit_sec - (now - last_call))
        if wait:
            time.sleep(wait)

        query = f"{city}, {ctr}"
        try:
            coords = opencage_geocode(query=query, api_key=api_key, country_code=country_code)
            if coords:
                mapping[key] = coords
        except Exception:
            pass
        finally:
            last_call = time.time()

    return mapping
