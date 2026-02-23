# tests/manual/test_geo.py
from src.core.services.geo_service import GeoService

geo = GeoService()

# Testes
assert geo.haversine(-23.42, -51.91, -23.42, -51.91) == 0.0
assert geo.haversine(-23.42, -51.91, -23.43, -51.92) > 10  # ~15km
assert geo.haversine(999, 999, -23.42, -51.91) is None
assert geo.classify_distance_zone(1.5, 2.0, 5.0) == 'near'
assert geo.classify_distance_zone(3.0, 2.0, 5.0) == 'medium'
assert geo.classify_distance_zone(6.0, 2.0, 5.0) == 'far'
assert geo.extract_coordinates_from_address({"lat": -23.42, "lng": -51.91}) == (-23.42, -51.91)

print("✅ Todos os testes de GeoService passaram!")