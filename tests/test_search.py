import json
import os
import pytest
from typing import List, Tuple, Dict

import uuid
import time

# Import the class
from ..airflow.pipelines.adaptive_search.core.search import AdaptiveSearch

# A fixture to patch all external calls
@pytest.fixture(autouse=True)
def fake_external(monkeypatch, tmp_path):
    # 1. Fake get_city_center => single tile
    def fake_get_city_center(city: str) -> Tuple[float, float, float]:
        return (12.34, 56.78, 0.1)  # lat, lng, viewport

    # 2. Fake tile generator => two tiles
    def fake_generate_initial_tiles(lat, lng, viewport, step) -> List[Tuple[float, float, float]]:
        return [(12.34, 56.78, step), (12.35, 56.79, step)]

    # 3. Fake place search: returns N dummy places, plus count/pages
    def fake_get_nearby_places(lat, lng, radius, loc_type) -> Tuple[List[Dict], int, int]:
        # Return two distinct place_ids on first tile,
        # then the same two plus one new on the second tile
        base = int((lat + lng) * 100)
        places = [
            {"place_id": f"p{base+1}", "name": "Test1"},
            {"place_id": f"p{base+2}", "name": "Test2"},
        ]
        # On the second tile, add one more
        if lat > 12.34:
            places.append({"place_id": "p999", "name": "Extra"})
        count = len(places)
        pages = 1
        return places, count, pages

    # 4. Redirect flush_chunk and save_search_state to tmp_path
    def fake_flush_chunk(city, chunk_buffer):
        path = tmp_path / f"chunk_{city}.json"
        with open(path, "w") as f:
            json.dump(chunk_buffer, f)

    def fake_save_search_state(city, tiles, high_stack, processed, seen_ids, deep_count):
        path = tmp_path / f"state_{city}.json"
        with open(path, "w") as f:
            json.dump({
                "tiles": tiles,
                "high": high_stack,
                "processed": list(processed),
                "seen": list(seen_ids),
                "deep": deep_count,
            }, f)
        return str(path)

    monkeypatch.setattr("yourmodule.get_city_center", fake_get_city_center)
    monkeypatch.setattr("yourmodule.generate_initial_tiles", fake_generate_initial_tiles)
    monkeypatch.setattr("yourmodule.get_nearby_places", fake_get_nearby_places)
    monkeypatch.setattr("yourmodule.flush_chunk", fake_flush_chunk)
    monkeypatch.setattr("yourmodule.save_search_state", fake_save_search_state)

    yield  # test runs here

@pytest.mark.usefixtures("fake_external")
def test_run_initial_and_deep(tmp_path):
    # 1. Instantiate with small max_deep_dives to stop early
    search = AdaptiveSearch(
        city="TestCity",
        initial_radius=0.01,
        initial_step=0.01,
        min_step=0.001,
        high_density_threshold=0,
        chunk_size=10
    )
    search.max_deep_dives = 1

    # 2. Kick off the entire run
    search.run()

    # 3. Assert final object state
    #   - We expect at least 3 unique place_ids: p1245, p1246, p999
    assert "p1245" in search.seen_place_ids
    assert "p1246" in search.seen_place_ids
    assert "p999" in search.seen_place_ids

    # 4. Assert that flush_chunk wrote at least one file
    files = list(tmp_path.glob("chunk_TestCity*.json"))
    assert files, "Expected at least one chunk file"

    # 5. Assert that save_search_state wrote a checkpoint
    state_files = list(tmp_path.glob("state_TestCity*.json"))
    assert state_files, "Expected to save state at least once"

    # 6. Optionally, read the last state file and check its contents
    with open(state_files[-1]) as f:
        data = json.load(f)
    assert data["seen"] == sorted(list(search.seen_place_ids))
