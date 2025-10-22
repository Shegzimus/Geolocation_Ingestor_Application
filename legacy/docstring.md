# Restaurant Search Script Documentation

## Overview

This script implements an adaptive, density-based spatial search algorithm to comprehensively collect restaurant data from the Google Places API for a specified city. It uses a tile-based approach with intelligent subdivision to handle high-density areas efficiently while minimizing API calls.

## Core Algorithm

### Search Strategy

The script employs a two-phase adaptive search approach:

1. **Initial Grid Scan**: Divides the city into a uniform grid of tiles and searches each one
2. **Adaptive Deep Dive**: Recursively subdivides high-density tiles until either:
   - The minimum resolution is reached (`MIN_STEP`)
   - The API returns fewer than 60 results (indicating complete coverage)

### High-Density Detection

A tile is marked as "high-density" when:
- The API returns exactly 60 results (20 per page × 3 pages)
- All 3 pages of results are returned

This indicates the search area contains more restaurants than can be retrieved in a single query.

## Key Features

### 1. Checkpoint System

**Purpose**: Resume interrupted searches without losing progress

**Checkpoint Data**:
- `initial_tiles`: Complete grid covering the city
- `high_density_stack`: Queue of tiles requiring subdivision
- `processed`: Set of already-searched tile coordinates
- `seen_place_ids`: Unique restaurant identifiers
- `deep_count`: Number of deep dive iterations performed

**Storage**: Binary pickle format with atomic writes (temp file → rename)

**Naming**: `checkpoint_{city}_{timestamp}.ckpt`

### 2. Chunked CSV Writing

**Purpose**: Avoid memory overflow and enable incremental data access

**Mechanism**:
- Buffer holds up to `CHUNK_SIZE` (default: 500) restaurants
- Automatically flushes to CSV when buffer is full
- Appends to existing file if present

**Output Format**: `{city}_restaurants_{timestamp}.csv`

### 3. Structured JSON Logging

**Purpose**: Enable programmatic analysis of search performance

**Log Components**:
- Timestamp, log level, logger name
- Operation type (e.g., "nearby_search", "geocode")
- Unique session/search IDs for traceability
- API metrics (requests, results, efficiency ratios)
- Error details with context

**Output**: `logs/depth_first_restaurant_search_{timestamp}.log`

### 4. API Usage Tracking

**Metrics Collected**:
- `total_requests`: Number of API calls made
- `results_returned`: Total places returned (including duplicates)
- `unique_results`: New places found (deduplicated)
- `efficiency_ratio`: unique_results / results_returned

## Configuration Constants

| Constant | Default | Description |
|----------|---------|-------------|
| `INITIAL_RADIUS` | 2000m | Starting search radius (unused in current implementation) |
| `INITIAL_STEP` | 0.015° | Grid tile size (~1.1km at equator) |
| `MIN_STEP` | 0.0025° | Minimum tile size for subdivision |
| `TYPE` | "restaurant" | Google Places type filter |
| `HIGH_DENSITY_THRESHOLD` | 60 | Result count triggering subdivision |
| `MAX_RESULTS_PER_PAGE` | 20 | Google API limit per page |
| `MAX_PAGES` | 3 | Google API pagination limit |
| `CHUNK_SIZE` | 500 | CSV write buffer size |

## Key Functions

### `get_city_center(city: str) -> tuple[float, float, dict]`

Geocodes a city name to coordinates and viewport bounds.

**Returns**: (latitude, longitude, viewport_dict)

**API**: Google Geocoding API

---

### `get_nearby_places(lat, lng, radius, type_) -> tuple[list, int, int]`

Searches for places within a radius of coordinates.

**Returns**: (places_list, result_count, page_count)

**API**: Google Places Nearby Search API

**Pagination**: Automatically follows `next_page_token` up to 3 pages

---

### `generate_initial_tiles(center_lat, center_lng, viewport, initial_step) -> list`

Creates a uniform grid of tiles covering the city bounds with padding.

**Padding**: ±0.05° beyond viewport to ensure complete coverage

**Returns**: List of (lat, lng, size) tuples

---

### `subdivide_tile(tile) -> list`

Divides a tile into 4 equal quadrants (SW, SE, NW, NE).

**Input**: (lat, lng, size)

**Output**: 4 tiles with size/2, positioned at corners

---

### `calculate_search_radius(lat, size) -> float`

Converts tile size from degrees to meters for API radius parameter.

**Formula**: `radius = (size/2) × √2 × meters_per_degree_avg`

**Approximation**: Uses 111km/degree with latitude-based longitude correction

---

### `collect_all_places_adaptive(city: str) -> None`

**Main orchestration function** that executes the complete search workflow:

1. Load checkpoint or initialize state
2. Geocode city center and generate initial tiles
3. **Phase 1**: Scan all initial tiles
   - Search each tile
   - Deduplicate results
   - Identify high-density areas
   - Flush chunks periodically
   - Save checkpoint
4. **Phase 2**: Deep dive into high-density stack
   - Pop tile from stack
   - Search tile
   - Subdivide if still at max results and above min resolution
   - Continue until stack is empty
5. Final flush and cleanup

---

### `save_search_state(city, initial_tiles, high_density_stack, processed, seen_place_ids, deep_count) -> None`

Persists search state to checkpoint file with atomic write pattern.

---

### `load_search_state(city: str) -> dict | None`

Loads checkpoint data if available, returns None if not found.

---

### `flush_chunk(city: str, chunk_buffer: list) -> None`

Writes buffered results to CSV (append mode if file exists).

## Data Flow

```
City Name
    ↓
Geocoding → City Center + Viewport
    ↓
Generate Initial Tiles (Grid)
    ↓
┌─────────────────────────────────────┐
│ For Each Tile:                      │
│   - Search (API)                    │
│   - Deduplicate                     │
│   - Buffer Results                  │
│   - Check Density                   │
│   - Flush if Buffer Full            │
│   - Mark High-Density → Stack       │
└─────────────────────────────────────┘
    ↓
Checkpoint (State Saved)
    ↓
┌─────────────────────────────────────┐
│ While High-Density Stack Not Empty: │
│   - Pop Tile                        │
│   - Search (API)                    │
│   - Deduplicate                     │
│   - Buffer Results                  │
│   - Subdivide if Still Maxed Out    │
│   - Periodic Checkpoint             │
└─────────────────────────────────────┘
    ↓
Final Flush → CSV
    ↓
Cleanup Checkpoint Files
```

## Error Handling

### API Rate Limiting
- **Detection**: `OVER_QUERY_LIMIT` status
- **Response**: 5-second wait, continue
- **Logging**: Critical error logged

### Checkpoint Failures
- **Write**: Uses temp file + atomic rename
- **Read**: Returns None, falls back to fresh start
- **Corruption**: Logged as error, temp file cleaned up

### Geocoding Failures
- **Missing City**: Raises ValueError
- **Network Errors**: Propagates exception with logging

## Resume Capability

The script can resume from an interrupted search by loading the checkpoint for a specific city:

```python
state = load_search_state(city="london_20250422_200815")
```

**Resume Detection**:
- Checks if `initial_tiles` exist and are all processed
- Logs whether initial scan was completed
- Continues from `high_density_stack` if present

## Performance Considerations

### API Efficiency
- **Deduplication**: Uses `seen_place_ids` set for O(1) lookup
- **Minimal Requests**: Only subdivides when necessary
- **Pagination**: Retrieves all available results per tile

### Memory Management
- **Chunked Writes**: Prevents memory overflow on large cities
- **Set-Based Storage**: Efficient duplicate detection

### Search Coverage
- **Viewport Padding**: Ensures no edge restaurants are missed
- **Adaptive Resolution**: Balances coverage vs. API usage

## Environment Requirements

### API Key
Set `SHEGZ_MAPS_API_KEY` in `.env` file

### Required Libraries
- `requests`: HTTP client
- `pandas`: CSV handling
- `python-dotenv`: Environment variables
- Standard library: `pickle`, `os`, `logging`, `time`, `math`, `uuid`, `datetime`, `json`

### Directory Structure
```
project/
├── .env (API key)
├── logs/ (created automatically)
├── script.py
├── checkpoint_*.ckpt (created during search)
└── *_restaurants_*.csv (output)
```

## Known Limitations

1. **Latitude Dependency**: Tile size calculation uses approximate conversion (varies with latitude)
2. **API Quota**: No built-in quota management beyond error detection
3. **Checkpoint Naming**: Timestamp in checkpoint name requires exact match for resume
4. **Single City**: Processes one city per execution
5. **No Concurrency**: Sequential processing (MAX_WORKERS constant unused)
6. **Hard-Coded Type**: Only searches for "restaurant" type

## Usage Example

```python
if __name__ == "__main__":
    city = "London"
    collect_all_places_adaptive(city)
```

**Output**:
- `london_restaurants_20250419_143022.csv`
- `logs/depth_first_restaurant_search_20250419_143022.log`
- `checkpoint_london.ckpt` (removed upon completion)

## Future Improvements

- Make `TYPE` configurable (e.g., support cafes, bars)
- Implement concurrent tile processing using `MAX_WORKERS`
- Add quota monitoring and auto-throttling
- Support batch city processing
- Improve latitude-dependent calculations
- Add progress bar/UI for long-running searches