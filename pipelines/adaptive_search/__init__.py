
"""
adaptive_search
~~~~~~~~~~~~~~~

Adaptive, density driven Places API extraction for Gontrel.

This package provides tools for adaptive search and data extraction using tiling and density-based methods.
"""

__version__ = "0.1.0"

# -------------------------------------------------------------------
# Package-level logger (this lives in utils/logging.py, not to be
# confused with the stdlib `logging` package)
# -------------------------------------------------------------------
from .utils.logger     import logger  

# -------------------------------------------------------------------
# Core search & geocoding
# -------------------------------------------------------------------
from .core.geocode     import get_city_center
from .core.places      import get_nearby_places
from .core.search      import collect_all_places_adaptive

# -------------------------------------------------------------------
# Spatial tiling
# -------------------------------------------------------------------
from .spatial.tiles    import generate_initial_tiles, subdivide_tile, create_tile

# -------------------------------------------------------------------
# Checkpointing & metrics
# -------------------------------------------------------------------
from .utils.checkpoint import save_checkpoint, load_checkpoint
from .utils.metrics    import Metrics

# -------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------
__all__ = [
    # logging
    "logger",
    # core
    "get_city_center",
    "get_nearby_places",
    "collect_all_places_adaptive",
    # spatial
    "generate_initial_tiles",
    "subdivide_tile",
    "create_tile",
    # utils
    "save_checkpoint",
    "load_checkpoint",
    "Metrics",
]