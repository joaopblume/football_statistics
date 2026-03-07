"""League configuration and mapping helpers.

Central registry for all supported leagues.  Only leagues that have been
tested and confirmed to work via soccerdata ESPN are listed here.

Adding a new league:
1. Verify the league works end-to-end in soccerdata manually.
2. Add an entry to LEAGUE_CONFIGS.
3. If soccerdata does not ship a native mapping, set ``custom_espn_mapping``
   with the ESPN key and season boundaries.
4. Add seeds to infra/postgres/migrations/ and re-run them.
"""

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Only leagues that have been validated against soccerdata ESPN.
# custom_espn_mapping: set for leagues NOT natively supported by soccerdata.
#                      None means soccerdata already knows about this league.
LEAGUE_CONFIGS: dict[str, dict[str, Any]] = {
    "BRA-Brasileirao": {
        "slug": "brasileirao",
        "custom_espn_mapping": {
            "ESPN": "bra.1",
            "season_start": "Apr",
            "season_end": "Dec",
        },
    },
    "ITA-Serie A": {
        "slug": "serie_a",
        "custom_espn_mapping": None,  # natively registered in soccerdata
    },
    "ENG-Premier League": {
        "slug": "premier_league",
        "custom_espn_mapping": None,
    },
    "FRA-Ligue 1": {
        "slug": "ligue_1",
        "custom_espn_mapping": None,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_league_mapping(league_key: str) -> None:
    """Register *league_key* in soccerdata's runtime config if needed.

    Only injects a mapping when the league has a ``custom_espn_mapping``
    defined (i.e. it is not natively registered in soccerdata) AND is not
    already present in the config dict.
    """
    config = LEAGUE_CONFIGS.get(league_key)
    if config is None:
        LOGGER.warning(
            "ensure_league_mapping: league=%r is not in LEAGUE_CONFIGS — "
            "soccerdata may not support it.",
            league_key,
        )
        return

    mapping = config.get("custom_espn_mapping")
    if mapping is None:
        # Natively registered — nothing to do
        return

    from soccerdata import _config  # late import to avoid module-level side-effects

    if league_key not in _config.LEAGUE_DICT:
        _config.LEAGUE_DICT[league_key] = mapping
        LOGGER.debug("ensure_league_mapping: registered %r in soccerdata", league_key)


def get_league_slug(league_key: str) -> str:
    """Return the filesystem-safe slug for *league_key*.

    Used to build MinIO paths like ``espn/brasileirao/2024/``.
    Falls back to a generic slugification if the league is unknown.
    """
    config = LEAGUE_CONFIGS.get(league_key)
    if config and config.get("slug"):
        return config["slug"]
    # Generic fallback: lowercase, replace non-alphanumeric with underscore
    import re
    return re.sub(r"[^a-z0-9]+", "_", league_key.lower()).strip("_")
