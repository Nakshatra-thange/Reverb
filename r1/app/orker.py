import os

SCHEDULING_MODE = os.environ.get("SCHEDULING_MODE", "strict")  # "strict" or "weighted"
TIER_WEIGHTS = {"high": 4, "default": 2, "low": 1}  # high served 4x as often as low


def build_tier_sequence() -> list[str]:
    if SCHEDULING_MODE == "strict":
        return TIERS  # existing behavior: always drain high fully before default, before low
    sequence = []
    for tier, weight in TIER_WEIGHTS.items():
        sequence.extend([tier] * weight)
    return sequence  # e.g. ['high','high','high','high','default','default','low']