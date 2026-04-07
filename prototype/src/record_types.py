"""Integer constants for record_type and value_type fields.

These map to the production Django Record.recordtype and Record.value_type enums.
Verify against edm_backend.edm.models.Record before use in production.
"""

# record_type values — each 15-minute slot may carry multiple record types
RECORD_TYPE_CONSUMPTION          = 1   # Energy consumed from grid
RECORD_TYPE_PRODUCTION           = 2   # Energy produced by local generation
RECORD_TYPE_SELF_CONSUMPTION     = 3   # Locally produced energy consumed locally
RECORD_TYPE_GRID_SURPLUS         = 4   # Locally produced energy fed back to grid
RECORD_TYPE_COMMUNITY_SHARE      = 5   # Community allocation share

# value_type values — distinguishes metering resolution / correction state
VALUE_TYPE_MEASURED              = 1   # Raw meter reading
VALUE_TYPE_ESTIMATED             = 2   # Interpolated / estimated value
VALUE_TYPE_CORRECTED             = 3   # Corrected after meter read revision

# SNAP billing window — 10:00–16:00 Vienna local time (Europe/Vienna)
SNAP_HOUR_START = 10   # inclusive: hour >= 10
SNAP_HOUR_END   = 16   # exclusive: hour < 16
