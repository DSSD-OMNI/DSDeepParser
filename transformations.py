"""Custom transformation functions available in config via operation: custom."""


def extract_domain(record: dict) -> dict:
    if "url" in record:
        import urllib.parse
        record["domain"] = urllib.parse.urlparse(record["url"]).netloc
    return record


def normalize_team_name(record: dict) -> dict:
    """Normalize team names for cross-source joins."""
    name_map = {
        "Man Utd": "Manchester United",
        "Man City": "Manchester City",
        "Spurs": "Tottenham Hotspur",
        "Nott'm Forest": "Nottingham Forest",
    }
    for field in ("team", "team_name", "team_fpl"):
        if field in record and record[field] in name_map:
            record[f"{field}_normalized"] = name_map[record[field]]
    return record
