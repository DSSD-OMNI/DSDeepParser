"""
ML Feature Engine — ETL pipeline for computing features per manager per GW.

Combines raw tables:
  - manager_history (points, rank, transfers)
  - league_standings (league position, total points)
  - raw_team_elo (team Elo ratings)
  - raw_understat_teams (xG, xGA)
  - raw_calendar_fixtures (days since last match, break flags)
  - raw_fci_stats (player-level xG/xA if available)

Output: ``features`` table and ``lri_scores`` table.
"""

import asyncio
import logging
import sqlite3
from typing import List, Dict, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class MLFeatureEngine:
    """Calculates ML features from raw parsed data."""

    # Weights for LRI (League Rating Index) calculation
    LRI_WEIGHTS = {
        "form_5gw": 0.15,
        "rank_percentile": 0.20,
        "total_points_norm": 0.15,
        "transfer_efficiency": 0.10,
        "captain_accuracy": 0.10,
        "bench_utilization": 0.05,
        "xg_team_trend": 0.10,
        "opponent_elo_norm": 0.10,
        "consistency_score": 0.05,
    }

    def __init__(self, db, config: dict = None):
        self.db = db
        self.config = config or {}

    async def recalculate_all_features(self) -> None:
        """Main ETL: compute features for all managers across all GWs."""
        logger.info("Starting ML feature recalculation…")

        try:
            # 1. Get all managers from standings
            managers = await self._get_managers()
            if not managers:
                logger.warning("No managers found for feature calculation")
                return

            # 2. Get support data
            elo_data = await self._get_elo_map()
            understat_data = await self._get_understat_map()
            fixture_data = await self._get_fixture_map()

            # 3. Calculate features per manager
            all_features = []
            for mgr in managers:
                mgr_id = mgr.get("entry") or mgr.get("manager_id")
                if not mgr_id:
                    continue
                features = await self._compute_manager_features(
                    mgr_id, mgr, elo_data, understat_data, fixture_data,
                )
                if features:
                    all_features.append(features)

            # 4. Store features
            if all_features:
                await self.db.store(all_features, {
                    "table": "features",
                    "mode": "overwrite",
                })
                logger.info("Stored %d feature records", len(all_features))

                # 5. Calculate and store LRI
                lri_records = self._calculate_lri(all_features)
                if lri_records:
                    await self.db.store(lri_records, {
                        "table": "lri_scores",
                        "mode": "overwrite",
                    })
                    logger.info("Stored %d LRI scores", len(lri_records))

        except Exception as exc:
            logger.error("Feature recalculation failed: %s", exc, exc_info=True)

        logger.info("Feature recalculation complete")

    async def periodic_recalc(self, interval_hours: int) -> None:
        """Run recalculation periodically."""
        while True:
            await self.recalculate_all_features()
            await asyncio.sleep(interval_hours * 3600)

    # ── Data loading ─────────────────────────────────────────────────

    async def _get_managers(self) -> List[Dict]:
        try:
            return await self.db.query(
                'SELECT DISTINCT * FROM league_standings ORDER BY "total_points" DESC'
            )
        except Exception:
            try:
                return await self.db.query("SELECT DISTINCT * FROM league_standings")
            except Exception as exc:
                logger.warning("Cannot read league_standings: %s", exc)
                return []

    async def _get_elo_map(self) -> Dict[str, int]:
        """team_name -> elo rating."""
        try:
            rows = await self.db.query("SELECT team_fpl, elo FROM raw_team_elo")
            return {r["team_fpl"]: int(r["elo"]) for r in rows if r.get("team_fpl")}
        except Exception:
            return {}

    async def _get_understat_map(self) -> Dict[str, Dict]:
        """team_name -> {xg_per_match, xga_per_match, ...}."""
        try:
            rows = await self.db.query(
                "SELECT * FROM raw_understat_teams WHERE record_type='team'"
            )
            return {r["team"]: r for r in rows if r.get("team")}
        except Exception:
            return {}

    async def _get_fixture_map(self) -> List[Dict]:
        try:
            return await self.db.query(
                "SELECT * FROM raw_calendar_fixtures WHERE record_type='fixture' ORDER BY kickoff_time"
            )
        except Exception:
            return []

    async def _get_manager_history(self, manager_id) -> List[Dict]:
        try:
            return await self.db.query(
                "SELECT * FROM manager_history WHERE manager_id = ? ORDER BY event",
                (str(manager_id),),
            )
        except Exception:
            return []

    # ── Feature computation ──────────────────────────────────────────

    async def _compute_manager_features(
        self,
        manager_id,
        standing: Dict,
        elo_data: Dict,
        understat_data: Dict,
        fixture_data: List[Dict],
    ) -> Optional[Dict]:
        """Compute feature vector for a single manager."""
        try:
            history = await self._get_manager_history(manager_id)
            now = datetime.now(timezone.utc).isoformat()

            # Basic standings features
            rank = _safe_int(standing.get("rank"), 0)
            total_pts = _safe_int(standing.get("total_points") or standing.get("total"), 0)
            event_pts = _safe_int(standing.get("event_total") or standing.get("event_points"), 0)

            # Form (last 5 GWs from history)
            form_5gw = 0.0
            if history:
                recent = history[-5:]
                recent_pts = [_safe_int(h.get("points") or h.get("total_points"), 0) for h in recent]
                form_5gw = sum(recent_pts) / max(len(recent_pts), 1)

            # Rank percentile (lower = better)
            total_entries = _safe_int(standing.get("total_entries"), 1) or 1
            rank_percentile = (rank - 1) / max(total_entries - 1, 1) if rank > 0 else 0.5

            # Transfer efficiency
            total_transfers = sum(_safe_int(h.get("event_transfers"), 0) for h in history) if history else 0
            transfer_cost = sum(_safe_int(h.get("event_transfers_cost"), 0) for h in history) if history else 0

            # Points per transfer (higher = better)
            transfer_efficiency = total_pts / max(total_transfers, 1) if total_transfers > 0 else total_pts

            # Captain accuracy (approximation — needs picks data for exact calc)
            captain_accuracy = 0.0

            # Bench utilization (needs picks data)
            bench_utilization = 0.0

            # Consistency (std dev of GW scores — lower is more consistent)
            consistency_score = 0.0
            if history and len(history) > 2:
                pts_list = [_safe_int(h.get("points"), 0) for h in history]
                mean_pts = sum(pts_list) / len(pts_list)
                variance = sum((p - mean_pts) ** 2 for p in pts_list) / len(pts_list)
                std_dev = variance ** 0.5
                consistency_score = max(0, 1 - (std_dev / max(mean_pts, 1)))

            # Team xG trend (average across EPL)
            avg_xg = 0.0
            if understat_data:
                xg_values = [
                    float(v.get("xg_per_match", 0))
                    for v in understat_data.values()
                    if v.get("xg_per_match")
                ]
                avg_xg = sum(xg_values) / max(len(xg_values), 1)

            # Opponent Elo (normalized)
            avg_elo = 0.0
            if elo_data:
                elo_vals = list(elo_data.values())
                avg_elo = sum(elo_vals) / max(len(elo_vals), 1)
            opponent_elo_norm = avg_elo / 2000 if avg_elo else 0.5

            # Normalize total points
            total_points_norm = min(total_pts / 2500, 1.0)  # 2500 is approx max possible

            return {
                "manager_id": str(manager_id),
                "manager_name": standing.get("player_name", standing.get("entry_name", "")),
                "rank": rank,
                "total_points": total_pts,
                "event_points": event_pts,
                "form_5gw": round(form_5gw, 2),
                "rank_percentile": round(rank_percentile, 4),
                "total_points_norm": round(total_points_norm, 4),
                "transfer_efficiency": round(transfer_efficiency, 2),
                "total_transfers": total_transfers,
                "transfer_cost": transfer_cost,
                "captain_accuracy": round(captain_accuracy, 2),
                "bench_utilization": round(bench_utilization, 2),
                "consistency_score": round(consistency_score, 4),
                "xg_team_trend": round(avg_xg, 2),
                "opponent_elo_norm": round(opponent_elo_norm, 4),
                "calculated_at": now,
            }
        except Exception as exc:
            logger.debug("Feature calc failed for manager %s: %s", manager_id, exc)
            return None

    # ── LRI calculation ──────────────────────────────────────────────

    def _calculate_lri(self, features: List[Dict]) -> List[Dict]:
        """Calculate League Rating Index for each manager."""
        lri_records = []
        for feat in features:
            score = 0.0
            for field, weight in self.LRI_WEIGHTS.items():
                val = feat.get(field, 0)
                try:
                    score += float(val) * weight
                except (TypeError, ValueError):
                    pass

            lri_records.append({
                "manager_id": feat["manager_id"],
                "manager_name": feat.get("manager_name", ""),
                "lri_score": round(score, 4),
                "rank": feat.get("rank", 0),
                "total_points": feat.get("total_points", 0),
                "form_5gw": feat.get("form_5gw", 0),
                "calculated_at": feat.get("calculated_at", ""),
            })

        # Sort by LRI descending and add LRI rank
        lri_records.sort(key=lambda r: r["lri_score"], reverse=True)
        for i, r in enumerate(lri_records, 1):
            r["lri_rank"] = i

        return lri_records


def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default
