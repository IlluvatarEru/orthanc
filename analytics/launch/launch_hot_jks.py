"""
Launch script for computing hot JK rankings and price trends.

Usage:
    python -m analytics.launch.launch_hot_jks
    python -m analytics.launch.launch_hot_jks --db-path flats.db
"""

import argparse

from analytics.src.hot_jks_analytics import compute_heat_scores, compute_price_trends
from common.src.logging_config import setup_logging
from db.src.write_read_database import OrthancDB

logger = setup_logging(__name__, log_file="hot_jks.log")


def main(db_path: str = "flats.db"):
    logger.info("Starting hot JKs computation")

    with OrthancDB(db_path) as db:
        week, scores = compute_heat_scores(db)
        logger.info(f"Heat scores: {len(scores)} JKs for week {week}")
        if scores:
            db.save_heat_scores(scores, week)
            logger.info(f"Saved {len(scores)} heat scores for week {week}")
            top3 = scores[:3]
            for s in top3:
                logger.info(
                    f"  {s['jk_name']}: {s['heat_score']:.4f} "
                    f"(listings={s['active_listings']})"
                )

        current_week, ref_week, trends = compute_price_trends(db)
        logger.info(f"Price trends: {len(trends)} JKs ({ref_week} -> {current_week})")
        if trends:
            db.save_price_trends(trends, current_week)
            logger.info(f"Saved {len(trends)} price trends for week {current_week}")

    logger.info("Hot JKs computation complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute hot JK rankings and price trends"
    )
    parser.add_argument(
        "--db-path",
        default="flats.db",
        help="Database file path (default: flats.db)",
    )
    args = parser.parse_args()
    main(db_path=args.db_path)
