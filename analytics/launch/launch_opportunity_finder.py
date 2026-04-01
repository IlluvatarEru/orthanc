"""
Launch script for finding resale opportunities across all residential complexes.

This script:
1. Loops through all JKs in the database
2. Analyzes each JK for sales opportunities by comparing sale prices to median prices of similar flats
3. Creates a ranking of all opportunities
4. Writes the top 50 opportunities to a CSV file

Usage:
    python -m analytics.launch.launch_opportunity_finder --discount-threshold 0.15 --output opportunities.csv
"""

import argparse
import csv
from datetime import datetime
from typing import List, Dict

from analytics.src.jk_sales_analytics import analyze_jk_for_sales
from common.src.logging_config import setup_logging
from db.src.write_read_database import OrthancDB

logger = setup_logging(__name__, log_file="opportunity_finder.log")


def find_all_opportunities(
    db_path: str = "flats.db", discount_threshold: float = 0.15, city: str = "almaty"
) -> List[Dict]:
    """
    Find all resale opportunities across all JKs.

    :param db_path: str, path to database file
    :param discount_threshold: float, minimum discount percentage (e.g., 0.15 = 15%)
    :param city: str, city name to filter JKs by (default: "almaty")
    :return: List[Dict], list of all opportunities with flat details
    """
    logger.info(
        f"Starting opportunity analysis across all JKs (discount threshold: {discount_threshold * 100}%, city: {city})"
    )

    db = OrthancDB(db_path)
    jks = db.get_all_jks_excluding_blacklisted(city)
    if not jks:
        logger.warning("No JKs found in database")
        return []

    all_opportunities = []
    successful_jks = 0
    failed_jks = 0

    for i, jk_info in enumerate(jks, 1):
        jk_name = jk_info["residential_complex"]
        jk_city = jk_info.get("city")
        logger.info(f"[{i}/{len(jks)}] Analyzing opportunities for: {jk_name}")

        # Analyze JK for sales opportunities (filter by city to avoid cross-city name collisions)
        analysis = analyze_jk_for_sales(
            jk_name,
            sale_discount_percentage=discount_threshold,
            db_path=db_path,
            city=jk_city,
        )

        # Extract opportunities from all flat types
        opportunities_by_type = analysis["current_market"].opportunities

        jk_opportunity_count = 0
        for flat_type, opportunities in opportunities_by_type.items():
            for opp in opportunities:
                # Create a flat dictionary for CSV export
                opportunity_dict = {
                    "rank": None,  # Will be filled after sorting
                    "flat_id": opp.flat_info.flat_id,
                    "residential_complex": opp.flat_info.residential_complex or jk_name,
                    "price": opp.flat_info.price,
                    "area": opp.flat_info.area,
                    "flat_type": opp.flat_info.flat_type,
                    "floor": opp.flat_info.floor or "",
                    "total_floors": opp.flat_info.total_floors or "",
                    "construction_year": opp.flat_info.construction_year or "",
                    "parking": opp.flat_info.parking or "",
                    "discount_percentage_vs_median": round(
                        opp.discount_percentage_vs_median, 2
                    ),
                    "median_price": round(opp.stats_for_flat_type.median_price, 0),
                    "mean_price": round(opp.stats_for_flat_type.mean_price, 0),
                    "min_price": round(opp.stats_for_flat_type.min_price, 0),
                    "max_price": round(opp.stats_for_flat_type.max_price, 0),
                    "sample_size": opp.stats_for_flat_type.count,
                    "query_date": opp.query_date,
                    "url": f"https://krisha.kz/a/show/{opp.flat_info.flat_id}",
                    "description": (
                        opp.flat_info.description[:200]
                        if opp.flat_info.description
                        else ""
                    ),
                    "condition": opp.flat_info.condition or "unknown",
                    "first_seen_at": getattr(opp.flat_info, "first_seen_at", "") or "",
                    "relist_count": getattr(opp.flat_info, "relist_count", 0) or 0,
                }
                all_opportunities.append(opportunity_dict)
                jk_opportunity_count += 1

        if jk_opportunity_count > 0:
            successful_jks += 1
            logger.info(f"  Found {jk_opportunity_count} opportunities in {jk_name}")
        else:
            logger.info(f"  No opportunities found in {jk_name}")

    logger.info(
        f"Opportunity analysis completed: {len(all_opportunities)} total opportunities found "
        f"across {successful_jks} JKs ({failed_jks} failed)"
    )

    return all_opportunities


def filter_opportunities(
    opportunities: List[Dict],
    max_discount: float = 50.0,
    max_price: int = None,
    flat_types: List[str] = None,
) -> List[Dict]:
    """
    Filter opportunities by discount cap, max price, and flat types.

    :param opportunities: List[Dict], list of opportunity dictionaries
    :param max_discount: float, maximum discount percentage to allow (default: 50%)
    :param max_price: int, maximum price to include (optional)
    :param flat_types: List[str], allowed flat types e.g. ["Studio", "1BR", "2BR"] (optional)
    :return: List[Dict], filtered opportunities
    """
    before_count = len(opportunities)

    filtered = [
        opp
        for opp in opportunities
        if opp["discount_percentage_vs_median"] <= max_discount
        and (max_price is None or opp["price"] <= max_price)
        and (flat_types is None or opp["flat_type"] in flat_types)
    ]

    filtered_count = before_count - len(filtered)
    if filtered_count > 0:
        logger.info(
            f"Filtered out {filtered_count} opportunities "
            f"(max_discount={max_discount}%, max_price={max_price}, flat_types={flat_types})"
        )

    return filtered


def rank_opportunities(opportunities: List[Dict]) -> List[Dict]:
    """
    Rank opportunities by discount percentage (highest discount first).

    :param opportunities: List[Dict], list of opportunity dictionaries
    :return: List[Dict], ranked opportunities with rank field filled
    """
    # Sort by discount_percentage_vs_median (descending)
    ranked = sorted(
        opportunities,
        key=lambda x: x["discount_percentage_vs_median"],
        reverse=True,
    )

    # Add rank field
    for rank, opp in enumerate(ranked, 1):
        opp["rank"] = rank

    return ranked


def save_opportunities_to_db(
    opportunities: List[Dict], db_path: str = "flats.db"
) -> str:
    """
    Save opportunities to database with a run timestamp.

    :param opportunities: List[Dict], list of opportunity dictionaries
    :param db_path: str, path to database file
    :return: str, run timestamp used for this batch
    """
    if not opportunities:
        logger.warning("No opportunities to save to database")
        return None

    # Generate run timestamp with second precision
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        f"Saving {len(opportunities)} opportunities to database with run_timestamp: {run_timestamp}"
    )

    db = OrthancDB(db_path)
    try:
        inserted_count = db.insert_opportunity_analysis_batch(
            opportunities, run_timestamp
        )
        logger.info(f"Successfully saved {inserted_count} opportunities to database")
        return run_timestamp
    except Exception as e:
        logger.error(f"Error saving opportunities to database: {e}")
        raise


def write_opportunities_to_csv(
    opportunities: List[Dict], output_file: str = "opportunities.csv"
) -> None:
    """
    Write opportunities to CSV file.

    :param opportunities: List[Dict], list of opportunity dictionaries
    :param output_file: str, output CSV file path
    """
    if not opportunities:
        logger.warning("No opportunities to write to CSV")
        return

    logger.info(f"Writing {len(opportunities)} opportunities to {output_file}")

    # Define CSV columns
    fieldnames = [
        "rank",
        "flat_id",
        "residential_complex",
        "price",
        "area",
        "flat_type",
        "floor",
        "total_floors",
        "construction_year",
        "parking",
        "discount_percentage_vs_median",
        "median_price",
        "mean_price",
        "min_price",
        "max_price",
        "sample_size",
        "query_date",
        "url",
        "description",
        "condition",
        "first_seen_at",
        "relist_count",
    ]

    try:
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(opportunities)

        logger.info(
            f"Successfully wrote {len(opportunities)} opportunities to {output_file}"
        )
    except Exception as e:
        logger.error(f"Error writing CSV file: {e}")
        raise


def _find_opportunities_for_city(
    city: str,
    db_path: str,
    discount_threshold: float,
    top_n: int,
    max_discount: float,
    max_price: int = None,
    flat_types: List[str] = None,
    default_filter_min: int = 50,
) -> List[Dict]:
    """
    Find, filter, and rank top N opportunities for a single city.

    Ensures at least default_filter_min opportunities match the default
    dashboard filters (price <= 50M, seller_type = owner, S/1/2BR) so the
    dashboard isn't empty after filtering.

    :param city: str, city slug (e.g. "almaty")
    :param db_path: str, path to database file
    :param discount_threshold: float, minimum discount percentage
    :param top_n: int, number of top opportunities per city
    :param max_discount: float, maximum discount percentage to allow
    :param max_price: int, maximum price to include (optional)
    :param flat_types: List[str], allowed flat types (optional)
    :param default_filter_min: int, minimum opps matching default dashboard filters
    :return: List[Dict], ranked top N opportunities for this city
    """
    logger.info(f"--- Analyzing city: {city} ---")

    all_opportunities = find_all_opportunities(db_path, discount_threshold, city)
    if not all_opportunities:
        logger.warning(f"No opportunities found for {city}")
        return []

    filtered = filter_opportunities(
        all_opportunities, max_discount, max_price=max_price, flat_types=flat_types
    )
    if not filtered:
        logger.warning(f"No opportunities remaining after filtering for {city}")
        return []

    ranked = rank_opportunities(filtered)
    top = ranked[:top_n]

    # Ensure enough opportunities match default dashboard filters
    # (price <= 50M, seller_type = owner, flat_type in S/1/2BR)
    # We check seller_type from sales_flats since it's not in the opportunity dict
    default_flat_types = {"Studio", "1BR", "2BR"}
    default_max_price = 50_000_000

    # Load seller_type for all flat_ids in one query
    db = OrthancDB(db_path)
    db.connect()
    all_flat_ids = [opp["flat_id"] for opp in ranked]
    seller_types = {}
    if all_flat_ids:
        placeholders = ",".join(["?"] * len(all_flat_ids))
        cursor = db.conn.execute(
            f"SELECT flat_id, seller_type FROM sales_flats "
            f"WHERE flat_id IN ({placeholders}) "
            f"ORDER BY query_date DESC",
            all_flat_ids,
        )
        for row in cursor.fetchall():
            if row[0] not in seller_types:
                seller_types[row[0]] = row[1]
    db.disconnect()

    top_ids = {opp["flat_id"] for opp in top}

    def _matches_default(opp):
        return (
            opp["price"] <= default_max_price
            and opp.get("flat_type") in default_flat_types
            and seller_types.get(opp["flat_id"]) == "owner"
        )

    default_count = sum(1 for opp in top if _matches_default(opp))

    if default_count < default_filter_min:
        # Find additional opps from the full ranked list that match defaults
        extras = [
            opp
            for opp in ranked[top_n:]
            if _matches_default(opp) and opp["flat_id"] not in top_ids
        ]
        needed = default_filter_min - default_count
        top.extend(extras[:needed])
        added = min(needed, len(extras))
        if added > 0:
            logger.info(
                f"Added {added} extra default-filter opportunities "
                f"({default_count} -> {default_count + added})"
            )
        else:
            logger.info(
                f"Only {default_count} opportunities match default dashboard filters "
                f"(owner, <=50M, S/1/2BR) — no more available to add. "
                f"Consider lowering --discount-threshold to find more."
            )

    logger.info(
        f"{city}: {len(all_opportunities)} total -> {len(filtered)} filtered -> {len(top)} saved"
    )
    return top


def main(
    db_path: str = "flats.db",
    discount_threshold: float = 0.15,
    output_file: str = "opportunities.csv",
    top_n: int = 300,
    max_discount: float = 50.0,
    max_price: int = 80000000,
    flat_types: List[str] = None,
    cities: List[str] = None,
) -> None:
    """
    Main function to find and rank opportunities across all JKs.
    Runs per city, saving top N per city under a single run timestamp.

    :param db_path: str, path to database file
    :param discount_threshold: float, minimum discount percentage (e.g., 0.15 = 15%)
    :param output_file: str, output CSV file path
    :param top_n: int, number of top opportunities per city (default: 100)
    :param max_discount: float, maximum discount percentage to allow (default: 50%, filters likely scams)
    :param max_price: int, maximum flat price (default: 80M KZT)
    :param flat_types: List[str], allowed flat types (default: Studio, 1BR, 2BR)
    :param cities: List[str], city slugs to analyze (default: ["almaty"])
    """
    if flat_types is None:
        flat_types = ["Studio", "1BR", "2BR"]
    if cities is None:
        cities = ["almaty"]

    logger.info("=" * 80)
    logger.info("Starting Opportunity Finder")
    logger.info("=" * 80)
    logger.info(f"Database: {db_path}")
    logger.info(f"Cities: {cities}")
    logger.info(f"Discount threshold: {discount_threshold * 100}%")
    logger.info(f"Max discount filter: {max_discount}% (filters likely scams)")
    logger.info(f"Max price: {max_price:,} KZT")
    logger.info(f"Flat types: {flat_types}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Top N per city: {top_n}")
    logger.info("=" * 80)

    start_time = datetime.now()

    all_top_opportunities = []
    for city in cities:
        city_opps = _find_opportunities_for_city(
            city,
            db_path,
            discount_threshold,
            top_n,
            max_discount,
            max_price=max_price,
            flat_types=flat_types,
        )
        all_top_opportunities.extend(city_opps)

    if not all_top_opportunities:
        logger.warning("No opportunities found across any city. Exiting.")
        return

    # Re-rank the combined list so ranks are 1..N
    for rank, opp in enumerate(all_top_opportunities, 1):
        opp["rank"] = rank

    logger.info(
        f"Top opportunity: {all_top_opportunities[0]['residential_complex']} "
        f"({all_top_opportunities[0]['discount_percentage_vs_median']}% discount)"
    )

    # Save to database with run timestamp
    run_timestamp = save_opportunities_to_db(all_top_opportunities, db_path)
    logger.info(
        f"Saved {len(all_top_opportunities)} opportunities to database with run_timestamp: {run_timestamp}"
    )

    # Write to CSV
    write_opportunities_to_csv(all_top_opportunities, output_file)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info("=" * 80)
    logger.info("Opportunity Finder Completed")
    logger.info(f"Total opportunities saved: {len(all_top_opportunities)}")
    logger.info(f"Top {top_n} per city: {', '.join(cities)}")
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find resale opportunities across all residential complexes"
    )
    parser.add_argument(
        "--db-path",
        default="flats.db",
        help="Database file path (default: flats.db)",
    )
    parser.add_argument(
        "--discount-threshold",
        type=float,
        default=0.10,
        help="Minimum discount percentage threshold (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--output",
        default="opportunities.csv",
        help="Output CSV file path (default: opportunities.csv)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=300,
        help="Number of top opportunities per city (default: 300)",
    )
    parser.add_argument(
        "--max-discount",
        type=float,
        default=50.0,
        help="Maximum discount percentage to allow, filters likely scams (default: 50.0%%)",
    )
    parser.add_argument(
        "--max-price",
        type=int,
        default=80000000,
        help="Maximum flat price in KZT (default: 80000000)",
    )
    parser.add_argument(
        "--flat-types",
        type=str,
        nargs="+",
        default=["Studio", "1BR", "2BR"],
        help="Flat types to include (default: Studio 1BR 2BR)",
    )
    parser.add_argument(
        "--city",
        type=str,
        nargs="+",
        default=["almaty"],
        help="City name(s) to analyze (default: almaty)",
    )

    args = parser.parse_args()

    main(
        db_path=args.db_path,
        discount_threshold=args.discount_threshold,
        output_file=args.output,
        top_n=args.top_n,
        max_discount=args.max_discount,
        max_price=args.max_price,
        flat_types=args.flat_types,
        cities=args.city,
    )
