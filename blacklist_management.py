#!/usr/bin/env python3
"""
JK Blacklist Management Tool

A command-line tool for managing blacklisted residential complexes (JKs).
This tool allows you to add, remove, and list blacklisted JKs that will be
excluded from scraping operations.

Usage:
    python blacklist_management.py --action list
    python blacklist_management.py --action add --name "Meridian Apartments"  --notes "Test
    python blacklist_management.py --action remove --name "Meridian Apartments"  --notes "Test
"""

import argparse
import logging
import sys
from typing import Optional

from db.src.write_read_database import OrthancDB

# Add the project root to the Python path

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def list_blacklisted_jks(db_path: str = "flats.db") -> None:
    """
    List all blacklisted JKs.

    :param db_path: str, path to database file
    """
    db = OrthancDB(db_path)
    blacklisted = db.get_blacklisted_jks()

    if not blacklisted:
        logging.info("✅ No blacklisted JKs found.")
        return

    logging.info(f"📋 Blacklisted JKs ({len(blacklisted)}):")
    logging.info("-" * 80)
    for jk in blacklisted:
        logging.info(f"ID: {jk['id']}")
        logging.info(f"Name: {jk['name']}")
        logging.info(f"Krisha ID: {jk['krisha_id']}")
        logging.info(f"Blacklisted: {jk['blacklisted_at']}")
        logging.info(f"Notes: {jk['notes'] or 'No notes'}")
        logging.info("-" * 80)


def add_jk_to_blacklist(
    name: str, krisha_id: str, notes: Optional[str] = None, db_path: str = "flats.db"
) -> bool:
    """
    Add a JK to the blacklist.

    :param name: str, name of the JK
    :param krisha_id: str, Krisha ID of the JK
    :param notes: Optional[str], notes about why it's blacklisted
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)

    # Check if JK is already blacklisted
    if db.is_jk_blacklisted(krisha_id=krisha_id) or db.is_jk_blacklisted(name=name):
        logging.info(f"⚠️  JK '{name}' is already blacklisted.")
        return False

    success = db.blacklist_jk(krisha_id, name, notes)

    if success:
        logging.info(f"✅ Successfully blacklisted JK: {name}")
        if notes:
            logging.info(f"   Notes: {notes}")
    else:
        logging.info(f"❌ Failed to blacklist JK: {name}")

    return success


def add_jk_to_blacklist_by_name(
    name: str, notes: str = "", db_path: str = "flats.db"
) -> bool:
    """
    Add a JK to the blacklist by name, automatically finding the Krisha ID.

    :param name: str, name of the JK
    :param notes: str, notes about why it's blacklisted
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)

    # Check if JK is already blacklisted
    if db.is_jk_blacklisted(name=name):
        logging.info(f"⚠️  JK '{name}' is already blacklisted.")
        return False

    success = db.blacklist_jk_by_name(name, notes)

    if success:
        logging.info(f"✅ Successfully blacklisted JK by name: {name}")
        if notes:
            logging.info(f"   Notes: {notes}")
    else:
        logging.info(f"❌ Failed to blacklist JK by name: {name}")

    return success


def add_jk_to_blacklist_by_id(
    jk_id: str, notes: str = "", db_path: str = "flats.db"
) -> bool:
    """
    Add a JK to the blacklist by ID, automatically finding the name.

    :param jk_id: str, ID of the JK
    :param notes: str, notes about why it's blacklisted
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)

    # Check if JK is already blacklisted
    if db.is_jk_blacklisted(krisha_id=jk_id):
        logging.info(f"⚠️  JK with ID '{jk_id}' is already blacklisted.")
        return False

    success = db.blacklist_jk_by_id(jk_id, notes)

    if success:
        logging.info(f"✅ Successfully blacklisted JK by ID: {jk_id}")
        if notes:
            logging.info(f"   Notes: {notes}")
    else:
        logging.info(f"❌ Failed to blacklist JK by ID: {jk_id}")

    return success


def remove_jk_from_blacklist(
    name: Optional[str] = None,
    krisha_id: Optional[str] = None,
    db_path: str = "flats.db",
) -> bool:
    """
    Remove a JK from the blacklist.

    :param name: Optional[str], name of the JK to remove
    :param krisha_id: Optional[str], Krisha ID of the JK to remove
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    if not name and not krisha_id:
        logging.info("❌ Either --name or --krisha-id must be provided.")
        return False

    db = OrthancDB(db_path)

    # Check if JK is blacklisted
    if not db.is_jk_blacklisted(krisha_id=krisha_id, name=name):
        logging.info(f"⚠️  JK '{name or krisha_id}' is not in the blacklist.")
        return False

    success = db.whitelist_jk(krisha_id=krisha_id, name=name)

    if success:
        logging.info(f"✅ Successfully removed JK from blacklist: {name or krisha_id}")
    else:
        logging.info(f"❌ Failed to remove JK from blacklist: {name or krisha_id}")

    return success


def remove_jk_from_blacklist_by_name(name: str, db_path: str = "flats.db") -> bool:
    """
    Remove a JK from the blacklist by name, automatically finding the Krisha ID.

    :param name: str, name of the JK to remove
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)

    # Check if JK is blacklisted
    if not db.is_jk_blacklisted(name=name):
        logging.info(f"⚠️  JK '{name}' is not in the blacklist.")
        return False

    try:
        success = db.whitelist_jk_by_name(name)
        logging.info(f"✅ Successfully removed JK from blacklist by name: {name}")
        return success
    except Exception as e:
        logging.info(f"❌ Failed to remove JK from blacklist by name: {name}")
        logging.info(f"   Error: {e}")
        return False


def remove_jk_from_blacklist_by_id(jk_id: str, db_path: str = "flats.db") -> bool:
    """
    Remove a JK from the blacklist by ID, automatically finding the name.

    :param jk_id: str, ID of the JK to remove
    :param db_path: str, path to database file
    :return: bool, True if successful
    """
    db = OrthancDB(db_path)

    # Check if JK is blacklisted
    if not db.is_jk_blacklisted(krisha_id=jk_id):
        logging.info(f"⚠️  JK with ID '{jk_id}' is not in the blacklist.")
        return False

    try:
        success = db.whitelist_jk_by_id(jk_id)
        logging.info(f"✅ Successfully removed JK from blacklist by ID: {jk_id}")
        return success
    except Exception as e:
        logging.info(f"❌ Failed to remove JK from blacklist by ID: {jk_id}")
        logging.info(f"   Error: {e}")
        return False


def main():
    """
    Main function for the blacklist management tool.
    """
    parser = argparse.ArgumentParser(
        description="Manage blacklisted residential complexes (JKs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all blacklisted JKs
  python blacklist_management.py --action list
  
  # Add a JK to blacklist (with both name and Krisha ID)
  python blacklist_management.py --action add --name "Problematic JK" --krisha-id "jk_123" --notes "Causes errors"
  
  # Add a JK to blacklist by name only (auto-finds Krisha ID)
  python blacklist_management.py --action add --name "Problematic JK" --notes "Causes errors"
  
  # Add a JK to blacklist by ID only (auto-finds name)
  python blacklist_management.py --action add --jk-id "jk_123" --notes "Causes errors"
  
  # Remove JK by name (auto-finds Krisha ID)
  python blacklist_management.py --action remove --name "Problematic JK"
  
  # Remove JK by Krisha ID (traditional method)
  python blacklist_management.py --action remove --krisha-id "jk_123"
  
  # Remove JK by ID only (auto-finds name)
  python blacklist_management.py --action remove --jk-id "jk_123"
        """,
    )

    parser.add_argument(
        "--action",
        choices=["list", "add", "remove"],
        required=True,
        help="Action to perform: list, add, or remove",
    )

    parser.add_argument(
        "--name", help="JK name (use with --action add or --action remove)"
    )

    parser.add_argument(
        "--krisha-id", help="Krisha ID (use with --action add or --action remove)"
    )

    parser.add_argument(
        "--jk-id", help="JK ID (use with --action add or --action remove)"
    )

    parser.add_argument(
        "--notes", help="Notes about why the JK is blacklisted (only for add actions)"
    )

    parser.add_argument(
        "--db-path",
        default="flats.db",
        help="Path to database file (default: flats.db)",
    )

    args = parser.parse_args()

    # Handle list action
    if args.action == "list":
        list_blacklisted_jks(db_path=args.db_path)
        return

    # Handle add action - determine which function to use based on provided parameters
    if args.action == "add":
        if args.name and args.krisha_id:
            # Both name and krisha_id provided - use traditional method
            add_jk_to_blacklist(
                name=args.name,
                krisha_id=args.krisha_id,
                notes=args.notes,
                db_path=args.db_path,
            )
        elif args.name and not args.krisha_id and not args.jk_id:
            # Only name provided - use by_name method
            add_jk_to_blacklist_by_name(
                name=args.name, notes=args.notes or "", db_path=args.db_path
            )
        elif args.jk_id and not args.name and not args.krisha_id:
            # Only jk_id provided - use by_id method
            add_jk_to_blacklist_by_id(
                jk_id=args.jk_id, notes=args.notes or "", db_path=args.db_path
            )
        else:
            logging.info("❌ For add action, provide either:")
            logging.info("   - Both --name and --krisha-id (traditional method)")
            logging.info("   - Only --name (auto-finds Krisha ID)")
            logging.info("   - Only --jk-id (auto-finds name)")
            sys.exit(1)

    # Handle remove action - determine which function to use based on provided parameters
    elif args.action == "remove":
        if args.name and args.krisha_id:
            # Both name and krisha_id provided - use traditional method
            remove_jk_from_blacklist(
                name=args.name, krisha_id=args.krisha_id, db_path=args.db_path
            )
        elif args.name and not args.krisha_id and not args.jk_id:
            # Only name provided - use by_name method
            remove_jk_from_blacklist_by_name(name=args.name, db_path=args.db_path)
        elif args.krisha_id and not args.name and not args.jk_id:
            # Only krisha_id provided - use traditional method
            remove_jk_from_blacklist(krisha_id=args.krisha_id, db_path=args.db_path)
        elif args.jk_id and not args.name and not args.krisha_id:
            # Only jk_id provided - use by_id method
            remove_jk_from_blacklist_by_id(jk_id=args.jk_id, db_path=args.db_path)
        else:
            logging.info("❌ For remove action, provide one of:")
            logging.info("   - --name (auto-finds Krisha ID)")
            logging.info("   - --krisha-id (traditional method)")
            logging.info("   - --jk-id (auto-finds name)")
            sys.exit(1)


if __name__ == "__main__":
    main()
