#!/usr/bin/env python3
"""Simple status service that returns JSON with system health info."""

import argparse
import json
import sqlite3
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler

# Processes to monitor: display name -> command substring to match
MONITORED_PROCESSES = {
    "daily-sales": "launch_scraping_all_jks --mode daily-sales",
    "daily-rentals": "launch_scraping_all_jks --mode daily-rentals",
    "market-data": "price.launch.launch_market_data",
}


def get_service_status(service_name: str) -> bool:
    """Check if a systemd service is active."""
    try:
        result = subprocess.run(
            ["/usr/bin/systemctl", "is-active", service_name],
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() == "active"
    except Exception:
        return False


def get_process_uptimes() -> dict:
    """Get uptime info for monitored processes using ps."""
    result = {}
    try:
        ps_output = subprocess.run(
            ["/usr/bin/ps", "ax", "-o", "lstart=,args="],
            capture_output=True,
            text=True,
        )
        lines = ps_output.stdout.strip().splitlines()
        for name, cmd_pattern in MONITORED_PROCESSES.items():
            for line in lines:
                if cmd_pattern in line:
                    # lstart format: "Tue Feb 18 02:15:03 2026"
                    # First 24 chars are the lstart, rest is the command
                    since = line[:24].strip()
                    result[name] = {"status": "running", "since": since}
                    break
            else:
                result[name] = {"status": "stopped", "since": None}
    except Exception:
        pass
    return result


def get_latest_timestamps(db_path: str) -> dict:
    """Get latest timestamps from database."""
    timestamps = {"price": None, "rental": None, "sale": None}
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("SELECT MAX(fetched_at) FROM mid_prices")
        timestamps["price"] = cur.fetchone()[0]

        cur.execute("SELECT MAX(scraped_at) FROM rental_flats")
        timestamps["rental"] = cur.fetchone()[0]

        cur.execute("SELECT MAX(scraped_at) FROM sales_flats")
        timestamps["sale"] = cur.fetchone()[0]

        conn.close()
    except Exception as e:
        timestamps["error"] = str(e)
    return timestamps


class StatusHandler(BaseHTTPRequestHandler):
    db_path = "flats.db"

    def do_GET(self):
        status = {
            "services": {
                "api": "up" if get_service_status("orthanc-api") else "down",
                "web": "up" if get_service_status("orthanc-web") else "down",
            },
            "processes": get_process_uptimes(),
            "latest_timestamps": get_latest_timestamps(self.db_path),
        }

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(status, indent=2).encode())

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    parser = argparse.ArgumentParser(description="Orthanc Status Service")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8002, help="Port to bind to")
    parser.add_argument("--db", default="flats.db", help="Path to database")
    args = parser.parse_args()

    StatusHandler.db_path = args.db

    server = HTTPServer((args.host, args.port), StatusHandler)
    print(f"Status service running on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
