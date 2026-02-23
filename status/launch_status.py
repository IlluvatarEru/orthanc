#!/usr/bin/env python3
"""Simple status service that returns JSON with system health info."""

import argparse
import json
import os
import re
import sqlite3
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler

# Persistent processes to monitor: display name -> command substring to match
MONITORED_PROCESSES = {
    "market-data": "price.launch.launch_market_data",
}

PIPELINE_LOG = "/root/orthanc/daily_pipeline.out"
PIPELINE_CMD_PATTERN = "daily_sales_pipeline"


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
                    since = line[:24].strip()
                    result[name] = {"status": "running", "since": since}
                    break
            else:
                result[name] = {"status": "stopped", "since": None}
    except Exception:
        pass
    return result


def get_pipeline_status() -> dict:
    """Get daily sales pipeline status from cron job."""
    status = {"running": False, "last_start": None, "last_complete": None}

    # Check if pipeline is currently running
    try:
        ps_output = subprocess.run(
            ["/usr/bin/ps", "ax", "-o", "args="],
            capture_output=True,
            text=True,
        )
        status["running"] = PIPELINE_CMD_PATTERN in ps_output.stdout
    except Exception:
        pass

    # Parse log file for last start/complete timestamps
    if os.path.exists(PIPELINE_LOG):
        try:
            result = subprocess.run(
                ["/usr/bin/tail", "-50", PIPELINE_LOG],
                capture_output=True,
                text=True,
            )
            lines = result.stdout.strip().splitlines()
            for line in reversed(lines):
                if "Starting daily sales pipeline" in line and not status["last_start"]:
                    match = re.search(r"=== (.+?) ===", line)
                    if match:
                        status["last_start"] = match.group(1)
                if (
                    "Daily sales pipeline complete" in line
                    and not status["last_complete"]
                ):
                    match = re.search(r"=== (.+?) ===", line)
                    if match:
                        status["last_complete"] = match.group(1)
                if status["last_start"] and status["last_complete"]:
                    break
        except Exception:
            pass

    return status


def get_latest_pipeline_run(db_path: str) -> dict:
    """Get the most recent pipeline run stats from database."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT started_at, finished_at, duration_seconds, city,
                   jks_total, jks_successful, jks_failed, flats_scraped,
                   rate_limited, http_errors, request_errors, error_breakdown
            FROM pipeline_runs
            ORDER BY finished_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


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
            "pipeline": get_pipeline_status(),
            "last_run": get_latest_pipeline_run(self.db_path),
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
