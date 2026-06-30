# TODO: Conduct final code review and verification on the 3-section rendering and clock calibration logic
import time
import sys
from datetime import datetime
import json
import urllib.request
import redis

# Redis configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
# Flink SQL now aggregates with symbol-specific sharded keys (e.g. BTCUSDT_volume_agg)
REDIS_KEY = "BTCUSDT_volume_agg"


def get_terminal_size():
    """Get the current terminal size."""
    try:
        import os

        size = os.get_terminal_size()
        return size.columns, size.lines
    except OSError:
        return 80, 24


def get_binance_time_offset():
    """Calculate the clock offset between local machine and Binance server (in ms) to calibrate latency."""
    try:
        # Measure RTT start
        start_ms = int(time.time() * 1000)

        # Call Binance Public API to get server time (Using native urllib)
        url = "https://api.binance.com/api/v3/time"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=3) as response:
            res_data = json.loads(response.read().decode())
            server_time_ms = res_data["serverTime"]

        # Measure RTT end
        end_ms = int(time.time() * 1000)

        # Calculate estimated local time matching when the API returned
        local_time_ms = (start_ms + end_ms) // 2

        # Offset = Local time - Binance server time
        offset_ms = local_time_ms - server_time_ms
        return offset_ms
    except Exception as e:
        # Fallback to 0 if there are networking issues
        return 0


class CryptoDashboard:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

        # Perform time calibration with Binance server
        sys.stdout.write("\033[H\033[JCalibrating clock offset with Binance server...\n")
        sys.stdout.flush()
        self.time_offset_ms = get_binance_time_offset()

        # State tracking variables
        self.last_event_time = 0
        self.last_latency_ms = 0
        self.last_update_sys_time = 0.0

    def connect(self):
        try:
            self.r.ping()
        except redis.ConnectionError as e:
            print(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
            sys.exit(1)

    def render(self, data):
        cols, _ = get_terminal_size()

        # Safely parse values with try-except to handle startup lag when some fields aren't populated yet
        try:
            # 1. Parsing Global Cumulative Metrics
            volume = float(data.get(b"volume", b"0.0").decode("utf-8"))
            trade_count = int(data.get(b"trade_count", b"0").decode("utf-8"))
            global_vwap = float(data.get(b"global_vwap", b"0.0").decode("utf-8"))
            latest_event_ms = int(data.get(b"event_time", b"0").decode("utf-8"))

            # 2. Parsing Sliding Window Metrics (Hop)
            vwap_1m = float(data.get(b"vwap_1m", b"0.0").decode("utf-8"))
            volume_1m = float(data.get(b"volume_1m", b"0.0").decode("utf-8"))
            volatility_1m = float(data.get(b"volatility_1m", b"0.0").decode("utf-8"))
            spread_1m = float(data.get(b"spread_1m", b"0.0").decode("utf-8"))

            # 3. Parsing Candlestick Metrics (Tumble)
            open_1m = float(data.get(b"open_1m", b"0.0").decode("utf-8"))
            high_1m = float(data.get(b"high_1m", b"0.0").decode("utf-8"))
            low_1m = float(data.get(b"low_1m", b"0.0").decode("utf-8"))
            close_1m = float(data.get(b"close_1m", b"0.0").decode("utf-8"))
            volume_1m_tumble = float(data.get(b"volume_1m_tumble", b"0.0").decode("utf-8"))
            candle_end_time = data.get(b"candle_end_time", b"N/A").decode("utf-8")
        except (ValueError, TypeError, KeyError) as e:
            # Fallback to defaults if Redis contains incomplete data during job bootstrap
            return

        current_time_ms = int(time.time() * 1000)
        current_time_sec = time.time()

        # Determine Latency and State
        state_str = "Active"
        latency_ms = 0
        event_time_str = "N/A"

        if latest_event_ms > 0:
            dt = datetime.fromtimestamp(latest_event_ms / 1000.0)
            event_time_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # If event_time has updated (New trade received)
            if latest_event_ms != self.last_event_time:
                # Calibrate current time by subtracting the clock drift offset
                calibrated_now_ms = current_time_ms - self.time_offset_ms
                latency_ms = calibrated_now_ms - latest_event_ms

                # Protect against negative latency values in case of tiny network latency & clock mismatch
                if latency_ms < 0:
                    latency_ms = 0

                self.last_latency_ms = latency_ms
                self.last_event_time = latest_event_ms
                self.last_update_sys_time = current_time_sec
                state_str = "Active"
            else:
                # If event_time is the same (No new trades, freeze latency and show idle duration)
                latency_ms = self.last_latency_ms
                idle_sec = current_time_sec - self.last_update_sys_time if self.last_update_sys_time > 0 else 0.0
                state_str = f"Idle: {idle_sec:.1f}s"
                if self.last_update_sys_time == 0.0:
                    state_str = "Pending"

        # Format window completion timestamp nicely if it's formatted as SQL timestamp
        if candle_end_time != "N/A" and "." in candle_end_time:
            # e.g., "2026-06-30 00:08:00.000" -> "00:08:00"
            try:
                candle_end_time = candle_end_time.split(" ")[1].split(".")[0]
            except IndexError:
                pass

        # ANSI Colors
        CYAN = "\033[96m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        RED = "\033[91m"
        WHITE = "\033[97m"
        RESET = "\033[0m"
        BOLD = "\033[1m"

        # Define Latency Color
        if latency_ms < 300:
            latency_color = GREEN
        elif latency_ms < 1000:
            latency_color = YELLOW
        else:
            latency_color = RED

        # Header
        sys.stdout.write("\033[H\033[J")  # Clear screen and move cursor to top-left
        sys.stdout.write(f"{BOLD}{CYAN}" + "=" * cols + f"{RESET}\n")
        sys.stdout.write(f"{BOLD}{CYAN}  Crypto Real-Time Dashboard (Binance stream via Kafka & Flink){RESET}\n")
        sys.stdout.write(f"{BOLD}{CYAN}" + "=" * cols + f"{RESET}\n\n")

        # Layout Section 1: Global Cumulative Metrics
        sys.stdout.write(f"{BOLD}{CYAN}[1. Global Cumulative Metrics]{RESET}\n")
        sys.stdout.write(f"  Target Asset:          {BOLD}{WHITE}BTCUSDT{RESET}\n")
        sys.stdout.write(f"  Cumulative Volume:     {BOLD}{YELLOW}{volume:,.4f} BTC{RESET}\n")
        sys.stdout.write(f"  Total Trades:          {BOLD}{CYAN}{trade_count:,}{RESET} txs\n")
        sys.stdout.write(f"  Global VWAP:           {BOLD}{GREEN}${global_vwap:,.2f}{RESET} (Volume-Weighted)\n\n")

        # Layout Section 2: Recent 1-Min Sliding Metrics (Hop Window)
        sys.stdout.write(f"{BOLD}{CYAN}[2. Recent 1-Min Sliding Window (5s Update)]{RESET}\n")
        sys.stdout.write(f"  Sliding 1m VWAP:       {BOLD}{GREEN}${vwap_1m:,.2f}{RESET}\n")
        sys.stdout.write(f"  Sliding 1m Volume:     {BOLD}{YELLOW}{volume_1m:,.4f} BTC{RESET}\n")
        sys.stdout.write(f"  Sliding 1m Volatility: {BOLD}{WHITE}{volatility_1m:,.4f}{RESET} (StdDev)\n")
        sys.stdout.write(f"  Sliding 1m High-Low:   {BOLD}{YELLOW}${spread_1m:,.2f}{RESET} spread\n\n")

        # Layout Section 3: Latest 1-Min Candlestick (Tumble Window)
        # Determine Candlestick Trend Color
        if open_1m == 0.0:
            candle_color = WHITE
            trend_str = "Pending Window..."
        elif close_1m >= open_1m:
            candle_color = GREEN
            trend_str = "▲ Bullish"
        else:
            candle_color = RED
            trend_str = "▼ Bearish"

        sys.stdout.write(f"{BOLD}{CYAN}[3. Latest 1-Min Completed Candlestick (Tumble)]{RESET}\n")
        sys.stdout.write(
            f"  Candle Status:         {BOLD}{candle_color}{trend_str}{RESET}  |  Completed At: {BOLD}{WHITE}{candle_end_time}{RESET}\n"
        )
        sys.stdout.write(
            f"  Open:  {BOLD}{WHITE}${open_1m:,.2f}{RESET}  |  High:  {BOLD}{GREEN}${high_1m:,.2f}{RESET}\n"
        )
        sys.stdout.write(
            f"  Low:   {BOLD}{RED}${low_1m:,.2f}{RESET}  |  Close: {BOLD}{candle_color}${close_1m:,.2f}{RESET}\n"
        )
        sys.stdout.write(f"  Candle Volume:         {BOLD}{YELLOW}{volume_1m_tumble:,.4f} BTC{RESET}\n\n")

        # System health & Metadata
        sys.stdout.write(f"{BOLD}{CYAN}" + "-" * cols + f"{RESET}\n")
        sys.stdout.write(f"  Latest Event Time (Flink): {event_time_str}\n")
        sys.stdout.write(
            f"  End-to-End Latency:        {BOLD}{latency_color}{latency_ms:,} ms{RESET} ({BOLD}{candle_color}{state_str}{RESET})\n"
        )
        sys.stdout.write(f"  Clock Drift Offset:        {self.time_offset_ms:+,} ms (Calibrated)\n")
        sys.stdout.write(f"  Last Polled (Local):   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sys.stdout.write(f"{BOLD}{CYAN}" + "=" * cols + f"{RESET}\n")
        sys.stdout.write("\n  Press Ctrl+C to exit...\n")
        sys.stdout.flush()

    def run(self):
        self.connect()
        try:
            # Hide cursor
            sys.stdout.write("\033[?25l")
            sys.stdout.flush()

            while True:
                data = self.r.hgetall(REDIS_KEY)
                if data:
                    self.render(data)
                else:
                    # Clear and show waiting message if no data exists
                    sys.stdout.write("\033[H\033[J")
                    print("Waiting for aggregation data from Flink... (Ensure producer is running)")
                    sys.stdout.flush()

                time.sleep(0.1)

        except KeyboardInterrupt:
            # Show cursor before exit
            sys.stdout.write("\033[?25h\n")
            sys.stdout.write("Dashboard terminated. Exiting...\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stdout.write("\033[?25h\n")
            print(f"Error in dashboard runtime: {e}")


def main():
    dashboard = CryptoDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()
