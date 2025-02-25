import logging
try:
    from settings import *
    from .colors import *
except ImportError:
    from colors import *
from collections import defaultdict
import asyncio
import json, requests
from decimal import Decimal
import datetime
import asyncpg
import sys
import io, os, gc

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

LOG_DIR = 'dev/logs'
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    format=f'{cc.LIGHT_CYAN}[DexAI] %(levelname)s | %(message)s{cc.RESET}',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'dexter.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

def get_solana_price_usd():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd')
        data = response.json()
        price = data['solana']['usd']
        return str(price)
    except Exception:
        logging.info(f"{cc.RED}Failed to get Solana price from Coingecko{cc.RESET}")
        return '247.61'  # Fallback price

class Analyzer:
    def __init__(self, db_dsn):
        self.db_dsn = db_dsn
        self.total_supply = Decimal('1000000000')
        self.sol_price_usd = Decimal(get_solana_price_usd())
        self.seen_mints = set()
        self.top_creators = {}

    async def load_data(self, chunk_size=1000, offset=0):
        data = []
        
        conn = await asyncpg.connect(self.db_dsn)
        try:
            async with conn.transaction(isolation='repeatable_read'):
                rows = await conn.fetch(
                    "SELECT * FROM stagnant_mints ORDER BY mint_id LIMIT $1 OFFSET $2", chunk_size, offset
                )

            for row in rows:
                price_history = json.loads(row['price_history']) if row['price_history'] else {}
                creation_time_list = sorted(price_history.keys(), key=lambda x: float(x)) if price_history else []
                creation_time = creation_time_list[0] if creation_time_list else None

                record = {
                    "mint_id": row['mint_id'],
                    "name": row['name'],
                    "symbol": row['symbol'],
                    "owner": row['owner'],
                    "holders": json.loads(row['holders']) if row['holders'] else {},
                    "price_history": price_history,
                    "tx_counts": json.loads(row['tx_counts']) if row['tx_counts'] else {},
                    "volume": json.loads(row['volume']) if row['volume'] else {},
                    "peak_price_change": row['peak_price_change'],
                    "peak_market_cap": row['peak_market_cap'],
                    "final_market_cap": row['final_market_cap'],
                    "final_ohlc": json.loads(row['final_ohlc']) if row['final_ohlc'] else {},
                    "mint_sig": row['mint_sig'],
                    "bonding_curve": row['bonding_curve'],
                    "slot_delay": row['slot_delay'],
                    "creation_time": creation_time,
                }
                data.append(record)
        finally:
            await conn.close()
        
        return data

    def _get_peak_price(self, price_history):
        prices = [Decimal(str(v)) for v in price_history.values() if v]
        if not prices:
            return Decimal(0)
        return max(prices)

    def get_market_cap(self, current_price):
        price_in_usd = Decimal(current_price) * self.sol_price_usd
        return self.total_supply * price_in_usd

    def _calculate_median(self, values):
        values = sorted([float(v) for v in values if v])
        if not values:
            return 0
        n = len(values)
        return values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2

    def _format_timestamp(self, ts):
        dt = datetime.datetime.fromtimestamp(ts, datetime.UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    def _format_small_number(self, num):
        f_num = float(num)
        return f"{f_num:.10f}".rstrip('0').rstrip('.') if f_num > 0 else "0.0"

    def _is_successful_mint(self, token_info):
        """
        Determine if a mint is successful or a fast rug based on criteria:
        0. Sniping price = Price closest to X second after the first transaction.
        1. Highest price at least 150% of sniping price.
        2. The highest price was reached after at least 25 swaps.
        3. Fast rug: Price drops >= 40% in 3 seconds.
        """
        price_history = token_info["price_history"]
        if not price_history:
            return False, 0

        # Get sorted timestamps and corresponding prices
        timestamps = sorted(price_history.keys(), key=lambda x: float(x))
        prices = [Decimal(str(price_history[t])) for t in timestamps]

        if not timestamps or not prices:
            return False, 0

        # Check for fast rug (RUG_DROP_THRESHOLD SETS % of PRICE DROP TO CONSIDER RUG)
        RUG_DROP_THRESHOLD = Decimal("0.50") #50% PRICE DROP
        # INTERVAL TO CONSIDER PRICE DROP FOR DETECTING AS RUG
        RUG_TIME_WINDOW = 3.0

        for i in range(len(timestamps)):
            start_time = float(timestamps[i])
            start_price = prices[i]
            
            for j in range(i + 1, len(timestamps)):
                end_time = float(timestamps[j])
                end_price = prices[j]
                
                if end_time - start_time > RUG_TIME_WINDOW:
                    break

                price_drop_pct = (start_price - end_price) / start_price
                if price_drop_pct >= RUG_DROP_THRESHOLD:
                    # Flag as a rug
                    return False, 0

        # 0: Calculate the sniping price
        first_timestamp = float(timestamps[0])
        target_timestamp = first_timestamp + SNIPING_PRICE_TIME

        # Find the closest timestamp to the target
        closest_index = min(range(len(timestamps)), key=lambda i: abs(float(timestamps[i]) - target_timestamp))
        sniping_price = prices[closest_index]

        # 1: Check if highest price is at least 150% of the sniping price
        peak_price = Decimal(str(token_info["high_price"]))
        if peak_price < sniping_price * Decimal(f"{SNIPE_PRICE_TO_PEAK_PRICE_RATIO}"):
            return False, 0

        # 2: Ensure the highest price was reached after at least 100 swaps
        try:
            peak_index = next(i for i, p in enumerate(prices) if p == peak_price)
        except StopIteration:
            return False, 0

        if peak_index < HIGHEST_PRICE_MIN_SWAPS:
            return False, 0

        # Calculate the profit ratio based on sniping price
        ratio = float((peak_price - sniping_price) / sniping_price) * 100

        return True, ratio

    def process_results_sync(self, show_result=True):
        return self.process_results(show_result=show_result)

    async def analyze_market(self):
        chunk_size = 25000
        offset = 0
        logging.info("Loading data from database in chunks...")

        while True:
            data_chunk = await self.load_data(chunk_size=chunk_size, offset=offset)
            if not data_chunk:
                break

            self.analyze_top_creators_sync(data_chunk)

            del data_chunk
            gc.collect()

            offset += chunk_size

        logging.info("All data processed. Market analysis completed.")

    def process_results(self, show_result=True):
        leaderboard = {}

        logging.info(f"Creators in the database: {len(self.top_creators)}")

        for creator, data in self.top_creators.items():
            
            mint_count = data['mint_count']
            median_peak_mc = data.get('median_peak_market_cap', 0)
            median_open_prices = data['median_open_price']
            median_high_prices = data['median_peak_price']
            total_swaps = data['total_swaps']

            # Here is the main condition to set, this makes the algorithm choose best creators, tweak it to your liking
            if (
                (mint_count >= 2 and median_peak_mc >= MEDIAN_PEAK_MC_ABOVE_2_MINTS and total_swaps >= TOTAL_SWAPS_ABOVE_2_MINTS) or # Owner has more than 2 mints, median peak market cap is at least 7500$, and total swaps are at least 5
                (mint_count >= 1 and median_peak_mc >= MEDIAN_PEAK_MC_1_MINT and total_swaps >= TOTAL_SWAPS_1_MINT) # Owner has 1 mint, median peak market cap is at least 7000$, and total swaps are at least 5
            ): 
                success_count = 0
                unsuccess_count = 0
                success_ratios = []

                for token in data['tokens']:
                    is_successful, ratio = self._is_successful_mint(token) #1

                    if is_successful:
                        success_count += 1
                        success_ratios.append(ratio)
                    else:
                        unsuccess_count += 1

                total_mints = success_count + unsuccess_count
                trust_factor = success_count / total_mints

                #2 If trust_factor < X, consider creator unsuccessful => exclude
                if total_mints == 0 or trust_factor < TRUST_FACTOR_RATIO:
                    continue

                avg_success_ratio = (sum(success_ratios) / success_count) if success_count > 0 else 0.0
                median_success_ratio = self._calculate_median(success_ratios)

                performance_score = (
                    mint_count 
                    * data['median_peak_market_cap'] 
                    * median_success_ratio
                    / (median_open_prices if median_open_prices > 0 else 1)
                )

                if (unsuccess_count > 0):
                    too_close = any(delay < 900 for delay in data['creation_delays'])
                    if too_close:
                        logging.info(f"{cc.YELLOW}Creator {creator} should not be trusted. Excluding from leaderboard.{cc.RESET}")
                        continue

                leaderboard[creator] = {
                    "mint_count": mint_count,
                    "median_peak_market_cap": data['median_peak_market_cap'],
                    "median_market_cap": data["median_market_cap"],
                    "median_open_price": median_open_prices,
                    "median_high_price": median_high_prices,
                    "performance_score": performance_score,
                    "trust_factor": trust_factor,
                    "avg_success_ratio": avg_success_ratio,
                    "median_success_ratio": median_success_ratio,
                    "success_count": success_count,
                    "unsuccess_count": unsuccess_count,
                    "total_swaps": total_swaps
                }

            if show_result:
                logging.info(f"\n{cc.MAGENTA}Leaderboard (Top Performers):{cc.RESET}")
                for rank, entry in enumerate(leaderboard, start=1):
                    logging.info(f"Rank {rank}:")
                    logging.info(f"  Creator: {entry['creator']}")
                    logging.info(f"  Mint Count: {entry['mint_count']}")
                    logging.info(f"  Successful Mints: {entry['success_count']}")
                    logging.info(f"  Unsuccessful Mints: {entry['unsuccess_count']}")
                    logging.info(f"  Total Swaps: {entry['total_swaps']}")
                    logging.info(f"  Median Peak Market Cap: {entry['median_peak_market_cap']:.2f}$")
                    logging.info(f"  Median Market Cap: {entry['median_market_cap']:.2f}$")
                    logging.info(f"  Median Open Price: {entry['median_open_price']:.10f} SOL")
                    logging.info(f"  Median High Price: {entry['median_high_price']:.10f} SOL")
                    logging.info(f"  Median Success Ratio (Peak/Open): {entry['median_success_ratio']:.2f}")
                    logging.info(f"  Average Success Ratio (Peak/Open): {entry['avg_success_ratio']:.2f}")
                    logging.info(f"  Trust Factor: {entry['trust_factor']:.2f}")
                    logging.info(f"  Performance Score: {entry['performance_score']:.2f}\n")

        return leaderboard  # Return the leaderboard list

if __name__ == "__main__":
    analyzer = Analyzer("postgres://dexter_user:admin123@127.0.0.1/dexter_db")
    asyncio.run(analyzer.analyze_market())
    analyzer.process_results()
