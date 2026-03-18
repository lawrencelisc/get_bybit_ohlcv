import os
import gc
import ccxt
import pandas as pd
import warnings

from loguru import logger
from pathlib import Path
from datetime import datetime, timezone

from core.orchestrator import DataSourceConfig


class DataCenterSrv:
    warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
    data_folder_bybit = Path(__file__).parent.parent.parent / 'share_algoB' / 'bybit_data'

    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df
        self.data_folder_bybit.mkdir(parents=True, exist_ok=True)


    def get_exchange_trade(self, symbol: str):
        market_symbol = f'{symbol}/USDT:USDT'
        try:
            self.bybit = ccxt.bybit()
            self.markets = self.bybit.load_markets()
            return self.markets.get(market_symbol)
        except Exception as e:
            logger.exception(f"Failed to load exchange info for {symbol}: {e}")
            return None


    def get_bybit_data(self, unix_since_ms: int, symbol: str, res: str):
        """從 Bybit 獲取 1M 數據並格式化"""
        symbol_usdt = f"{symbol}USDT"
        self.get_exchange_trade(symbol)

        bybit_data = self.bybit.fetchOHLCV(symbol_usdt, res, since=unix_since_ms)

        if not bybit_data:
            return pd.DataFrame()

        df = pd.DataFrame(bybit_data, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['datetime'], unit='ms', utc=True)
        df = df.set_index('date')
        df = df.rename(columns={'close': 'c', 'high': 'h', 'low': 'l', 'open': 'o', 'volume': 'v'})

        return df[['o', 'h', 'l', 'c', 'v']]


    def update_1m_data(self):
        """核心數據庫更新引擎：按月切割 Parquet"""

        if self.strat_df is None or self.strat_df.empty:
            logger.error('strat_df is empty or None.')
            return

        if not {'name', 'symbol'}.issubset(self.strat_df.columns):
            logger.error('strat_df missing required columns: name, symbol')
            return

        res = '1m'
        now_unix = int(datetime.now(timezone.utc).timestamp())

        for _, row in self.strat_df.iterrows():
            symbol = str(row['symbol'])
            symbol_usdt = f"{symbol}USDT"

            existing_files = list(self.data_folder_bybit.glob(f"{symbol_usdt}_*.parquet"))

            if existing_files:
                # 利用檔名排序找到最新月份 (例如 BTCUSDT_2026_03.parquet)
                latest_file = max(existing_files)
                try:
                    df_latest = pd.read_parquet(latest_file)
                    last_ts_ms = int(df_latest.index[-1].timestamp() * 1000)
                    logger.info(f"[{symbol}] Found existing data, fetching since {df_latest.index[-1]}")
                except Exception as e:
                    logger.error(f"Failed to read {latest_file}: {e}")
                    last_ts_ms = (now_unix - 200 * 60) * 1000
            else:
                logger.info(f"[{symbol}] No existing data found. Fetching fresh data.")
                last_ts_ms = (now_unix - 200 * 60) * 1000

            df_new = self.get_bybit_data(last_ts_ms, symbol, res)

            if df_new.empty:
                logger.info(f"[{symbol}] No new data fetched.")
                continue

            df_new['year'] = df_new.index.year
            df_new['month'] = df_new.index.month

            for (year, month), group_df in df_new.groupby(['year', 'month']):
                filename = f"{symbol_usdt}_{year}_{month:02d}.parquet"
                file_path = self.data_folder_bybit / filename

                group_df = group_df.drop(columns=['year', 'month'])

                if file_path.exists():
                    df_existing = pd.read_parquet(file_path)
                    df_combined = pd.concat([df_existing, group_df])
                    df_combined = df_combined[~df_combined.index.duplicated(keep='last')]
                    df_combined = df_combined.sort_index()
                else:
                    df_combined = group_df.sort_index()

                try:
                    df_combined.to_parquet(file_path)
                    logger.success(f"[{symbol}] Saved {len(df_combined)} rows to {filename}")
                except Exception as e:
                    logger.error(f"[{symbol}] Failed to save {filename}: {e}")

        gc.collect()