import yaml
import pandas as pd
from pathlib import Path
from loguru import logger


class DataSourceConfig:
    def __init__(self):
        # 統一獲取專案根目錄 (假設 config 與 core 資料夾平排)
        self.project_root = Path(__file__).parent.parent
        # 外部共享數據庫目錄
        self.share_algo_dir = self.project_root.parent / 'share_algoB'

    def create_folder(self):
        """建立 Bybit 數據資料夾"""
        folder_path = self.share_algo_dir / 'bybit_data'
        folder_path.mkdir(parents=True, exist_ok=True)

    def load_info_dict(self):
        """載入策略設定表 (su_table.csv)"""
        csv_path = self.project_root / 'config' / 'su_table.csv'
        try:
            return pd.read_csv(csv_path)
        except FileNotFoundError:
            logger.error(f'❌ 找不到策略設定檔: {csv_path}')

            return pd.DataFrame()

    def load_bybit_api_config(self, symbol: str):
        """載入特定幣種的 API Keys (config.yaml)"""
        config_path = self.project_root / 'config' / 'config.yaml'
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)

            bybit_sub = f'algo_1m_{symbol.lower()}'
            bybit_sub_api = config.get(bybit_sub, {})

            sub_api = f'{symbol}_1M_API_KEY'
            sub_secret = f'{symbol}_1M_SECRET_KEY'

            # 檢查 Key 是否存在且不為空
            if not bybit_sub_api.get(sub_api) or not bybit_sub_api.get(sub_secret):
                raise ValueError(f'❌ 缺少 API Keys: {sub_api} 或 {sub_secret}')

            return bybit_sub_api

        except FileNotFoundError:
            logger.error(f'❌ 找不到 API 設定檔: {config_path}')
            raise
        except Exception as e:
            logger.error(f'❌ 讀取 config.yaml 時發生錯誤: {e}')
            raise