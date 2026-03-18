import schedule
import time
import datetime as dt
from loguru import logger

from core.orchestrator import DataSourceConfig
from core.datacenter import DataCenterSrv


def scheduler():
    utc_now = dt.datetime.now(dt.UTC)
    logger.info('Starting algo_seq at (UTC) {}', utc_now.strftime('%Y-%m-%d %H:%M:%S'))

    # 1. Load strategy configuration
    ds = DataSourceConfig()
    ds.create_folder()
    strat_df = ds.load_info_dict()

    if strat_df is None or strat_df.empty:
        logger.error('Failed to load strategy configuration or it is empty.')
        return

    logger.info('Loaded #{} rows of strategy configuration', len(strat_df))
    dcs = DataCenterSrv(strat_df)
    dcs.update_1m_data()

    logger.info('Data fetching and Parquet updating complete')


if __name__ == '__main__':
    logger.info('Starting DataCenter 1M fetching scheduler')
    schedule.every().minute.at(':40').do(scheduler)

    try:
        scheduler()

        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received; program terminated.')