'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from multiprocessing import Process
import time
import logging
import os
from datetime import timedelta

from cryptostore.util import get_time_interval
from cryptostore.aggregator.redis import Redis
from cryptostore.aggregator.kafka import Kafka
from cryptostore.data.storage import Storage
from cryptostore.config import DynamicConfig
from cryptostore.exceptions import EngineWriteError
from cryptofeed.defines import CANDLES, L2_BOOK, FUNDING
import signal

LOG = logging.getLogger('cryptostore')


class Aggregator(Process):
    def __init__(self, config_file=None):
        self.config_file = config_file
        super().__init__()
        self.daemon = True

    def run(self):
        LOG.info("Aggregator running on PID %d", os.getpid())
        loop = asyncio.get_event_loop()
        self.config = DynamicConfig(file_name=self.config_file)
        loop.create_task(self.loop())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        except Exception:
            LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)
            os.kill(os.getppid(), signal.SIGKILL)

    async def loop(self):
        if self.config.cache == 'redis':
            cache = Redis(ip=self.config.redis['ip'],
                          port=self.config.redis['port'],
                          password=os.environ.get('REDIS_PASSWORD', None) or self.config.redis.get('password', None),
                          socket=self.config.redis.socket,
                          del_after_read=self.config.redis['del_after_read'],
                          flush=self.config.redis['start_flush'],
                          retention=self.config.redis.retention_time if 'retention_time' in self.config.redis else None)
            cache2 = Redis(ip=self.config.redis['ip2'],
                          port=self.config.redis['port2'],
                          password=os.environ.get('REDIS_PASSWORD', None) or self.config.redis.get('password', None),
                          socket=self.config.redis.socket,
                          del_after_read=self.config.redis['del_after_read'],
                          flush=self.config.redis['start_flush'],
                          retention=self.config.redis.retention_time if 'retention_time' in self.config.redis else None)
        elif self.config.cache == 'kafka':
            cache = Kafka(self.config.kafka['ip'],
                          self.config.kafka['port'],
                          flush=self.config.kafka['start_flush'])

        interval = self.config.storage_interval
        time_partition = False
        multiplier = 1
        if not isinstance(interval, int):
            if len(interval) > 1:
                multiplier = int(interval[:-1])
                interval = interval[-1]
            base_interval = interval
            if interval in {'M', 'H', 'D'}:
                time_partition = True
                if interval == 'M':
                    interval = 60 * multiplier
                elif interval == 'H':
                    interval = 3600 * multiplier
                else:
                    interval = 86400 * multiplier

        parquet_buffer = dict()
        df_candles = None
        df_thicknesses = None
        df_ask_thickness = None
        df_bid_thickness = None
        df_funding = None
        ask_thickness_mean = 0
        bid_thickness_mean = 0
        while True:
            start, end = None, None
            try:
                aggregation_start = time.time()
                if time_partition:
                    interval_start = aggregation_start
                    if end:
                        interval_start = end + timedelta(seconds=interval + 1)
                    start, end = get_time_interval(interval_start, base_interval, multiplier=multiplier)
                if 'exchanges' in self.config and self.config.exchanges:
                    store = Storage(self.config, parquet_buffer=parquet_buffer)
                    for exchange in self.config.exchanges:
                        for dtype in self.config.exchanges[exchange]:
                            # Skip over the retries arg in the config if present.
                            if dtype in {'retries', 'channel_timeouts', 'http_proxy'}:
                                continue
                            for pair in self.config.exchanges[exchange][dtype] if 'symbols' not in self.config.exchanges[exchange][dtype] else self.config.exchanges[exchange][dtype]['symbols']:
                                LOG.info('Reading %s-%s-%s', exchange, dtype, pair)
                                data = cache.read(exchange, dtype, pair, start=start, end=end)
                                if len(data) == 0:
                                    LOG.info('No data for %s-%s-%s', exchange, dtype, pair)
                                    continue

                                store.aggregate(data)

                                if dtype == CANDLES:
                                    df_candles = store.s[0].data.to_pandas()
                                elif dtype == L2_BOOK:
                                    df_thicknesses = store.s[0].data.to_pandas()
                                    df_ask_thickness = df_thicknesses[df_thicknesses['side']=='ask']
                                    df_bid_thickness = df_thicknesses[df_thicknesses['side']=='bid']
                                    ask_thickness_mean = df_ask_thickness['thickness'].mean()
                                    bid_thickness_mean = df_bid_thickness['thickness'].mean()
                                elif dtype == FUNDING:
                                    df_funding = store.s[0].data.to_pandas()
                                    rate_array = df_funding['rate'].unique()
                                    current_funding_rate = rate_array[-1]

                                if ('FUTURES' or 'DELIVERY') not in exchange:
                                    current_funding_rate = 0

                                retries = 0
                                cache.delete(exchange, dtype, pair)
                                LOG.info('Write Complete %s-%s-%s', exchange, dtype, pair)
                        if df_candles is not None:
                            df_candles['ask_thickness'] = ask_thickness_mean
                            df_candles['bid_thickness'] = bid_thickness_mean
                            df_candles['funding_rate'] = current_funding_rate
                            cache2.add_dataframe(exchange, df_candles)
                    total = time.time() - aggregation_start
                    wait = interval - total
                    if wait <= 0:
                        LOG.warning("Storage operations currently take %.1f seconds, longer than the interval of %d", total, interval)
                        wait = 0.5
                    await asyncio.sleep(wait)
                else:
                    await asyncio.sleep(30)
            except Exception:
                LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)
                os.kill(os.getppid(), signal.SIGKILL)
                raise
