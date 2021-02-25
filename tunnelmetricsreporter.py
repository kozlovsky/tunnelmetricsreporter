import logging
import queue
import threading
import time

from typing import List, Dict, Tuple

import pydantic
import requests


logging.basicConfig(level=logging.INFO)


SECOND = 1
MINUTE = 60
HOUR = 60 * MINUTE

COLLECTOR_URL = 'http://localhost:3322/tunnel/report'
REPORTING_INTERVAL = 60 * SECOND
MAX_QUEUE_SIZE = 100000

MAX_COUNTER_VALUE = 2 ** 31

start_time = time.time()


class ReporterSettings(pydantic.BaseSettings):
    collector_url: str = pydantic.Field(COLLECTOR_URL)
    reporting_interval: float = pydantic.Field(REPORTING_INTERVAL)
    max_queue_size: int = pydantic.Field(MAX_QUEUE_SIZE)

    class Config:
        env_file = '.env'
        env_prefix = 'tunnelmetricsreporter_'


class Record:
    __slots__ = ['t', 'uploaded_bytes', 'downloaded_bytes', 'uptime']
    def __init__(self, t, uploaded_bytes, downloaded_bytes, uptime):
        self.t = t
        self.uploaded_bytes = uploaded_bytes
        self.downloaded_bytes = downloaded_bytes
        self.uptime = uptime


class MetricsReporter:
    def __init__(self, instance_index: int, session, settings: ReporterSettings = None):
        self.instance_index = instance_index
        self.session = session
        if settings is None:
            settings = ReporterSettings()
        self.settings = settings
        self.lock = threading.Lock()
        self.queue = queue.Queue()
        self.output_thread = OutputThread(self)
        self.exiting = threading.Event()
        self.finished = False

    def start(self):
        logging.info('Starting MetricsReporter output thread')
        self.output_thread.start()

    def shutdown(self):
        if self.exiting.is_set():
            return
        logging.info('Shutting down MetricsReporter...')
        self.exiting.set()
        self.queue.put(None)
        self.output_thread.join()
        logging.info('MetricsReporter shutdown complete')
        self.finished = True

    def report_statistics(self):
        if self.exiting.is_set():
            return

        if self.queue.qsize() > self.settings.max_queue_size:
            self.exiting.set()
            logging.error('MetricsReporter: Max queue size exceeded')
            return

        t = time.time()
        endpoint = self.session.ipv8.endpoint
        self.queue.put(Record(time.time(), endpoint.bytes_up, endpoint.bytes_down, t - self.session.ipv8_start_time))

    def wait_for_records(self, thread_name) -> Tuple[bool, List[Record]]:
        exiting = False
        records = []

        try:
            record = self.queue.get_nowait()
        except queue.Empty:
            logging.debug('Waiting in thread %s', thread_name)
            record = self.queue.get()

        while True:
            if record is None:
                exiting = True
                break
            else:
                records.append(record)
                try:
                    record = self.queue.get_nowait()
                except queue.Empty:
                    break
        return exiting, records

    def prepare_data(self, records: List[Record]) -> List[Dict]:
        # Called from OutputThread
        data = []
        for record in records:
            data.append(dict(t=record.t,
                             uploaded_bytes=record.uploaded_bytes,
                             downloaded_bytes=record.downloaded_bytes,
                             uptime=record.uptime))
        return data

    def send_data(self, data: List[Dict]):
        # Called from OutputThread
        try:
            t = time.time()
            requests.post(self.settings.collector_url, json={
                'instance_index': self.instance_index,
                'metrics': data,
            })
            logging.info('Post metrics to `%s` in %.4f seconds',
                         self.settings.collector_url, time.time() - t)
        except Exception as e:
            # No traceback logging to prevent excessive log spam
            logging.error("MetricsReporter %d: %s: %s",
                          self.instance_index, type(e).__name__, e)


class OutputThread(threading.Thread):
    def __init__(self, reporter: MetricsReporter):
        super().__init__(name='MetricsReporterOutput%s' % reporter.instance_index)
        self.reporter = reporter

    def run(self):
        logging.info('Starting thread %s', self.name)
        try:
            exiting = False
            while not exiting:
                exiting, records = self.reporter.wait_for_records(self.name)
                if records:
                    data = self.reporter.prepare_data(records)
                    self.reporter.send_data(data)

        except Exception as e:
            logging.exception('%s: %s: %s', self.name, type(e).__name__, e)

        self.reporter.exiting.set()
        logging.info('Finishing thread %s', self.name)
