import logging
import os
import queue
import threading
import time

from dataclasses import dataclass
from typing import List, Dict, Tuple

import pydantic
import requests

from tribler_common.simpledefs import NTFY


logging.basicConfig(level=logging.INFO)


SECOND = 1
MINUTE = 60
HOUR = 60 * MINUTE

COLLECTOR_URL = 'http://localhost:3322/tunnel/report'
REPORTING_INTERVAL = 60 * SECOND
MAX_QUEUE_SIZE = 100000

MAX_COUNTER_VALUE = 2 ** 50
MAX_SUM_VALUE = 2 ** 50

NTFY_TUNNEL_REMOVE = "tunnel_remove"


start_time = time.time()


class ReporterSettings(pydantic.BaseSettings):
    collector_url: str = pydantic.Field(COLLECTOR_URL)
    reporting_interval: float = pydantic.Field(REPORTING_INTERVAL)
    max_queue_size: int = pydantic.Field(MAX_QUEUE_SIZE)

    class Config:
        env_file = '.env'
        env_prefix = 'tunnelmetricsreporter_'


@dataclass
class Record:
    t: float
    uptime: float
    uploaded_bytes: int
    downloaded_bytes: int
    circuits_num: int
    rejected_positive_balance_count: int
    rejected_positive_balance_total: float
    rejected_negative_balance_count: int
    rejected_negative_balance_total: float
    removed_circuits_count: int
    removed_circuits_duration_total: float
    removed_circuits_downloaded_total: float
    removed_circuits_uploaded_total: float


class MetricsReporter:
    def __init__(self, session, settings: ReporterSettings = None):
        self.node_name = os.environ.get('ANSIBLE_INVENTORY_HOSTNAME')
        self.instance_index = int(os.environ.get("HELPER_INDEX", "0"))
        self.session = session
        if self.session.tunnel_community is None:
            raise RuntimeError('Tunnel community was not loaded')
        if settings is None:
            settings = ReporterSettings()
        self.settings = settings
        self.queue = queue.Queue()
        self.output_thread = OutputThread(self)
        self.exiting = threading.Event()
        self.finished = False
        self.previous_reject_callback = None
        self.lock = threading.Lock()
        self.removed_circuits_count = 0
        self.removed_circuits_duration_total = 0
        self.removed_circuits_downloaded_total = 0
        self.removed_circuits_uploaded_total = 0
        self.rejected_negative_balance_count = 0
        self.rejected_negative_balance_total = 0
        self.rejected_positive_balance_count = 0
        self.rejected_positive_balance_total = 0

    def start(self):
        logging.info('Starting MetricsReporter output thread')
        self.output_thread.start()
        self.session.register_task('report_statistics', self.report_statistics,
                                   interval=self.settings.reporting_interval)
        self.session.notifier.add_observer(NTFY.TUNNEL_REMOVE, self.circuit_removed)
        tunnel_community = self.session.tunnel_community
        self.previous_reject_callback = tunnel_community.reject_callback
        tunnel_community.reject_callback = self.on_circuit_reject

    def shutdown(self):
        if self.exiting.is_set():
            return
        logging.info('Shutting down MetricsReporter...')
        self.exiting.set()
        self.queue.put(None)
        self.output_thread.join()
        logging.info('MetricsReporter shutdown complete')
        self.finished = True

    def circuit_removed(self, circuit, additional_info):
        duration = time.time() - circuit.creation_time
        # circuit.circuit_id, circuit.bytes_up, circuit.bytes_down
        with self.lock:
            self.removed_circuits_count += 1
            self.removed_circuits_duration_total += duration
            self.removed_circuits_downloaded_total += circuit.bytes_down
            self.removed_circuits_uploaded_total += circuit.bytes_up
            if (
                self.removed_circuits_count > MAX_COUNTER_VALUE or
                self.removed_circuits_duration_total > MAX_SUM_VALUE or
                self.removed_circuits_downloaded_total > MAX_SUM_VALUE or
                self.removed_circuits_uploaded_total > MAX_SUM_VALUE
            ):
                self.removed_circuits_count = 0
                self.removed_circuits_duration_total = 0
                self.removed_circuits_downloaded_total = 0
                self.removed_circuits_uploaded_total = 0

    def on_circuit_reject(self, reject_time, balance):
        balance = float(balance)
        with self.lock:
            if balance > 0:
                self.rejected_positive_balance_count += 1
                self.rejected_positive_balance_total += balance
                if self.rejected_positive_balance_total > MAX_SUM_VALUE:
                    self.rejected_positive_balance_count = 0
                    self.rejected_positive_balance_total = 0
            else:
                self.rejected_negative_balance_count += 1
                self.rejected_negative_balance_total += (-balance)  # should we check for overflow?
                if self.rejected_negative_balance_total > MAX_SUM_VALUE:
                    self.rejected_negative_balance_count = 0
                    self.rejected_negative_balance_total = 0

        if self.previous_reject_callback:
            self.previous_reject_callback(reject_time, balance)

    def report_statistics(self):
        if self.exiting.is_set():
            return

        if self.queue.qsize() > self.settings.max_queue_size:
            self.exiting.set()
            logging.error('MetricsReporter: Max queue size exceeded')
            return

        with self.lock:
            t = time.time()
            self.queue.put(Record(
                t=time.time(),
                uptime=t - self.session.ipv8_start_time,
                uploaded_bytes=self.session.ipv8.endpoint.bytes_up,
                downloaded_bytes=self.session.ipv8.endpoint.bytes_down,
                circuits_num=len(self.session.tunnel_community.circuits),
                removed_circuits_count=self.removed_circuits_count,
                rejected_positive_balance_count = self.rejected_positive_balance_count,
                rejected_positive_balance_total = self.rejected_positive_balance_total,
                rejected_negative_balance_count = self.rejected_negative_balance_count,
                rejected_negative_balance_total = self.rejected_negative_balance_total,
                removed_circuits_duration_total = self.removed_circuits_duration_total,
                removed_circuits_downloaded_total = self.removed_circuits_downloaded_total,
                removed_circuits_uploaded_total = self.removed_circuits_uploaded_total,
            ))

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
            data.append(dict(
                t=record.t,
                uptime=record.uptime,
                uploaded_bytes=record.uploaded_bytes,
                downloaded_bytes=record.downloaded_bytes,
                circuits_num=record.circuits_num,
                removed_circuits_count=record.removed_circuits_count,
                rejected_positive_balance_count=record.rejected_positive_balance_count,
                rejected_positive_balance_total=record.rejected_positive_balance_total,
                rejected_negative_balance_count=record.rejected_negative_balance_count,
                rejected_negative_balance_total=record.rejected_negative_balance_total,
                removed_circuits_duration_total=record.removed_circuits_duration_total,
                removed_circuits_downloaded_total=record.removed_circuits_downloaded_total,
                removed_circuits_uploaded_total=record.removed_circuits_uploaded_total,
            ))
        return data

    def send_data(self, data: List[Dict]):
        # Called from OutputThread
        try:
            t = time.time()
            requests.post(self.settings.collector_url, json={
                'node_name': self.node_name,
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
