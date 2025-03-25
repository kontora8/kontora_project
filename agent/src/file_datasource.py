import csv
from datetime import datetime
from typing import Optional, TextIO

from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename

        self.is_reading = False

        self.acc_file: Optional[TextIO] = None
        self.gps_file: Optional[TextIO] = None

        self.acc_reader: Optional[csv.DictReader] = None
        self.gps_reader: Optional[csv.DictReader] = None

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        if not self.is_reading:
            raise RuntimeError("startReading() must be called before reading data")

        if not self.acc_reader or not self.gps_reader:
            raise RuntimeError("File readers not initialized")

        acc_row = next(self.acc_reader, None)
        gps_row = next(self.gps_reader, None)

        if acc_row is None:
            self._rewind_acc_file()
            acc_row = next(self.acc_reader)

        if gps_row is None:
            self._rewind_gps_file()
            gps_row = next(self.gps_reader)
        
        print(acc_row)

        acc_data = Accelerometer(
            x=int(acc_row["x"]), y=int(acc_row["y"]), z=int(acc_row["z"]), air=int(acc_row["air"]), noise=int(acc_row["noise"])
        )

        gps_data = Gps(
            longitude=float(gps_row["longitude"]), latitude=float(gps_row["latitude"])
        )

        return AggregatedData(
            accelerometer=acc_data,
            gps=gps_data,
            timestamp=datetime.now(),
            user_id=config.USER_ID,
        )

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.acc_file = open(self.accelerometer_filename, "r")
        self.gps_file = open(self.gps_filename, "r")

        self.acc_reader = csv.DictReader(self.acc_file)
        self.gps_reader = csv.DictReader(self.gps_file)

        self.is_reading = True

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        self.is_reading = False

        if self.acc_file:
            self.acc_file.close()
            self.acc_file = None
            self.acc_reader = None

        if self.gps_file:
            self.gps_file.close()
            self.gps_file = None
            self.gps_reader = None

    def _rewind_acc_file(self):
        """Скидання потоку файлу акселерометра на початок для реалізації нескінченного читання"""
        if self.acc_file:
            self.acc_file.seek(0)
            self.acc_reader = csv.DictReader(self.acc_file)

    def _rewind_gps_file(self):
        """Скидання потоку файлу GPS на початок для реалізації нескінченного читання"""
        if self.gps_file:
            self.gps_file.seek(0)
            self.gps_reader = csv.DictReader(self.gps_file)

    def __del__(self):
        """Деструктор для закриття файлів"""
        self.stopReading()
