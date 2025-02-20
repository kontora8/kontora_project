from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.acc_file = None
        self.gps_file = None
        self.parking_file = None
        self.acc_reader = None
        self.gps_reader = None
        self.parking_reader = None
        
    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        try:
            acc_data = next(self.acc_reader)
        except StopIteration:
            self.acc_file.seek(0)
            next(self.acc_reader)
            acc_data = next(self.acc_reader)

        try:
            gps_data = next(self.gps_reader)
        except StopIteration:
            self.gps_file.seek(0)
            next(self.gps_reader)
            gps_data = next(self.gps_reader)

        try:
            parking_data = next(self.parking_reader)
        except StopIteration:
            self.parking_file.seek(0)
            next(self.parking_reader)
            parking_data = next(self.parking_reader)
        
        acc = Accelerometer(
            int(acc_data[0]), 
            int(acc_data[1]), 
            int(acc_data[2])
        )
        
        gps = Gps(
            float(gps_data[0]),
            float(gps_data[1])
        )
        
        parking = Parking(
            int(parking_data[2]),
            Gps(
                float(parking_data[0]),
                float(parking_data[1])
            )
        )

        return AggregatedData(
            acc,
            gps,
            parking,
            datetime.now(),
            config.USER_ID
        )

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.acc_file = open(self.accelerometer_filename)
        self.gps_file = open(self.gps_filename)
        self.parking_file = open(self.parking_filename)
        
        self.acc_reader = reader(self.acc_file)
        self.gps_reader = reader(self.gps_file)
        self.parking_reader = reader(self.parking_file)
        
        next(self.acc_reader)
        next(self.gps_reader)
        next(self.parking_reader)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        if self.acc_file:
            self.acc_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()
