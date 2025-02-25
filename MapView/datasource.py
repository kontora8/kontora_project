import asyncio
import json
from datetime import datetime
import websockets
from kivy import Logger
from pydantic import BaseModel, field_validator
from config import STORE_HOST, STORE_PORT
import pandas as pd
import numpy as np
from scipy.signal import find_peaks

# Pydantic models
class ProcessedAgentData(BaseModel):
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )

class Datasource:
    def __init__(self):
        self.index = 0
        self.connection_status = None
        self._new_points = []
        self.data = pd.read_csv("data.csv")
        self.generate_gps_data()
        self.process_accelerometer_data()
        asyncio.ensure_future(self.connect_to_server())

    def get_new_points(self):
        Logger.debug(self._new_points)
        points = self._new_points
        self._new_points = []
        return points

    async def connect_to_server(self):
        uri = f"ws://{STORE_HOST}:{STORE_PORT}/ws/"
        while True:
            Logger.debug("CONNECT TO SERVER")
            async with websockets.connect(uri) as websocket:
                self.connection_status = "Connected"
                try:
                    while True:
                        data = await websocket.recv()
                        parsed_data = json.loads(data)
                        self.handle_received_data(parsed_data)
                except websockets.ConnectionClosedOK:
                    self.connection_status = "Disconnected"
                    Logger.debug("SERVER DISCONNECT")

    def handle_received_data(self, data):
        Logger.debug(f"Received data: {data}")
        processed_agent_data_list = sorted(
            [
                ProcessedAgentData(**processed_data_json)
                for processed_data_json in json.loads(data)
            ],
            key=lambda v: v.timestamp,
        )
        new_points = [
            (
                processed_agent_data.road_state,
            )
            for processed_agent_data in processed_agent_data_list
        ]
        self._new_points.extend(new_points)

    def generate_gps_data(self):
        """GPS data geterator"""
        num_rows = len(self.data)
        base_lat, base_lon = 50.0, 30.0  # Стартові координати
        self.data["lat"] = (base_lat + np.linspace(0, 0.05, num_rows)).astype(float)
        self.data["lon"] = (base_lon + np.linspace(0, 0.05, num_rows)).astype(float)

    def process_accelerometer_data(self):
        """Accelerometer possibilities"""
        z_values = self.data["Z"].values
        peaks, _ = find_peaks(z_values, height=16680, distance=5, prominence=15, width=2)
        dips, _ = find_peaks(-z_values, height=-16650, distance=5, prominence=15, width=2)

        for i in peaks:
            self._new_points.append((float(self.data.iloc[i]['lat']), float(self.data.iloc[i]['lon']), "bump"))
        for i in dips:
            self._new_points.append((float(self.data.iloc[i]['lat']), float(self.data.iloc[i]['lon']), "pothole"))
