import asyncio
from kivy.app import App
from kivy_garden.mapview import MapMarker, MapView
from kivy.clock import Clock
from kivy.logger import Logger
from lineMapLayer import LineMapLayer
from datasource import Datasource


class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mapview = None
        self.car_marker = None
        self.datasource = Datasource()
        self.map_layer = LineMapLayer()

    def on_start(self):
        """
        Встановлює необхідні маркери, викликає функцію для оновлення мапи
        """
        self.mapview.add_layer(self.map_layer, mode="scatter")
        self.car_marker = MapMarker(source="images/car.png", lat=50.45, lon=30.52)
        self.mapview.add_widget(self.car_marker)
        Clock.schedule_interval(self.update, 1)

    def update(self, *args):
        """
        Викликається регулярно для оновлення мапи
        """
        new_points = self.datasource.get_new_points()
        for lat, lon, road_state in new_points:

            Logger.info(f"Updating car position: lat={lat}, lon={lon}")
            self.update_car_marker((lat, lon))
            self.map_layer.add_point((lat, lon))

            if road_state == "pothole":
                Logger.info(f"Pothole detected at: lat={lat}, lon={lon}")
                self.set_pothole_marker((lat, lon))
            elif road_state == "bump":
                Logger.info(f"Speed bump detected at: lat={lat}, lon={lon}")
                self.set_bump_marker((lat, lon))

    def update_car_marker(self, point):
        """
        Оновлює відображення маркера машини на мапі
        :param point: GPS координати
        """
        Logger.info(f"Car marker moved to: lat={point[0]}, lon={point[1]}")
        self.car_marker.lat, self.car_marker.lon = point

    def set_pothole_marker(self, point):
        """
        Встановлює маркер для ями
        :param point: GPS координати
        """
        pothole_marker = MapMarker(source="images/pothole.png", lat=point[0], lon=point[1])
        self.mapview.add_widget(pothole_marker)

    def set_bump_marker(self, point):
        """
        Встановлює маркер для лежачого поліцейського
        :param point: GPS координати
        """
        bump_marker = MapMarker(source="images/bump.png", lat=point[0], lon=point[1])
        self.mapview.add_widget(bump_marker)

    def build(self):
        """
        Ініціалізує мапу MapView(zoom=15, lat=50.45, lon=30.52)
        :return: мапу
        """
        self.mapview = MapView(zoom=15, lat=50.45, lon=30.52)
        return self.mapview


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MapViewApp().async_run(async_lib="asyncio"))
    loop.close()
