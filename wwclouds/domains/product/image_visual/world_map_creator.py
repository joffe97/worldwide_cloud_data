import mapnik
import pathlib


class WorldMapCreator:
    def __init__(self, resolution: tuple[int, int]):
        self.resolution = resolution

    @property
    def directory(self) -> str:
        return str(pathlib.Path(__file__).parent.resolve())

    @property
    def filename(self) -> str:
        return "world_map.png"

    @property
    def filepath(self) -> str:
        return f"{self.directory}/{self.filename}"

    @property
    def shapefile_path(self) -> str:
        return f"{self.directory}/world_borders.shp"

    def create_image(self) -> None:
        m = mapnik.Map(self.resolution[0], self.resolution[1],
                       "+datum=WGS84 +lat_0=0 +lat_ts=0 +lon_0=0 +no_defs +proj=eqc +type=crs +units=m +x_0=0 +y_0=0")
        m.background = mapnik.Color("#001044")
        r = mapnik.Rule()
        polygons = mapnik.PolygonSymbolizer()
        polygons.fill = mapnik.Color('#003F00')
        lines = mapnik.LineSymbolizer()
        lines.fill = mapnik.Color('#444444')
        for symbol in polygons, lines:
            r.symbols.append(symbol)
        s = mapnik.Style()
        s.rules.append(r)
        m.append_style('My Style', s)
        layer = mapnik.Layer("+datum=WGS84 +lat_0=0 +lat_ts=0 +lon_0=0 +no_defs +proj=eqc +type=crs +units=m +x_0=0 +y_0=0")
        layer.datasource = mapnik.Shapefile(file=self.shapefile_path)
        layer.styles.append('My Style')
        m.layers.append(layer)
        m.zoom_all()
        envelope = m.envelope()
        new_x, new_y = (envelope[2] - envelope[0]) // 2, (envelope[3] - envelope[1]) // 2
        m.zoom_to_box(mapnik.Box2d(-new_x, -new_y, new_x, new_y))
        mapnik.render_to_file(m, self.filepath, 'png')


if __name__ == '__main__':
    WorldMapCreator((6000, 3000)).create_image()
