from __future__ import annotations
from faulthandler import disable
from itertools import starmap
import random
import re
from typing import List, Any, Dict, Callable, Tuple
import logging
import os
import shutil
import argparse

from tqdm import tqdm, trange
from osgeo import ogr, gdal

LOGGER_NAME = "ecogis"
# `None` is basically the same as `True`, just doesn't enable it if the output isn't a TTY
DISPLAY_PROGRESS = None


class Partition:
    def __init__(self, closure: Callable[[ogr.Layer, ogr.Feature], Tuple[Any, ogr.Feature]]) -> None:
        self.closure = closure

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        return self.closure(*args, **kwds)


class Layer:
    def __init__(self, layer: ogr.Layer = None) -> None:
        self.__layer = layer

    def split(self, partition: Partition, name: str) -> List[ogr.Layer]:
        partitions: Dict[Any, List[ogr.Feature]] = dict()
        logger = logging.getLogger(LOGGER_NAME)

        for fidx in trange(self.__layer.GetFeatureCount(), desc="Partitioning Features", leave=False, disable=DISPLAY_PROGRESS):
            part = partition(self.__layer.GetFeature(fidx))
            if part is None:
                continue

            (key, value) = part
            if not key in partitions:
                partitions[key] = []
            partitions[key].append(value)

        driver: ogr.Driver = ogr.GetDriverByName("ESRI Shapefile")
        layers = []
        for (key, features) in tqdm(partitions.items(), desc="Creating Output Layers", leave=False, disable=DISPLAY_PROGRESS):
            path = f"{name}/{key}.shp"
            if os.path.exists(path):
                os.remove(path)

            ds: ogr.DataSource = driver.CreateDataSource(path)
            out_layer: ogr.Layer = ds.CreateLayer(
                f"{key}", self.__layer.GetSpatialRef())

            defn: ogr.FeatureDefn = out_layer.GetLayerDefn()
            for idx in range(defn.GetFieldCount()):
                out_layer.AlterFieldDefn(idx, defn.GetFieldDefn(idx))

            for feature in tqdm(features, desc="Adding features to layer", leave=False, disable=DISPLAY_PROGRESS):
                if feature is None:
                    logger.warning("There is a NULL feature here ...")
                    continue

                out_layer.CreateFeature(feature)

            layers.append(out_layer)

        return layers


class Source:
    def __init__(self, driver: ogr.Driver, data_source: ogr.DataSource, name: str) -> None:
        self.driver = driver
        self.data_source = data_source
        self.name = name

    @ staticmethod
    def from_file(file: str, name: str) -> Source:
        data_source: ogr.DataSource = ogr.Open(file)

        return Source(data_source.GetDriver(), data_source, name)

    def split(self, partition: Partition) -> List[Layer]:
        os.mkdir(self.name)
        layers = []
        for lidx in trange(self.data_source.GetLayerCount(), desc=f"Splitting {self.name}", leave=False, disable=DISPLAY_PROGRESS):
            ogr_layer = self.data_source.GetLayer(lidx)
            layer = Layer(ogr_layer)
            layers.extend(layer.split(partition(ogr_layer), self.name))

        return layers


file_regex = re.compile(".+\.shp$")


def main(**kwargs) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Loading all shapefiles ...")

    indir = kwargs["indir"]
    outdir = kwargs["outdir"]

    if not os.path.exists(indir):
        logger.error(f"Input directory: '{indir}' does not exist")
        return

    if os.path.exists(outdir):
        if os.path.isdir(outdir):
            shutil.rmtree(outdir)
        else:
            os.remove(outdir)

    os.mkdir(outdir)

    shapefiles: List[str] = []

    for path, _, files in os.walk(indir):
        for file in files:
            if file_regex.fullmatch(file):
                name = os.path.join(outdir, file[:-4])
                shapefiles.append((os.path.join(path, file), name))

    sources = starmap(Source.from_file, shapefiles)

    logger.info("Done!")

    def partition_by_lat_lon(layer: ogr.Layer) -> Callable[[ogr.Feature], Tuple[Any, ogr.Feature]]:
        logger = logging.getLogger(LOGGER_NAME)
        if layer is None:
            logging.getLogger(LOGGER_NAME).warning(
                "Contains a layer that is NULL")
            return lambda _: None

        extent = layer.GetExtent()
        center_x = (extent[0] + extent[1]) // 2
        center_y = (extent[2] + extent[3]) // 2

        def partition(feature) -> Tuple[Any, ogr.Feature]:
            geometry: ogr.Geometry = feature.GetGeometryRef()
            if geometry is None:
                logger.warning("Found a NULL geometry")
                return None

            geom: ogr.Geometry = geometry.GetBoundary()

            if geom is None:
                logger.warning("Found a NULL geometry")
                return None

            center: ogr.Geometry = geom.Centroid()
            x = center.GetX()
            y = center.GetY()

            if x == 0 and y == 0:
                logger.info("Found a geometry at (0,0)")
                return (random.choice(["BottomLeft", "TopLeft", "BottomRight", "TopRight"]), feature)

            if x <= center_x:
                if y <= center_y:
                    return ("BottomLeft", feature)
                return ("TopLeft", feature)
            elif y > center_y:
                return ("TopRight", feature)

            return ("BottomRight", feature)

        return partition

    for src in sources:
        src.split(Partition(partition_by_lat_lon))
        logger.info(f"Partitioned {src.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="EcoGis", description="Ecogy GIS Tools")

    parser.add_argument("input", type=str,
                        help="The directory to traverse for the input files")
    parser.add_argument("--output", type=str, default="out",
                        help="The directory to place all output files")
    parser.add_argument("--log-level", type=str, default="debug",
                        choices=["debug", "info", "warn", "error"])
    parser.add_argument("--quiet", action="store_true")

    args = parser.parse_args()

    if args.quiet:
        DISPLAY_PROGRESS = False

    logging.basicConfig(level=args.log_level.upper())
    # GDAL sometimes logs a lot of information, which may be useful, but isn't useful for us
    gdal.SetConfigOption("CPL_LOG", "/dev/null")

    args = {
        "indir": args.input,
        "outdir": args.output
    }

    main(**args)
