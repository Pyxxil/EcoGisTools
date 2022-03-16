from __future__ import annotations
from itertools import starmap
import re
from typing import Iterable, List, Dict, Optional, Tuple
import logging
import os
import shutil
import argparse
from math import ceil, floor

from tqdm import tqdm, trange
from osgeo import ogr, gdal
from qgis.core import QgsProject, QgsVectorLayer, QgsApplication

from memory_profiler import memory_usage

LOGGER_NAME = "ecogis"
# `None` is basically the same as `False`, just doesn't enable it if the output isn't a TTY
DISABLE_PROGRESS = None


class Partition:
    def __init__(self, layer: Optional[ogr.Layer] = None) -> None:
        self.layer = layer

    def set_layer(self, layer: ogr.Layer):
        self.layer = layer

    def layer_names(self) -> List[str]:
        if self.layer is None:
            logging.getLogger(LOGGER_NAME).error(
                "Tried to partition a layer without setting it"
            )
            return []
        return [
            self.layer.GetName(),
        ]

    def __call__(self) -> Iterable[Tuple[str, ogr.Feature]]:
        if self.layer is None:
            logging.getLogger(LOGGER_NAME).error(
                "Tried to partition a layer without setting it"
            )
            yield None
            return

        for fidx in range(self.layer.GetFeatureCount()):
            yield (self.layer_names()[0], self.layer.GetFeature(fidx))


class LatLonPartition(Partition):
    def __init__(self, count: int) -> None:
        super(Partition).__init__()
        self.count = count

        self.partitions: Optional[List[Tuple[int, int, int, int]]] = None

    def partition_to_layer_name(self, part) -> str:
        return f"{self.layer.GetName()}:{part[0]}-{part[1]}-{part[2]}-{part[3]}"

    def create_partitions(self):
        if self.layer is None:
            logging.getLogger(LOGGER_NAME).error(
                "Tried to partition a layer without setting it"
            )
            return
        elif self.partitions is not None:
            return

        extent: Tuple[int, int, int, int] = self.layer.GetExtent()
        if self.count == 1:
            self.partitions = [extent]
        elif self.count == 2:
            self.partitions = [
                (extent[0], extent[1] / 2, extent[2], extent[3]),
                (extent[1] / 2, extent[1], extent[2], extent[3]),
            ]
        else:
            top = ceil(self.count / 2)
            bottom = floor(self.count / 2)

            dt = int((extent[1] - extent[0]) // top)
            db = int((extent[1] - extent[0]) // bottom)
            self.partitions = [
                (
                    extent[0] + x * dt,
                    extent[0] + dt * (x + 1),
                    extent[2],
                    (extent[3] - extent[2]) // 2 + extent[2],
                )
                for x in range(top)
            ]
            self.partitions.extend(
                (
                    extent[0] + x * db,
                    extent[0] + db * (x + 1),
                    (extent[3] - extent[2]) // 2 + extent[2],
                    extent[3],
                )
                for x in range(bottom)
            )

    def layer_names(self) -> List[str]:
        self.create_partitions()

        return [self.partition_to_layer_name(part) for part in self.partitions]

    def __call__(self) -> Iterable[Tuple[str, ogr.Feature]]:
        self.create_partitions()

        logger = logging.getLogger(LOGGER_NAME)
        for fidx in range(self.layer.GetFeatureCount()):
            feature: ogr.Feature = self.layer.GetFeature(fidx)
            geometry: ogr.Geometry = feature.GetGeometryRef()
            if geometry is None:
                logger.warning("Found a NULL geometry")
                yield (None, None)
                continue

            (minX, maxX, minY, maxY) = geometry.GetEnvelope()

            x = (minX + maxX) // 2
            y = (minY + maxY) // 2

            for partition in self.partitions:
                (pMinX, pMaxX, pMinY, pMaxY) = partition
                if pMinX < x <= pMaxX and pMinY < y <= pMaxY:
                    yield (self.partition_to_layer_name(partition), feature)
                    break
            else:
                # This seems to happen on occassion. Not sure why, however
                # we'll just shove it into the last layer
                yield (self.partition_to_layer_name(self.partitions[-1]), feature)


class Layer:
    def __init__(self, layer: ogr.Layer = None) -> None:
        self.layer = layer

    def partition(self, partition: Partition, name: str):
        os.mkdir(name)
        partition.set_layer(self.layer)

        partitions: Dict[str, Tuple[ogr.DataSource, ogr.Layer]] = {}
        for key in partition.layer_names():
            path = f"{name}/{key}.fgb"
            if os.path.exists(path):
                os.remove(path)

            driver: ogr.Driver = ogr.GetDriverByName("FlatGeobuf")
            ds: ogr.DataSource = driver.CreateDataSource(path)
            out_layer: ogr.Layer = ds.CreateLayer(f"{key}", self.layer.GetSpatialRef())

            defn: ogr.FeatureDefn = out_layer.GetLayerDefn()
            for idx in range(defn.GetFieldCount()):
                out_layer.AlterFieldDefn(idx, defn.GetFieldDefn(idx))

            partitions[key] = (ds, out_layer)

        for (key, feature) in tqdm(
            partition(),
            total=self.layer.GetFeatureCount(),
            desc="Partitioning Features",
            leave=False,
            disable=DISABLE_PROGRESS,
        ):
            if key is None:
                continue
            partitions[key][1].CreateFeature(feature)

        for (ds, __) in partitions.values():
            ds.SyncToDisk()

        return list(partitions.keys())


class Source:
    def __init__(
        self, driver: ogr.Driver, data_source: ogr.DataSource, name: str
    ) -> None:
        self.driver = driver
        self.data_source = data_source
        self.name = name

    @staticmethod
    def from_file(file: str, name: str) -> Source:
        data_source: ogr.DataSource = ogr.Open(file)

        return Source(data_source.GetDriver(), data_source, name)

    def partition(self, partition: Partition):
        partitions = []
        for lidx in trange(
            self.data_source.GetLayerCount(),
            desc=f"Splitting {self.name}",
            leave=False,
            disable=DISABLE_PROGRESS,
        ):
            ogr_layer = self.data_source.GetLayer(lidx)
            partitions.extend(Layer(ogr_layer).partition(partition, self.name))
        return partitions


file_regex = re.compile(".+\.(shp|fgb)$")


def main(**kwargs) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Loading all shapefiles ...")

    indir = kwargs.pop("indir", "input")
    outdir = kwargs.pop("outdir", "out")
    count = kwargs.pop("layers", 4)

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

    logger.info("Done!")

    # Supply path to qgis install location
    QgsApplication.setPrefixPath("/usr/local/bin/", True)

    # Create a reference to the QgsApplication.  Setting the
    # second argument to False disables the GUI.
    qgs = QgsApplication([], False)

    # Load providers
    qgs.initQgis()

    partitions = []

    for src in starmap(Source.from_file, shapefiles):
        partitions.extend(
            (src.name, part) for part in src.partition(LatLonPartition(count))
        )
        logger.info(f"Partitioned {src.name}")

    # print(max(memory_usage(split)))

    project = QgsProject.instance()

    for (part, key) in partitions:
        path = os.path.abspath(os.path.join(os.curdir, f"{part}/{key}.fgb"))
        layer = QgsVectorLayer(path, key, "ogr")
        if not layer.isValid():
            logger.error(f"Invalid layer: {key} ({path})")
            continue

        project.addMapLayer(layer)

    project.write(f"{outdir}/ecogis.qgz")

    # Finally, exitQgis() is called to remove the
    # provider and layer registries from memory
    qgs.exitQgis()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="EcoGis", description="Ecogy GIS Tools")

    parser.add_argument(
        "input", type=str, help="The directory to traverse for the input files"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="out",
        help="The directory to place all output files",
    )
    parser.add_argument(
        "-l",
        "--layers",
        type=int,
        default=4,
        help="The number of layers to partition an input layer into",
    )
    parser.add_argument(
        "-ll",
        "--log-level",
        type=str,
        default="debug",
        choices=["debug", "info", "warn", "error"],
    )
    parser.add_argument("-q", "--quiet", action="count", default=0)
    parser.add_argument("-np", "--no-progress", action="store_true")

    args = parser.parse_args()

    if args.layers < 1:
        parser.error("You must have at least 1 output layer")

    if args.quiet > 0 or args.no_progress:
        DISABLE_PROGRESS = True
    if args.quiet > 1:
        args.log_level = "error"

    logging.basicConfig(level=args.log_level.upper())
    # GDAL sometimes logs a lot of information, which may be useful, but isn't useful for us
    gdal.SetConfigOption("CPL_LOG", "/dev/null")

    args = {"indir": args.input, "outdir": args.output, "layers": args.layers}

    main(**args)
