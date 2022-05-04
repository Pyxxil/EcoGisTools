#!/usr/bin/env python3

from __future__ import annotations
from itertools import starmap
import re
from typing import Iterable, List, Dict, Optional, Tuple
import logging
import os
import shutil
import argparse
from math import ceil, floor
from pathlib import Path

from tqdm import tqdm, trange
from osgeo import ogr, gdal
from qgis.core import QgsProject, QgsVectorLayer, QgsApplication, QgsCoordinateReferenceSystem, QgsSettings

ECOGIS_LOGGER = "ecogis"
# `None` is basically the same as `False`, just doesn't enable it if the output isn't a TTY
DISABLE_PROGRESS = None

DEFAULT_CRS = 3857


class Partition:
    def __init__(self, layer: Optional[ogr.Layer] = None) -> None:
        self.layer = layer

    def set_layer(self, layer: ogr.Layer):
        self.layer = layer

    def layer_names(self) -> List[str]:
        if self.layer is None:
            logging.getLogger(ECOGIS_LOGGER).error(
                "Tried to partition a layer without setting it"
            )
            return []
        return [
            self.layer.GetName(),
        ]

    def __call__(self) -> Iterable[Tuple[str, ogr.Feature]]:
        if self.layer is None:
            logging.getLogger(ECOGIS_LOGGER).error(
                "Tried to partition a layer without setting it"
            )
            yield (None, None)
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
            logging.getLogger(ECOGIS_LOGGER).error(
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

        logger = logging.getLogger(ECOGIS_LOGGER)
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
        dir = Path(name)
        if dir.exists():
            logging.getLogger(ECOGIS_LOGGER).error(f"{name} already exists")
            return []

        dir.mkdir(parents=True)
        partition.set_layer(self.layer)

        partitions: Dict[str, Tuple[ogr.DataSource, ogr.Layer]] = {}
        for key in partition.layer_names():
            path = dir.joinpath(Path(f"{key}.fgb"))
            if path.exists():
                os.remove(str(path))

            driver: ogr.Driver = ogr.GetDriverByName("FlatGeobuf")
            ds: ogr.DataSource = driver.CreateDataSource(str(path))
            out_layer: ogr.Layer = ds.CreateLayer(
                f"{key}", self.layer.GetSpatialRef(), self.layer.GetLayerDefn().GetGeomType())

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
    def from_file(file: str, name: str) -> Optional[Source]:
        data_source: ogr.DataSource = ogr.Open(file)

        if data_source is None:
            logging.getLogger(ECOGIS_LOGGER).error(
                f"Invalid shapefile: {file}")
            return None

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


shape_file_regex = re.compile(".+\.(shp|fgb)$")


def main(**kwargs) -> None:
    logger = logging.getLogger(ECOGIS_LOGGER)
    logger.info("Finding shapefiles ...")

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

    Path(outdir).mkdir()

    shapefiles: List[str] = []

    for path, _, files in os.walk(indir):
        for file in files:
            if shape_file_regex.fullmatch(file):
                name = os.path.join(outdir, os.path.relpath(
                    os.path.join(path, Path(file).stem), indir))
                shapefiles.append((os.path.join(path, file), name))

    logger.info("Done!")

    # Supply path to qgis install location
    QgsApplication.setPrefixPath("/usr/bin", True)

    # Create a reference to the QgsApplication. Setting the
    # second argument to False disables the GUI.
    qgs = QgsApplication([], False)

    # Load providers
    qgs.initQgis()

    partitions = []

    for src in starmap(Source.from_file, shapefiles):
        if src is None:
            continue

        partitions.extend(
            (src.name, part) for part in src.partition(LatLonPartition(count))
        )
        logger.info(f"Partitioned {src.name}")

    logger.info("Partitioning finished. Creating project ...")

    project = QgsProject.instance()
    crs = QgsCoordinateReferenceSystem.fromEpsgId(DEFAULT_CRS)
    project.setCrs(crs)

    for (part, key) in partitions:
        path = os.path.abspath(os.path.join(os.curdir, f"{part}/{key}.fgb"))
        layer = QgsVectorLayer(path, key, "ogr")
        if not layer.isValid():
            logger.error(f"Invalid layer: {key} ({path})")
            continue

        project.addMapLayer(layer)

    project.write(f"{outdir}/ecogis.qgz")

    logger.info("Project created!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="EcoGis", description="Ecogy GIS Tools")

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
        help="Determine what type of information should be logged",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="count",
        default=0,
        help="Lower the amount of information displayed (-qq will stop almost all output)",
    )
    parser.add_argument(
        "-np",
        "--no-progress",
        action="store_true",
        help="Don't display any progress bars",
    )

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
    # Occassionally some of the shapefiles will come with non-existent or corrupted SHX files
    gdal.SetConfigOption("SHAPE_RESTORE_SHX", "YES")

    args = {"indir": args.input, "outdir": args.output, "layers": args.layers}

    main(**args)
