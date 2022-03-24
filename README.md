# EcoGisTools
Tools for working with GIS files

## Requirements
This does require QGis for desktop to be installed (which should also automatically instal the GDAL library used),
and all other requirements are in the [requirements file](requirements.txt).

## Usage

The most simple usage of EcoGis is `./ecogis.py <dir>`, which will traverse `dir` for individual shape files,
partition them into seperate layers, and write them to a directory called `out` (change this with the `-o` flag).

All options can be seen by running `./ecogis.py -h`.
