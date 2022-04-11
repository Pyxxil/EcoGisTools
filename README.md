# EcoGisTools

Tools for working with GIS files

## Requirements

This does require QGis for desktop to be installed (which should also automatically instal the GDAL library used),
and all other requirements are in the [requirements file](requirements.txt).

## Usage

The most simple usage of EcoGis is `./ecogis.py <dir>`, which will traverse `dir` for individual shape files,
partition them into seperate layers, and write them to a directory called `out` (change this with the `-o` flag).

All options can be seen by running `./ecogis.py -h`.

Once run, the output directory will contain a `ecogis.qgz` QGIS project file. Opening this file in QGIS will load
all of the layers created by the program.

### QGIS Server

In order to have the file work with QGIS server properly, you will need to load the project into QGIS.

**NOTE**: You may want to untick "Render" in the bottom right of the QGIS window, which should improve performance

Go to:
 1. Project -> Properties
 2. Go to the QGIS Server tab
 3. Tick "Enable Service Capabilities"
 4. Under WMS
    1. Tick "CRS restrictions", Click "Used"
    2. Tick both "Exclude layouts" and "Exclude layers", but leave both empty
    3. Tick "Add geometry to feature response"
 5. Click "Apply", and then "OK"
 6. Save the file
 7. Close QGIS, and now the project is ready to be used with QGIS server