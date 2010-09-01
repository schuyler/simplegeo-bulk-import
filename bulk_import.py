#!/usr/bin/env python

##### Set your SimpleGeo credentials here

SIMPLEGEO_TOKEN  = ""
SIMPLEGEO_SECRET = ""

##### Documentation

"""
bulk_import.py performs a bulk import of a CSV file containing latitudes and
longitudes, or a GIS point dataset into the SimpleGeo database.

If available, bulk_import.py uses the Python bindings to the OGR library
(http://gdal.org/ogr) to read dozens of GIS vector formats, including ESRI
Shapefiles, GML, KML, GeoRSS, GeoJSON, GPX, and more.

  http://www.gdal.org/ogr/ogr_formats.html

The library uses the python-simplegeo library to write to the SimpleGeo API.

  http://github.com/simplegeo/python-simplegeo

You can set your SimpleGeo credentials in one of two ways: Either set them in
the script at the very top, or create environment variables in your shell named
SIMPLEGEO_TOKEN and SIMPLEGEO_SECRET that contain your credentials.

You can use bulk_import.py in one of two ways: First, as a command line script::

    $ python bulk_import.py <SimpleGeo layer> <GIS dataset> [<ID column>]

e.g.::

    $ python bulk_import.py net.nocat.cities cities.gml name

IMPORTANT NOTE FOR CSV FILES: The CSV file must begin with a header line, and
the columns containing the latitude and longitude *must* be called "latitude"
and "longitude", respectively. This requirement may be relaxed in a future
version.

SimpleGeo records require a unique ID. If your dataset has a unique ID column,
you can provide it. If you leave out the ID column, IDs will be assigned to
records from the dataset sequentially.

If a simple bulk upload isn't sophisticated enough, you can use bulk_import.py
as a library, using a callback from your own script to mutate or reject records
before they are added to your SimpleGeo layer. An example is given in
import_tiger_lm.py, where we want to reject records that lack a "fullname"
attribute::

    from bulk_import import create_client, add_records
    import sys

    def skip_unnamed_landmarks(id, point, attrs):
        if not attrs["fullname"]: return None
        return attrs["pointid"], point, attrs

    client = create_client()
    for input_file in sys.argv[1:]:
        add_records(client, "net.nocat.tigerlm", input_file, skip_unnamed_landmarks)

As you can see, we create a callback that takes a sequential ID, a (lat, lon)
tuple, and a dict of attributes. The callback returns None if we don't want to
store a record from the dataset; otherwise, it returns a tuple (ID, (lat, lon),
attrs) that is used to create the SimpleGeo record. We then call add_records()
from bulk_import.py with a client object, the name of an OGR-readable dataset,
the name of the SimpleGeo layer, and the callback.
"""

##### No user-serviceable parts below this line

import simplegeo
#import shapely.wkb, shapely.geometry
try:
    import osgeo.ogr
    OGR_SUPPORTED = True
except ImportError:
    OGR_SUPPORTED = False
import sys, os, time, csv

def get_csv_feature_count(filename):
    feature_count = 0
    for line in file(filename).xreadlines():
        feature_count += 1
    return feature_count

def read_from_csv(filename):
    csv_file = csv.DictReader(open(filename, mode='U'))
    if "longitude" not in csv_file.fieldnames or "latitude" not in csv_file.fieldnames:
        raise Exception(
            'Required "longitude" and "latitude" columns not found in %s"' % filename)
    for record in csv_file:
        lat = record.pop("latitude")
        lon = record.pop("longitude")
        yield (lon, lat), record

def get_ogr_feature_count (filename):
    source = osgeo.ogr.Open(filename, False)
    if not source: raise Exception("Can't open %s" % filename)

    layer = source.GetLayer(0)
    count = layer.GetFeatureCount()
    return count if count != -1 else None

def read_with_ogr (filename, fatal_errors=True):
    """Read features out of a shapefile and yield a tuple of (geometry, attrs)
       for each feature."""
    source = osgeo.ogr.Open(filename, False)
    if not source: raise Exception("Can't open %s" % filename)

    layer = source.GetLayer(0)
    defn = layer.GetLayerDefn()
    fields = [defn.GetFieldDefn(i).GetName().lower() for i in range(defn.GetFieldCount())]

    layer.ResetReading()
    ogr_feature = layer.GetNextFeature()
    while ogr_feature:
        # try:
        #   geometry = shapely.wkb.loads(ogr_feature.GetGeometryRef().ExportToWkb())
        # except Exception, e:
        #    if fatal_errors:
        #        raise
        #    else:
        #        print >>sys.stderr, "Shapely error:", e
        #    ogr_feature.Destroy()
        #    ogr_feature = layer.GetNextFeature()
        #    continue
        geometry_ref = ogr_feature.GetGeometryRef()
        geometry = (geometry_ref.GetX(), geometry_ref.GetY())
        attrs = {}
        for n, name in enumerate(fields):
            value = ogr_feature.GetField(n)
            if isinstance(value, basestring):
                try:
                    value = value.decode("utf-8")
                except UnicodeDecodeError:
                    value = value.decode("latin-1") 
            attrs[name] = value
        ogr_feature.Destroy()
        yield geometry, attrs
        ogr_feature = layer.GetNextFeature()
    source.Destroy()

def create_client(token=SIMPLEGEO_TOKEN, secret=SIMPLEGEO_SECRET):
    token = os.environ.get("SIMPLEGEO_TOKEN", token)
    secret = os.environ.get("SIMPLEGEO_SECRET", secret)
    return simplegeo.Client(token, secret)

def show_progress (total_imported, feature_count, start_time):
    runtime = time.time() - start_time
    records_per_sec = total_imported/runtime
    if not feature_count:
        print >>sys.stderr, "\r%d saved to %s (%.1f/s)" % (
            total_imported, sg_layer, records_per_sec),
    else:
        remaining = (feature_count - total_imported) / records_per_sec
        print >>sys.stderr, "\r% 6d / % 6d | % 4.1f%% | % 7.1f/s | %d:%02d remaining " % (
            total_imported, feature_count, (total_imported / float(feature_count)) * 100,
            records_per_sec, remaining/60, int(remaining)%60),

def add_records(client, sg_layer, input_file, callback):
    records = []
    start_time = time.time()
    total_imported = 0
    print >>sys.stderr, "Counting features...",
    if input_file.endswith(".csv"):
        layer = read_from_csv(input_file)
        feature_count = get_csv_feature_count(input_file)
    else:
        if not OGR_SUPPORTED:
            raise Exception("OGR Python support is not available")
        layer = read_with_ogr(input_file)
        feature_count = get_ogr_feature_count(input_file)
    print >>sys.stderr, "%d found." % feature_count
    # print >>sys.stderr, "Opening %s..." % input_file
    for id, ((lon, lat), attrs) in enumerate(layer):
        result = callback(id, (lat, lon), attrs)
        if result is None: continue
        id, (lat, lon), attrs = result 
        if "id" in attrs: # having an ID field in the dict breaks the record constructor
            attrs["_id"] = attrs.pop("id")
        record = simplegeo.Record(sg_layer, str(id), lat, lon, type="place", **attrs)
        records.append(record)
        total_imported += 1
        if len(records) == 100:
            client.add_records(sg_layer, records)
            show_progress(total_imported, feature_count, start_time)
            records = []
    if records:
        client.add_records(sg_layer, records)
        show_progress(total_imported, feature_count, start_time)
        print >>sys.stderr, ""

if __name__ == "__main__":
    sg_layer, input_file = sys.argv[1:3]
    id_field = sys.argv[3] if len(sys.argv) >= 4 else None

    def set_id(id, coords, attrs):
        if id_field: id = attrs[id_field]
        return (id, coords, attrs)

    client = create_client()
    add_records(client, sg_layer, input_file, set_id)
