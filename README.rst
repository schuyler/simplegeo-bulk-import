bulk_import.py performs a bulk import of a GIS point dataset into the SimpleGeo
database.

bulk_import.py uses the Python bindings to the OGR library
(http://gdal.org/ogr) to read dozens of GIS vector formats, including
ESRI Shapefiles, GML, KML, GeoRSS, GeoJSON, GPX, and more.

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
