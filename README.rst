bulk_import.py performs a bulk import of a GIS point dataset into the SimpleGeo
database.

bulk_import.py uses the Python bindings to the OGR library
(http://gdal.org/ogr) to read dozens of GIS vector formats. The library uses
the python-simplegeo library to write to the SimpleGeo API.

You can set your SimpleGeo credentials in one of two ways: Either set them in
the script at the very top, or create environment variables in your shell named
SIMPLEGEO_TOKEN and SIMPLEGEO_SECRET that contain your credentials.

You can use bulk_import.py in one of two ways: First, as a command line script::

    $ python bulk_import.py <SimpleGeo layer> <GIS dataset> [<ID column>]

e.g.::

    $ python bulk_import.py net.nocat.cities cities.gml name

You can omit the ID column, in which case the first record will be given ID 0,
the second ID 1, and so on.

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
        add_records(client, input_file, "net.nocat.tigerlm", skip_unnamed_landmarks)


