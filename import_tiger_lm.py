from bulk_import import create_client, add_records
import sys

def skip_unnamed_landmarks(id, point, attrs):
    if not attrs["fullname"]: return None
    return attrs["pointid"], point, attrs

client = create_client()
for input_file in sys.argv[1:]:
    add_records(client, "net.nocat.tigerlm", input_file, skip_unnamed_landmarks)

