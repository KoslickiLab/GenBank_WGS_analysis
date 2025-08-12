from wgs_sketcher.db import DB
import json, sys
db = DB("/scratch/genbank_wgs/wgs_state/wgs_sketcher.sqlite")
print(json.dumps(db.stats(), indent=2))

