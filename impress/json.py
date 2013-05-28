from __future__ import absolute_import

import json
from json import loads

separators = ",", ":"

def dumps(obj):
	return json.dumps(obj, separators=separators)
