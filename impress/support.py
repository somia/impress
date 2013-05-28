from __future__ import absolute_import

import sys

from . import timeline
from .config import argument_parser, configure, log
from .registry import Registry
from .storage import Storage

def main(args):
	parser = argument_parser()
	parser.add_argument("--store", action="store_true", help="store changes to DynamoDB")
	parser.add_argument("--dump", action="store_true", help="print changes to stdout")
	parser.add_argument("sitename")
	parsed = parser.parse_args(args)

	configure("support")

	registry = Registry()
	site = Site(args.sitename)
	storage = Storage()

	if parsed.store:
		log.info("merging history (storing changes)")
	else:
		log.info("merging history (dry run)")

	scanned = 0
	supported = 0
	merged = 0
	failed = 0

	for row in storage.iterate_rows():
		scanned += 1

		model, pattern = registry.get_model_and_pattern(row.objkey)

		if not model:
			log.warning("no model configured for key %s", row.objkey)
			continue

		if not pattern:
			log.debug("no pattern configured for key %s", row.objkey)
			continue

		supported += 1

		try:
			if timeline.merge(row, model, pattern, parsed.store, parsed.dump):
				merged += 1
		except Exception:
			log.exception("merge failed for key %s", row.objkey)
			failed += 1

	log.info("done: scanned=%d supported=%d merged=%d failed=%d", scanned, supported, merged, failed)

if __name__ == "__main__":
	main(sys.argv[1:])
