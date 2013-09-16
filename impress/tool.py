from __future__ import absolute_import

import cPickle as pickle
import datetime
import json
import sys

from . import progress
from .backup import BackupFile, NewBackup
from .cache import Slot
from .config import argument_parser, configure, conf, log
from .site import Site
from .storage import Storage

def main(args):
	parser = argument_parser()
	subparsers = parser.add_subparsers()

	ExportCommand(subparsers)
	ExportJsonCommand(subparsers)
	ExportHistoryCommand(subparsers)
	ExportObjectHistoryCommand(subparsers)
	PrintObjectHistoryCommand(subparsers)
	ConvertToJsonCommand(subparsers)
	RestoreCommand(subparsers)
	RestoreHistoryCommand(subparsers)
	ResetCommand(subparsers)

	parsed = parser.parse_args(args)

	configure("tool", redirect_stderr=False)
	progress.enable()

	parsed.func(parsed)

class Command(object):

	args = []

	def __init__(self, subparsers):
		parser = subparsers.add_parser(self.name, help=self.help)

		for arg in self.args:
			parser.add_argument(arg["name"], action=arg["action"])

		parser.set_defaults(func=self)

class JsonMixin(object):

	def dump_backup_as_json(self, backup):
		slot = Slot.load_backup(backup)
		values = { slot.key: { objkey: modeldata.get() for objkey, modeldata in slot.cachedata.iteritems() } }
		json.dump(values, sys.stdout, indent=True)
		print

class ForceMixin(object):

	force_arg = dict(name="--force", action="store_true")

	def check_force(self, args):
		if not args.force:
			print >>sys.stderr, "You must specify --force if your heart is really in it."
			sys.exit(1)

class ExportCommand(Command):

	name = "export"
	help = "copy backup from DynamoDB to stdout"
	args = [
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		backup = Storage(Site(args.sitename)).get_cache_backup()
		if backup:
			NewBackup(backup.load()).dump(sys.stdout)
		else:
			sys.exit(1)

class ExportJsonCommand(Command, JsonMixin):

	name = "export-json"
	help = "load backup from DynamoDB and print it to stdout as JSON"
	args = [
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		backup = Storage(Site(args.sitename)).get_cache_backup()
		if backup:
			self.dump_backup_as_json(backup)
		else:
			sys.exit(1)

class ExportRowMixin(object):

	def export(self, row):
		values = { row.objkey: row.slots }
		print pickle.dumps(values)
		print

class ExportHistoryCommand(Command, ExportRowMixin):

	name = "export-history"
	help = "copy rows from DynamoDB to stdout as pickled"
	args = [
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		try:
			for row in Storage(Site(args.sitename)).iterate_rows():
				self.export(row)

		except KeyboardInterrupt:
			print >>sys.stderr
			print >>sys.stderr, "Interrupted"

class ObjectHistoryMixin(object):

	def get(self, args):
		return Storage(Site(args.sitename))._get(args.objkey)

class ExportObjectHistoryCommand(Command, ObjectHistoryMixin, ExportRowMixin):

	name = "export-object-history"
	help = "copy row from DynamoDB to stdout as pickled"
	args = [
		dict(name="sitename", action="store"),
		dict(name="objkey", action="store"),
	]

	def __call__(self, args):
		row = self.get(args)
		if row:
			self.export(row)

class PrintObjectHistoryCommand(Command, ObjectHistoryMixin):

	name = "print-object-history"
	help = "copy row from DynamoDB to stdout in human-readable format"
	args = [
		dict(name="sitename", action="store"),
		dict(name="objkey", action="store"),
	]

	def __call__(self, args):
		row = self.get(args)
		if row:
			for slotkey, values in sorted(row, reverse=True):
				print "Slot:\t%s" % slotkey

				for k, v in sorted(values.iteritems()):
					if isinstance(v, dict):
						print "   %s:" % k,

						for vk, vv in sorted(v.iteritems()):
							print "\t%s: %s" % (vk, vv)
					else:
						print "   %s:\t%s" % (k, v)

				print

class ConvertToJsonCommand(Command, JsonMixin):

	name = "convert-to-json"
	help = "load backup from FILE and print it to stdout as JSON"
	args = [
		dict(name="filename", action="store")
	]

	def __call__(self, args):
		self.dump_backup_as_json(BackupFile(args.filename))

class RestoreCommand(Command, ForceMixin):

	name = "restore"
	help = "load backup from FILE and store it to DynamoDB"
	args = [
		ForceMixin.force_arg,
		dict(name="filename", action="store"),
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		site = Site(args.sitename)
		storage = Storage(site)
		slot = Slot.load_backup(BackupFile(args.filename))

		self.check_force(args)

		if slot.is_active(site.current_datetime()):
			storage.insert_cache_backup(site, slot.make_backup())
		else:
			if not slot.store(storage):
				sys.exit(1)

class RestoreHistoryCommand(Command, ForceMixin):

	name = "restore-history"
	help = "read pickled rows from FILE and store them to DynamoDB"
	args = [
		ForceMixin.force_arg,
		dict(name="sitename", action="store"),
		dict(name="filename", action="store"),
	]

	def __call__(self, args):
		storage = Storage(Site(args.sitename))
		counter = progress.Counter(interval=100)

		self.check_force(args)

		with open(args.filename) as file:
			try:
				while True:
					for storekey, slots in pickle.load(file).iteritems():
						storage._replace(storekey, slots)
						counter.increment()

					file.read(2)
			except EOFError:
				pass

			counter.done()

class ResetCommand(Command, ForceMixin):

	name = "reset"
	help = "clear the cache backup in DynamoDB"
	args = [
		ForceMixin.force_arg,
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		site = Site(args.sitename)
		storage = Storage(site)
		empty = Slot(site.current_datetime()).make_backup()

		self.check_force(args)

		storage.insert_cache_backup(site, empty)

if __name__ == "__main__":
	main(sys.argv[1:])
