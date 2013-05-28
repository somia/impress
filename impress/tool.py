from __future__ import absolute_import

import cPickle as pickle
import datetime
import json
import sys

from . import progress
from .backup import BackupFile, NewBackup
from .cache import Day
from .config import argument_parser, configure, conf, log
from .site import Site
from .storage import Storage

def main(args):
	parser = argument_parser()
	subparsers = parser.add_subparsers()

	ExportCommand(subparsers)
	ExportJsonCommand(subparsers)
	ExportHistoryFromCassandraCommand(subparsers)
	ExportHistoryFromDynamoCommand(subparsers)
	ExportObjectHistoryCommand(subparsers)
	PrintObjectHistoryFromCassandraCommand(subparsers)
	PrintObjectHistoryFromDynamoCommand(subparsers)
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
		day = Day.load_backup(backup)
		values = { day.key: { objkey: modeldata.get() for objkey, modeldata in day.cachedata.iteritems() } }
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
	help = "copy backup from Cassandra to stdout"
	args = [
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		backup = Storage().get_cache_backup(Site(args.sitename))
		if backup:
			NewBackup(backup.load()).dump(sys.stdout)
		else:
			sys.exit(1)

class ExportJsonCommand(Command, JsonMixin):

	name = "export-json"
	help = "load backup from Cassandra and print it to stdout as JSON"
	args = [
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		backup = Storage().get_cache_backup(Site(args.sitename))
		if backup:
			self.dump_backup_as_json(backup)
		else:
			sys.exit(1)

class ExportRowMixin(object):

	def export(self, row):
		values = { row.storekey: row.slots }
		print pickle.dumps(values)
		print

class ExportHistoryCommandMixin(Command, ExportRowMixin):

	def __call__(self, args):
		try:
			for row in self.rows():
				self.export(row)

		except KeyboardInterrupt:
			print >>sys.stderr
			print >>sys.stderr, "Interrupted"

class ExportHistoryFromCassandraMixin(object):

	def rows(self):
		return Storage().iterate_rows()

class ExportHistoryFromDynamoMixin(object):

	def rows(self):
		return Storage().iterate_rows_dynamo()

class ExportHistoryFromCassandraCommand(ExportHistoryCommandMixin, ExportHistoryFromCassandraMixin):

	name = "export-history"
	help = "copy rows from Cassandra to stdout as pickled"

class ExportHistoryFromDynamoCommand(ExportHistoryCommandMixin, ExportHistoryFromDynamoMixin):

	name = "export-history-from-dynamodb"
	help = "copy rows from DynamoDB to stdout as pickled"

class ObjectHistoryFromCassandraMixin(object):

	def get(self, args):
		return Storage()._get(Site(args.sitename), args.objkey)

class ObjectHistoryFromDynamoMixin(object):

	def get(self, args):
		return Storage()._get_dynamo(Site(args.sitename), args.objkey)

class ExportObjectHistoryCommand(Command, ExportRowMixin, ObjectHistoryFromCassandraMixin):

	name = "export-object-history"
	help = "copy row from Cassandra to stdout as pickled"
	args = [
		dict(name="sitename", action="store"),
		dict(name="objkey", action="store"),
	]

	def __call__(self, args):
		row = self.get(args)
		if row:
			self.export(row)

class PrintObjectHistoryCommandMixin(Command):

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

class PrintObjectHistoryFromCassandraCommand(PrintObjectHistoryCommandMixin, ObjectHistoryFromCassandraMixin):

	name = "print-object-history"
	help = "copy row from Cassandra to stdout in human-readable format"
	args = [
		dict(name="sitename", action="store"),
		dict(name="objkey", action="store"),
	]

class PrintObjectHistoryFromDynamoCommand(PrintObjectHistoryCommandMixin, ObjectHistoryFromDynamoMixin):

	name = "print-object-history-from-dynamodb"
	help = "copy row from DynamoDB to stdout in human-readable format"
	args = [
		dict(name="sitename", action="store"),
		dict(name="objkey", action="store"),
	]

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
	help = "load backup from FILE and store it to Cassandra"
	args = [
		ForceMixin.force_arg,
		dict(name="filename", action="store"),
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		site = Site(args.sitename)
		storage = Storage()
		day = Day.load_backup(BackupFile(args.filename))

		self.check_force(args)

		if day.is_active(site.current_date()):
			storage.insert_cache_backup(site, day.make_backup())
		else:
			if not day.store(storage):
				sys.exit(1)

class RestoreHistoryCommand(Command, ForceMixin):

	name = "restore-history"
	help = "read pickled rows from FILE and store them to Cassandra"
	args = [
		ForceMixin.force_arg,
		dict(name="filename", action="store"),
	]

	def __call__(self, args):
		storage = Storage()
		counter = progress.Counter(interval=100)

		self.check_force(args)

		with open(args.filename) as file:
			try:
				while True:
					for storekey, slots in pickle.load(file).iteritems():
						storage._remove(storekey)
						storage._insert(storekey, slots)
						counter.increment()

					file.read(2)
			except EOFError:
				pass

			counter.done()

class ResetCommand(Command, ForceMixin):

	name = "reset"
	help = "clear the cache backup in Cassandra"
	args = [
		ForceMixin.force_arg,
		dict(name="sitename", action="store"),
	]

	def __call__(self, args):
		site = Site(args.sitename)
		storage = Storage()
		empty = Day(site.current_date()).make_backup()

		self.check_force(args)

		storage.insert_cache_backup(site, empty)

if __name__ == "__main__":
	main(sys.argv[1:])
