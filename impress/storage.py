""" External storage interface.
"""

from __future__ import absolute_import

import cPickle as pickle
import time

import boto.dynamodb

from . import eventlog
from . import json
from .backup import BackupData
from .config import conf, log
from .site import Site

INTERNAL_OBJKEY_PREFIX = "_"
CACHE_BACKUP_OBJKEY    = INTERNAL_OBJKEY_PREFIX + "cache"
CACHE_BACKUP_SLOTKEY   = "backup"

class Storage(object):
	""" DynamoDB abstraction.
	"""

	def __init__(self, site):
		""" @type site: Site
		"""
		self.site = site

		params = {}
		for key in ["aws_access_key_id", "aws_secret_access_key"]:
			value = conf.get("dynamodb", key, None)
			if value is not None:
				params[key] = value

		self.__conn = boto.dynamodb.connect_to_region(conf.get("dynamodb", "region"), **params)
		self.__table = None

	@property
	def table(self):
		if not self.__table:
			self.__table = self.__conn.get_table(name=self.site.dynamodb_table_name)

		return self.__table

	def reset(self):
		self.__conn.layer1.close()
		self.__table = None

	def _get(self, objkey):
		return self._make_row(
			objkey,
			self.table.query(
				hash_key           = objkey,
				consistent_read    = False,
				scan_index_forward = False,
			),
		)

	def insert(self, objkey, slotkey, values):
		""" Insert a single column to single key, encoded as JSON.
		    Does eventlogging.

		    @type objkey:  str
		    @type slotkey: str
		    @type values:  dict
		"""
		evlog_error = eventlog.ERROR_DYNAMODB
		try:
			self._insert(objkey, slotkey, values)
			evlog_error = 0
		finally:
			evlog_size = 0 # TODO
			evlog_type = ord(objkey[0])
			eventlog.logger.store(self.site.name, evlog_error, evlog_size, evlog_type)

	def _insert(self, objkey, slotkey, values):
		""" Insert columns to a single key.  Values are encoded as
		    JSON.  This is a low-level interface without eventlogging.

		    @type objkey: str
		    @type slots:  dict
		"""
		item = self.table.new_item(objkey, slotkey)

		for k, v in values.iteritems():
			if not isinstance(v, (int, long, float)):
				v = json.dumps(v)

			item[k] = v

		item.put()

	def _replace(self, objkey, slots):
		""" Remove all columns of a single key.  This is a low-level
		    interface without eventlogging.

		    @type objkey: str
		    @type slots:  dict
		"""
		TODO

	def mutate(self, row, insert={}, remove=[]):
		""" Insert and/or remove columns of a single key.  Values are
		    encoded as JSON.  Does eventlogging.

		    @type row:    Row
		    @type insert: dict(str=dict)
		    @type remove: list(str)
		"""
		insertdata = { k: json.dumps(v) for k, v in insert.iteritems() }

		evlog_error = eventlog.ERROR_DYNAMODB
		try:
			TODO
			evlog_error = 0
		finally:
			evlog_size = sum(len(v) for v in insertdata.itervalues())
			evlog_type = ord(row.objkey[0])
			eventlog.logger.mutate(self.site.name, evlog_error, evlog_size, evlog_type)

	def iterate_rows(self):
		""" Iterate through the stored objects (excluding cache
		    backups).

		    @rtype iterator(Row)
		"""
		objkey = None
		items = None

		for item in self.table.scan():
			if item.hash_key.startswith(INTERNAL_OBJKEY_PREFIX):
				continue

			if item.hash_key == objkey:
				items.append(item)
			else:
				if objkey is not None:
					yield self._make_row(objkey, items)

				objkey = item.hash_key
				items = [item]

		if objkey:
			yield self._make_row(objkey, items)

	def insert_cache_backup(self, backup):
		""" @type backup: NewBackup
		"""
		data = backup.dumps()

		evlog_error = eventlog.ERROR_DYNAMODB
		try:
			item = self.table.new_item(CACHE_BACKUP_OBJKEY, CACHE_BACKUP_SLOTKEY)
			item["data"] = data
			item["time"] = time.time()
			item.put()

			evlog_error = 0
		finally:
			eventlog.logger.cache_backup(self.site.name, evlog_error, len(data), False)

	def get_cache_backup(self):
		""" @rtype BackupData | NoneType
		"""
		try:
			item = self.table.get_item(
				hash_key        = CACHE_BACKUP_OBJKEY,
				range_key       = CACHE_BACKUP_SLOTKEY,
				consistent_read = False,
			)
			return BackupData(item["data"].encode("ascii"), item["time"])
		except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
			return None

	def _make_row(self, objkey, items):
		slots = {}

		for item in items:
			values = {}

			for k, v in item.iteritems():
				if k not in (item._hash_key_name, item._range_key_name):
					values[k] = json.loads(v)

			slots[item.range_key] = values

		return Row(objkey, slots, self)

class Row(object):
	""" A stored object's data.
	"""
	def __init__(self, objkey, slots, storage):
		self.objkey = objkey
		self.slots = slots
		self.storage = storage

	def __iter__(self):
		""" Iterate through time slots.

		    @rtype iterator(dict(str=dict))
		"""
		return self.slots.iteritems()

	def mutate(self, insert={}, remove=[]):
		""" Insert and/or remove columns.  Values are encoded as JSON.

		    @type insert: dict(str=dict)
		    @type remove: list(str)
		"""
		self.storage.mutate(self, insert, remove)
