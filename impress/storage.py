""" External storage interface.
"""

from __future__ import absolute_import

import cPickle as pickle
import time

import cassandra.ttypes as cassandratypes
import pycassa

import boto

from . import json
from .backup import BackupData
from .config import conf, log
from .site import Site

BUFFER_SIZE             = 32    # pycassa doesn't work with 1
COLUMN_COUNT            = 750

CONNECT_TIMEOUT         = 5
RETRY_COUNT             = 4
RETRY_DELAY             = 5

INTERNAL_KEY_PREFIX     = "x-"
CACHE_BACKUP_KEY_FORMAT = INTERNAL_KEY_PREFIX + "cache-backup-%s"
CACHE_BACKUP_COLUMN     = "pickled"

def retry(func):
	""" Execute func() until it succeeds.  RETRY_COUNT and RETRY_DELAY
	    apply.
	"""
	count = RETRY_COUNT

	while True:
		try:
			return func()
		except pycassa.NoServerAvailable:
			if count > 0:
				count -= 1
				log.warning("sleeping and reconnecting to cassandra")
				time.sleep(RETRY_DELAY)
			else:
				raise

class Storage(object):
	""" Cassandra/DynamoDB abstraction.
	"""

	__dynamo_site_suffix = None

	@classmethod
	def get_dynamo_site_suffix(cls):
		if cls.__dynamo_site_suffix is None:
			cls.__dynamo_site_suffix = conf.get("dynamodb", "site_suffix")
		return cls.__dynamo_site_suffix

	def __init__(self):
		servers = []
		for address in conf.get("cassandra", "servers").split():
			if ":" not in address:
				address += ":9160"
			servers.append(address)

		self.servers = servers
		self.keyspace = conf.get("cassandra", "keyspace")
		self.familyname = conf.get("cassandra", "column_family")
		self.logins = { self.keyspace: {} }

		self._client = None
		self._family = None

		try:
			self.dynamo_table = boto.connect_dynamodb(
				aws_access_key_id     = conf.get("dynamodb", "aws_access_key_id"),
				aws_secret_access_key = conf.get("dynamodb", "aws_secret_access_key"),
				host                  = conf.get("dynamodb", "host"),
			).get_table(
				name                  = conf.get("dynamodb", "table"),
			)
		except:
			log.exception("DynamoDB init failed")
			self.dynamo_table = None

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()

	def connect(self):
		if not self._family:
			self._client = pycassa.connect(servers=self.servers, logins=self.logins, framed_transport=True, timeout=CONNECT_TIMEOUT)
			self._family = pycassa.ColumnFamily(self._client, self.keyspace, self.familyname, BUFFER_SIZE)

		return self._family

	def close(self):
		if self._client:
			conn = self._client

			self._client = None
			self._family = None

			try:
				if conn._client:
					conn._transport.close()
					conn._client = None
			except:
				log.exception("cassandra connection closing failed")

	def _get(self, site, objkey):
		""" Get a single key, encoded as JSON.  This is a low-level
		    interface without eventlogging.

		    @type  site:   Site
		    @type  objkey: str
		    @rtype         Row | None
		"""
		storekey = self.make_key(site, objkey)
		try:
			columns = retry(lambda: self.connect().get(storekey, column_reversed=True))
			return self._make_row(storekey, columns)
		except cassandratypes.NotFoundException:
			return None

	def _get_dynamo(self, site, objkey):
		hashkey = self.make_dynamo_hashkey(site, objkey)
		return self._make_row_dynamo(
			hashkey,
			self.dynamo_table.query(
				hash_key           = hashkey,
				consistent_read    = False,
				scan_index_forward = False,
			),
		)

	def insert(self, site, objkey, slotkey, values):
		""" Insert a single column to single key, encoded as JSON.
		    Does eventlogging.

		    @type site:    Site
		    @type objkey:  str
		    @type slotkey: str
		    @type values:  dict
		"""
		storekey = self.make_key(site, objkey)
		slots = { slotkey: values }

		evlog_error = eventlog.ERROR_CASSANDRA
		try:
			self._insert(storekey, slots)
			evlog_error = 0
		finally:
			evlog_size = 0 # TODO
			evlog_type = ord(objkey[0])
			eventlog.logger.store(site.name, evlog_error, evlog_size, evlog_type)

	def _insert(self, storekey, slots):
		""" Insert columns to a single key.  Values are encoded as
		    JSON.  This is a low-level interface without eventlogging.

		    @type storekey: str
		    @type slots:    dict
		"""
		columns = { k: json.dumps(v) for k, v in slots.iteritems() }
		retry(lambda: self.connect().insert(storekey, columns))

	def insert_dynamo(self, site, objkey, slotkey, values):
		item = self.dynamo_table.new_item(self.make_dynamo_hashkey(site, objkey), slotkey)

		for k, v in values.iteritems():
			item[k] = json.dumps(v)

		item.put()

	def _remove(self, storekey):
		""" Remove all columns of a single key.  This is a low-level
		    interface without eventlogging.

		    @type storekey: str
		"""
		retry(lambda: self.connect().remove(storekey))

	def mutate(self, row, insert={}, remove=[]):
		""" Insert and/or remove columns of a single key.  Values are
		    encoded as JSON.  Does eventlogging.

		    @type row:    Row
		    @type insert: dict(str=dict)
		    @type remove: list(str)
		"""
		insertdata = { k: json.dumps(v) for k, v in insert.iteritems() }

		evlog_error = eventlog.ERROR_CASSANDRA
		try:
			retry(lambda: self.connect().insert_remove(row.storekey, insertdata, remove))
			evlog_error = 0
		finally:
			evlog_size = sum(len(v) for v in insertdata.itervalues())
			evlog_type = ord(row.objkey[0])
			eventlog.logger.mutate(row.site.name, evlog_error, evlog_size, evlog_type)

	def iterate_rows(self):
		""" Iterate through the stored objects (excluding cache
		    backups).

		    @rtype iterator(Row)
		"""
		for storekey, columns in self.connect().get_range(column_count=COLUMN_COUNT, retry_count=RETRY_COUNT * 10, retry_delay=RETRY_DELAY, log=log):
			if not storekey.startswith(INTERNAL_KEY_PREFIX):
				yield self._make_row(storekey, columns)

	def iterate_rows_dynamo(self):
		storekey = None
		items    = None

		for item in self.dynamo_table.scan():
			if item.hash_key.startswith(INTERNAL_KEY_PREFIX):
				continue

			if item.hash_key == storekey:
				items.append(item)
			else:
				if storekey is not None:
					yield self._make_row_dynamo(storekey, items)

				storekey = item.hash_key
				items    = [item]

		if storekey:
			yield self._make_row_dynamo(storekey, items)

	def insert_cache_backup(self, site, backup):
		""" @type site:   Site
		    @type backup: NewBackup
		"""
		storekey = CACHE_BACKUP_KEY_FORMAT % site
		data = backup.dumps()
		columns = { CACHE_BACKUP_COLUMN: data }

		evlog_error = eventlog.ERROR_CASSANDRA
		try:
			retry(lambda: self.connect().insert(storekey, columns))
			evlog_error = 0
		finally:
			eventlog.logger.cache_backup(site.name, evlog_error, len(data), False)

	def get_cache_backup(self, site):
		""" @type  site: Site
		    @rtype       BackupData | NoneType
		"""
		storekey = CACHE_BACKUP_KEY_FORMAT % site
		columns = [CACHE_BACKUP_COLUMN]

		try:
			result = retry(lambda: self.connect().get(storekey, columns, include_timestamp=True))
			return BackupData(*result[CACHE_BACKUP_COLUMN])
		except cassandratypes.NotFoundException:
			return None

	@staticmethod
	def make_key(site, objkey):
		""" @type  site:   Site
		    @type  objkey: str
		    @rtype         str
		"""
		return "_".join((site.name, objkey))

	@classmethod
	def make_dynamo_hashkey(cls, site, objkey):
		return "_".join((site.name + cls.get_dynamo_site_suffix(), objkey))

	@staticmethod
	def parse_key(storekey, site_suffix=None):
		""" @type  storekey: str
		    @rtype           Site, str
		"""
		sitename, objkey = storekey.split("_", 1)

		if site_suffix:
			assert sitename.endswith(site_suffix)
			sitename = sitename[:-len(site_suffix)]

		return Site(sitename), objkey

	def _make_row(self, storekey, columns):
		slots = { k: json.loads(v) for k, v in columns.iteritems() }
		return Row(storekey, slots, self)

	def _make_row_dynamo(self, storekey, items):
		slots = {}

		for item in items:
			values = {}

			for k, v in item.iteritems():
				if k not in (item._hash_key_name, item._range_key_name):
					values[k] = json.loads(v)

			slots[item.range_key] = values

		return Row(storekey, slots, self, self.get_dynamo_site_suffix())

class Row(object):
	""" A stored object's data.
	"""
	def __init__(self, storekey, slots, storage, site_suffix=None):
		self.site, self.objkey = storage.parse_key(storekey, site_suffix)
		self.storekey = storekey
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
