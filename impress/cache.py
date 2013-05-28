""" In-memory cache for accumulating data on a daily basis and storing it to
    external storage.
"""
from __future__ import absolute_import

import copy
import gc
import os
import sys
import threading
import time

from . import eventlog
from . import json
from . import progress
from . import util
from .backup import BackupFile, NewBackup
from .config import conf, log
from .site import Site
from .timeline import Slot

class Day(Slot):
	""" The objects' data of a given day.  The accumulation logic and
	    internal representation is specified per object with a custom model
	    module.
	"""
	def __init__(self, date, cachedata=None):
		Slot.__init__(self, date)
		self.cachedata = cachedata or {}

	def __nonzero__(self):
		return bool(self.cachedata)

	def clone(self):
		return Day(self.date, copy.deepcopy(self.cachedata))

	def is_active(self, current_date):
		""" Compares the day against the given date (presumably today).

		    @type  current_date: datetime.date
		    @rtype               bool
		"""
		return current_date <= self.date

	def add(self, objkeys, params, model, time):
		""" @type objkeys:  list(str)
		    @type params:   list | dict
		    @type model:    module
		    @type time:     datetime.time
		"""
		for objkey in objkeys:
			modeldata = self.cachedata.get(objkey)

			if modeldata is None:
				modeldata = model.CacheModel()
				self.cachedata[objkey] = modeldata
			else:
				assert isinstance(modeldata, model.CacheModel)

			modeldata.add(params, time)

	def get(self, objkeys, callback):
		""" @type objkeys:  list(str)
		    @type callback: callable(slotkey:str, objkey:str, values:dict)
		"""
		for objkey in objkeys:
			modeldata = self.cachedata.get(objkey)
			if modeldata:
				callback(self.key, objkey, modeldata.get())

	def store(self, site, storage):
		length = len(self.cachedata)
		errors = 0

		iterator = self.cachedata.iteritems()

		if progress.is_enabled():
			prefix = "storing site %s cache %s keys: " % (site, self.key)
			counter = progress.Counter(total=length, interval=1000, prefix=prefix)
			iterator = counter(iterator)
		else:
			log.debug("storing site %s cache %s with %d keys", site, self.key, length)

		for objkey, modeldata in iterator:
			try:
				storage.insert(site, objkey, self.key, modeldata.get())
			except:
				log.exception("object %s_%s slot %s insert failed", site, objkey, self.key)
				errors += 1

		if errors:
			log.error("failed to store %d/%d keys of site %s cache %s", errors, length, site, self.key)
		elif not progress.is_enabled():
			log.info("stored site %s cache %s", site, self.key)

		return errors == 0

	def store_dynamo(self, site, storage):
		length = len(self.cachedata)
		errors = 0

		log.debug("DynamoDB: storing site %s cache %s with %d keys", site, self.key, length)

		start_time = time.time()

		for objkey, modeldata in self.cachedata.iteritems():
			try:
				storage.insert_dynamo(site, objkey, self.key, modeldata.get())
			except:
				log.exception("DynamoDB: object %s_%s slot %s insert failed", site, objkey, self.key)
				errors += 1

		rate = length / (time.time() - start_time)

		if errors:
			log.error("DynamoDB: failed to store %d/%d keys of site %s cache %s (%f items per second)", errors, length, site, self.key, rate)
		else:
			log.info("DynamoDB: stored site %s cache %s (%f items per second)", site, self.key, rate)

	backup_version = 1

	@classmethod
	def load_backup(cls, backup):
		values = backup.load()

		version = values["version"]
		if version != cls.backup_version:
			raise Exception("unsupported cache backup version: " + version)

		date = values["date"]
		cachedata = values["cachedata"]

		for modeldata in cachedata.itervalues():
			modeldata.upgrade()

		return cls(date, cachedata)

	def make_backup(self):
		values = {
			"version": self.backup_version,
			"date": self.date,
			"cachedata": self.cachedata,
		}

		return NewBackup(values)

class Active(object):
	""" Maintains the daily cache.
	"""
	def __init__(self, site, storage):
		self.site = site
		self.local_backup_name = check_dirname(conf.get("backup", "local_cache_format").format(site=site))
		self.lock = threading.Lock()
		self.day = self.load_backup(storage)
		self.modified = False

	def add(self, objkeys, params, model):
		""" Accumulate objects' data.  Return the previous day's data
		    if the date has changed.

		    @type  objkeys: list(str)
		    @type  data:    str
		    @type  model:   module
		    @rtype          Day | NoneType
		"""
		with self.lock:
			now = self.site.current_datetime()

			yesterday = self.__rotate(now.date())

			self.day.add(objkeys, params, model, now.time())
			self.modified = True

		return yesterday

	def get(self, objkeys, callback):
		""" @type objkeys:  list(str)
		    @type callback: callable(slotkey:str, objkey:str, values:dict)
		"""
		with self.lock:
			self.day.get(objkeys, callback)

	def rotate(self, force=False):
		""" Return the previous day's data if the date has changed.

		    @rtype Day | NoneType
		"""
		with self.lock:
			return self.__rotate(self.site.current_date(), force)

	def __rotate(self, date, force=False):
		active    = self.day.is_active(date)
		yesterday = None

		if not active or force:
			yesterday = self.day

			if active:
				log.debug("cloning active site %s cache %s", self.site, yesterday)
				self.day = self.day.clone()
			else:
				self.day = Day(date)
				self.modified = True

			log.debug("rotating site %s cache %s", self.site, yesterday)

		return yesterday

	def load_backup(self, storage):
		log.debug("loading site %s cache backup", self.site)

		with util.timing() as loadtime:
			stored = storage.get_cache_backup(self.site)
			if not stored:
				log.warning("site %s cache backup not found from cassandra", self.site)

			local = self.open_local_backup()
			if local:
				log.warning("site %s local cache backup file found", self.site)

			if stored:
				if local:
					if stored.time < local.time:
						log.debug("local cache backup file is newest")
						del stored
						choice = local
					else:
						log.warning("cache backup in cassandra is newest")
						choice = stored
				else:
					choice = stored
			else:
				if local:
					choice = local
				else:
					choice = None

			if not choice:
				return Day(self.site.current_date())

			day = Day.load_backup(choice)

		log.info("site %s cache backup load time %d s", self.site, int(loadtime))

		return day

	def dump_backup(self, storage):
		with self.lock:
			if not self.modified:
				log.debug("site %s cache not modified since last dump", self.site)
				return

		log.debug("dumping site %s cache backup", self.site)

		ok = False

		try:
			with util.timing() as dumptime:
				storage.close()

				with self.lock:
					with util.Fork() as child:
						if child:
							gc.disable()

							backup = self.day.make_backup()
							try:
								storage.insert_cache_backup(self.site, backup)
								result = 0
							except:
								self.dump_local_backup(backup)
								raise

					self.modified = False

				try:
					child.join()
					ok = True
					util.safe(os.unlink, (self.local_backup_name,))
				except child.Error:
					log.error("site %s cache backup process failed", self.site)
		except:
			log.exception("site %s cache backup failed", self.site)

		if ok:
			log.info("site %s cache backup dump time %d s", self.site, int(dumptime))
		else:
			# undo state change
			with self.lock:
				self.modified = True

	def open_local_backup(self):
		if os.path.exists(self.local_backup_name):
			return BackupFile(self.local_backup_name)
		else:
			return None

	def dump_local_backup(self, backup):
		tempname = self.local_backup_name + ".tmp"

		evlog_error = eventlog.ERROR_OTHER
		try:
			with open(tempname, "w") as file:
				backup.dump(file)
		except:
			log.exception("local cache backup dumping failed: %s", tempname)
			util.safe(os.unlink, (tempname,))
		else:
			os.rename(tempname, self.local_backup_name)
			log.info("local cache backup: %s", self.local_backup_name)

			evlog_error = 0
		finally:
			eventlog.logger.cache_backup(self.site.name, evlog_error, 0, True)

class History(object):
	""" Holds previously active days until they are stored to Storage.
	"""
	def __init__(self, site):
		self.site = site
		self.local_backup_format = check_dirname(conf.get("backup", "local_history_format"))
		self.lock = threading.Lock()
		self.days = []

	def append(self, day):
		with self.lock:
			self.days.append(day)

	def get(self, objkeys, callback):
		""" @type objkeys:  list(str)
		    @type callback: callable(slotkey:str, objkey:str, values:dict)
		"""
		with self.lock:
			for day in self.days:
				day.get(objkeys, callback)

	def store(self, storage):
		storage.close()

		with self.lock:
			if not self.days:
				return

			with util.Fork() as child:
				if child:
					gc.set_debug(0)

					for day in self.days:
						if not day.store(self.site, storage):
							util.safe(self.dump_local_backup, (day,))

					try:
						if self.site.name.endswith("_dynamo") and storage.dynamo_table:
							for day in self.days:
								day.store_dynamo(self.site, storage)
					except:
						log.exception("DynamoDB store failed")

			count = len(self.days)

		child.join()

		with self.lock:
			del self.days[:count]

	def dump_local_backup(self, day):
		backup = day.make_backup()

		filename = self.local_backup_format.format(site=self.site, slot=day)
		partname = filename + ".partial"

		evlog_error = eventlog.ERROR_OTHER
		evlog_path = ""
		try:
			with open(partname, "w") as file:
				evlog_path = partname
				backup.dump(file)
		except:
			log.exception("local history backup dumping failed: %s", partname)
		else:
			os.rename(partname, filename)
			log.info("local history backup: %s", filename)

			evlog_error = 0
			evlog_path = filename
		finally:
			eventlog.logger.store_local_backup(self.site.name, evlog_error, evlog_path)

class SiteCache(object):
	""" Manages active cache and cache history per Site.
	"""
	def __init__(self, sitename, storage):
		site = Site(sitename)

		self.active = Active(site, storage)
		self.history = History(site)

	def add(self, objkeys, data, model):
		""" Accumulate objects's data in active cache.  The active
		    cache is rotated if necessary.

		    @type objkeys: list(str)
		    @type data:    str
		    @type model:   module
		"""
		params = json.loads(data)

		yesterday = self.active.add(objkeys, params, model)
		if yesterday:
			self.history.append(yesterday)

	def get(self, objkeys):
		""" Get objects' data from active cache and cache history.

		    @type  objkeys: list(str)
		    @rtype          str
		"""
		slots = {}

		def callback(slotkey, objkey, values):
			""" @type slotkey: str
			    @type objkey:  str
			    @type values:  dict
			"""
			objects = slots.get(slotkey)
			if objects is None:
				objects = []
				slots[slotkey] = objects

			objects.append(json.dumps(objkey) + ":" + json.dumps(values))

		self.history.get(objkeys, callback)
		self.active.get(objkeys, callback)

		json_slots = (json.dumps(slotkey) + ":{" + ",".join(objects) + "}" for slotkey, objects in slots.iteritems())

		return "{" + ",".join(json_slots) + "}"

	def flush(self, storage, force_rotate=False):
		""" Rotates active cache (if necessary), stores cache history
		    and backups active cache.
		"""
		yesterday = self.active.rotate(force_rotate)
		if yesterday:
			self.history.append(yesterday)

		util.safe(self.history.store, (storage,), error="history storing failed")
		util.safe(self.active.dump_backup, (storage,), error="backup dumping failed")

class Cache(object):
	""" Groups all known SiteCaches.
	"""
	def __init__(self, storage):
		self.sitecaches = { name: SiteCache(name, storage) for name in conf.options("site") }

	def add(self, sitename, objkeys, data, model):
		""" @type sitename: str
		    @type objkeys:  list(str)
		    @type data:     str
		    @type model:    module
		"""
		self.sitecaches[sitename].add(objkeys, data, model)

	def get(self, sitename, objkeys):
		""" @type  sitename: str
		    @type  objkeys:  list(str)
		    @rtype           str
		"""
		return self.sitecaches[sitename].get(objkeys)

	def flush(self, storage, force_rotate=False):
		for sitecache in self.sitecaches.itervalues():
			sitecache.flush(storage, force_rotate)

def check_dirname(path):
	""" Creates all directories in a filename path if they don't exist.
	"""
	dirpath = os.path.dirname(path)
	if os.path.isdir(dirpath):
		return path
	else:
		raise Exception("no such directory: " + dirpath)
