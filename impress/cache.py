""" In-memory cache for accumulating data on a daily basis and storing it to
    external storage.
"""
from __future__ import absolute_import

import copy
import datetime
import gc
import os
import sys
import time

from . import eventlog
from . import json
from . import progress
from . import util
from .backup import BackupFile, NewBackup
from .config import conf, log
from .registry import interval_type
from .site import Site
from .storage import Storage

class Slot(object):
	""" The objects' data of a given interval.  The accumulation logic and
	    internal representation is specified per object with a custom model
	    module.
	"""
	def __init__(self, interval, downtime=None, cachedata=None, add_downtime=None):
		self.interval = interval
		self.downtime = downtime
		self.cachedata = cachedata or {}

		# wrap callable in a tuple to avoid Python thinking it's a bound method
		self.__add_downtime = (add_downtime,)

	def __str__(self):
		return self.key

	def __nonzero__(self):
		return bool(self.cachedata)

	def init(self, site, now):
		""" @type site: Site
		    @type now:  datetime.datetime
		"""
		func, = self.__add_downtime
		if func:
			delta = func(site, now)
			if delta and delta > datetime.timedelta():
				self.downtime += delta

	@property
	def key(self):
		return self.interval.key

	def clone(self):
		return type(self)(self.interval, self.downtime, copy.deepcopy(self.cachedata))

	def is_active(self, now):
		""" Compares the interval against the given time.

		    @type  now: datetime.datetime
		    @rtype      bool
		"""
		return now >= self.interval.start and now < self.interval.end

	def add(self, objkeys, params, model, now):
		""" @type objkeys:  list(str)
		    @type params:   list | dict
		    @type model:    module
		    @type now:      datetime.datetime
		"""
		for objkey in objkeys:
			modeldata = self.cachedata.get(objkey)

			if modeldata is None:
				modeldata = model.CacheModel()
				self.cachedata[objkey] = modeldata
			else:
				assert isinstance(modeldata, model.CacheModel)

			modeldata.add(params, now - self.interval.start)

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

		log.debug("storing site %s cache %s with %d keys", site, self.key, length)

		start_time = time.time()

		for objkey, modeldata in self.cachedata.iteritems():
			try:
				storage.insert(objkey, self.key, modeldata.get())
			except:
				log.exception("site %s object %s slot %s insert failed", site, objkey, self.key)
				errors += 1

		rate = length / (time.time() - start_time)
		ok = (errors == 0)

		for i in xrange(10):
			try:
				storage.insert_avail_marker(self.key, length - errors, errors, self.downtime)
				break
			except:
				if i < 9:
					log.debug("avail marker", exc_info=True)
					time.sleep(1)
				else:
					log.exception("failed to store site %s cache %s avail marker", site, self.key)
					ok = False

		if self.downtime is None:
			downstr = "without downtime"
		else:
			downstr = "with %s downtime" % self.downtime

		if errors:
			log.error("failed to store %d/%d keys of site %s cache %s %s (%f items per second)", errors, length, site, self.key, downstr, rate)
		else:
			log.info("stored site %s cache %s %s (%f items per second)", site, self.key, downstr, rate)

		return ok

	backup_version = 3
	supported_backup_versions = 1, 2, 3

	@classmethod
	def load_backup(cls, backup):
		values = backup.load()

		version = values["version"]
		if version not in cls.supported_backup_versions:
			raise Exception("unsupported cache backup version: " + version)

		if "interval_start" not in values:
			date = values["date"]
			interval = interval_type(datetime.datetime(date.year, date.month, date.day))
		else:
			interval = interval_type(values["interval_start"])

		cachedata = values["cachedata"]

		for modeldata in cachedata.itervalues():
			modeldata.upgrade()

		downtime = values.get("downtime", datetime.timedelta())
		snapshot_end = values.get("snapshot_end")

		if snapshot_end is not None:
			def add_downtime(site, now):
				delta = min(now, interval.end - site.offset) - snapshot_end
				log.info("site %s cache backup has staled for %s", site, delta)
				return delta
		else:
			add_downtime = None

		return cls(interval, downtime, cachedata, add_downtime)

	def make_backup(self, snapshot_end):
		values = {
			"version": self.backup_version,
			"interval_start": self.interval.start,
			"cachedata": self.cachedata,
			"downtime": self.downtime or datetime.timedelta(),
			"snapshot_end": snapshot_end,
		}

		return NewBackup(values)

class Active(object):
	""" Maintains the current cache.
	"""
	def __init__(self, lock_type, site, storage):
		self.site = site
		self.local_backup_name = check_dirname(conf.get("backup", "local_cache_format").format(site=site))
		self.lock = lock_type()
		self.slot = self.load_backup(storage)
		self.modified = False

	def init(self, now):
		""" @type now: datetime.datetime
		"""
		self.slot.init(self.site, now)

		log.info("site %s cache %s initialized with %s downtime", self.site, self.slot, self.slot.downtime)

	def add(self, objkeys, params, model):
		""" Accumulate objects' data.  Return the previous slot if the interval
		    has changed.

		    @type  objkeys: list(str)
		    @type  data:    str
		    @type  model:   module
		    @rtype          Slot | NoneType
		"""
		with self.lock:
			now = self.site.current_datetime()

			rotated_slot = self.__rotate(now)

			self.slot.add(objkeys, params, model, now)
			self.modified = True

		return rotated_slot

	def get(self, objkeys, callback):
		""" @type objkeys:  list(str)
		    @type callback: callable(slotkey:str, objkey:str, values:dict)
		"""
		with self.lock:
			self.slot.get(objkeys, callback)

	def rotate(self, force=False):
		""" Return the previous slot if the interval has changed.

		    @rtype Slot | NoneType
		"""
		with self.lock:
			return self.__rotate(self.site.current_datetime(), force)

	def __rotate(self, now, force=False):
		active = self.slot.is_active(now)
		rotated_slot = None

		if not active or force:
			rotated_slot = self.slot

			if active:
				log.debug("cloning active site %s cache %s", self.site, rotated_slot)
				self.slot = self.slot.clone()
			else:
				self.slot = Slot(interval_type(now))
				self.modified = True

			log.debug("rotating site %s cache %s", self.site, rotated_slot)

		return rotated_slot

	def load_backup(self, storage):
		log.debug("loading site %s cache backup", self.site)

		with util.timing() as loadtime:
			stored = storage.get_cache_backup()
			if not stored:
				log.warning("site %s cache backup not found from dynamodb", self.site)

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
						log.warning("cache backup in dynamodb is newest")
						choice = stored
				else:
					choice = stored
			else:
				if local:
					choice = local
				else:
					choice = None

			if not choice:
				return self.__load_empty()

			slot = Slot.load_backup(choice)

		log.info("site %s cache backup load time %d s", self.site, int(loadtime))

		return slot

	def __load_empty(self):
		inteval = interval_type(self.site.current_datetime())

		def add_downtime(site, now):
			site_now = now + site.offset
			if site_now < interval.end:
				return site_now - interval.start
			else:
				return interval.delta

		return Slot(interval, datetime.timedelta(), {}, add_downtime)

	def dump_backup(self, storage, force):
		if not force:
			with self.lock:
				if not self.modified:
					log.debug("site %s cache not modified since last dump", self.site)
					return

		log.debug("dumping site %s cache backup", self.site)

		ok = False

		try:
			with util.timing() as dumptime:
				storage.reset()

				with self.lock:
					snapshot_end = datetime.datetime.today()

					with util.Fork() as child:
						if child:
							gc.disable()

							backup = self.slot.make_backup(snapshot_end)
							try:
								storage.insert_cache_backup(backup)
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
	""" Holds previously active slots until they are stored to Storage.
	"""
	def __init__(self, lock_type, site):
		self.site = site
		self.local_backup_format = check_dirname(conf.get("backup", "local_history_format"))
		self.lock = lock_type()
		self.slots = []

	def append(self, slot):
		with self.lock:
			self.slots.append(slot)

	def get(self, objkeys, callback):
		""" @type objkeys:  list(str)
		    @type callback: callable(slotkey:str, objkey:str, values:dict)
		"""
		with self.lock:
			for slot in self.slots:
				slot.get(objkeys, callback)

	def store(self, storage):
		storage.reset()

		with self.lock:
			if not self.slots:
				return

			with util.Fork() as child:
				if child:
					gc.set_debug(0)

					for slot in self.slots:
						if not slot.store(self.site, storage):
							util.safe(self.dump_local_backup, (slot,))

			count = len(self.slots)

		child.join()

		with self.lock:
			del self.slots[:count]

	def dump_local_backup(self, slot):
		backup = slot.make_backup(slot.interval.end)

		filename = self.local_backup_format.format(site=self.site, slot=slot)
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
	def __init__(self, lock_type, sitename):
		site = Site(sitename)
		self.storage = Storage(site)

		self.active = Active(lock_type, site, self.storage)
		self.history = History(lock_type, site)

	def init(self, now):
		""" @type now: datetime.datetime
		"""
		self.active.init(now)

	def add(self, objkeys, data, model):
		""" Accumulate objects's data in active cache.  The active
		    cache is rotated if necessary.

		    @type objkeys: list(str)
		    @type data:    str
		    @type model:   module
		"""
		params = json.loads(data)

		rotated_slot = self.active.add(objkeys, params, model)
		if rotated_slot:
			self.history.append(rotated_slot)

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

	def flush(self, force_rotate=False, force_backup=False):
		""" Rotates active cache (if necessary), stores cache history
		    and backups active cache.
		"""
		rotated_slot = self.active.rotate(force_rotate)
		if rotated_slot:
			self.history.append(rotated_slot)

		util.safe(self.history.store, (self.storage,), error="history storing failed")
		util.safe(self.active.dump_backup, (self.storage, force_backup), error="backup dumping failed")

class Cache(object):
	""" Groups all known SiteCaches.
	"""
	def __init__(self, lock_type):
		self.sitecaches = { name: SiteCache(lock_type, name) for name in conf.options("site") }

	def init(self):
		now = datetime.datetime.today()

		for sitecache in self.sitecaches.itervalues():
			sitecache.init(now)

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

	def flush(self, *args, **kwargs):
		for sitecache in self.sitecaches.itervalues():
			sitecache.flush(*args, **kwargs)

def check_dirname(path):
	""" Creates all directories in a filename path if they don't exist.
	"""
	dirpath = os.path.dirname(path)
	if os.path.isdir(dirpath):
		return path
	else:
		raise Exception("no such directory: " + dirpath)
