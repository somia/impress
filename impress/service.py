from __future__ import absolute_import

from . import eventlog
from . import util
from .cache import Cache
from .config import argument_parser, configure, log, reconfigure
from .registry import Registry

class Service(object):

	def __init__(self, lock_type):
		self.registry = Registry()
		self.cache = Cache(lock_type)

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.cache.flush()

	def add(self, site, objkeys, data):
		""" @type site:    str
		    @type objkeys: list(str)
		    @type data:    str
		"""
		evlog_error = eventlog.ERROR_OTHER
		evlog_size = 0
		evlog_count = 0

		try:
			evlog_size = len(data)
			evlog_count = len(objkeys)

			model = self.registry.get_common_model(objkeys)
			self.cache.add(site, objkeys, data, model)

			evlog_error = 0
		finally:
			eventlog.logger.add(site, evlog_error, evlog_size, evlog_count)

	def get(self, site, objkeys):
		""" @type  site:    str
		    @type  objkeys: list(str)
		    @rtype          str
		"""
		evlog_error = eventlog.ERROR_OTHER
		evlog_size = 0
		evlog_count = 0

		try:
			evlog_count = len(objkeys)

			data = self.cache.get(site, objkeys)

			evlog_size = len(data)
			evlog_error = 0
		finally:
			# XXX: quick hack
			if evlog_count > 255:
				evlog_count = 255

			eventlog.logger.get(site, evlog_error, evlog_size, evlog_count)

		return data

	def reconfigure(self):
		util.safe(reconfigure, error="reconfiguration failed")
		util.safe(self.registry.reconfigure, error="registry reconfiguration failed")

	def flush(self, *args, **kwargs):
		self.cache.flush(*args, **kwargs)

class Main(object):

	def __init__(self, args, lock_type):
		parser = argument_parser()
		parser.parse_args(args)

		configure("service")

		self.service = Service(lock_type)

	def __enter__(self):
		return self.service.__enter__()

	def __exit__(self, *exc):
		self.service.__exit__(*exc)

		log.info("exit")
