""" Event logging interface.
"""

ERROR_OTHER = 1
ERROR_CASSANDRA = 4
ERROR_NETWORK = 5

class NullLogger(object):

	def add(self, site, error, size, count):
		pass

	def get(self, site, error, size, count):
		pass

	def store(self, site, error, size, type):
		pass

	def mutate(self, site, error, size, type):
		pass

	def cache_backup(self, site, error, size, local):
		pass

	def store_local_backup(self, site, error, path):
		pass

	def thrift_error(self, error):
		pass

logger = NullLogger()