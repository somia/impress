from __future__ import absolute_import

import os
import sys
import time

from .config import log

def safe(func, args=(), kwargs={}, default=None, error=None):
	""" Call function and catch any exception.
	"""
	try:
		return func(*args, **kwargs)
	except:
		try:
			if error:
				log.exception(error)
		except:
			pass

		return default

class timing(object):
	""" Context manager for timing code block execution.
	"""
	def __enter__(self):
		self.start = time.time()
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.elapsed = time.time() - self.start

	def __str__(self):
		return str(self.elapsed)

	def __float__(self):
		return self.elapsed

	def __int__(self):
		return int(self.elapsed)

class Fork(object):
	""" Context manager for forking a child process which terminates at the
	    end of the context.
	"""
	def __init__(self):
		sys.stderr.flush()
		sys.stdout.flush()

		self.pid = os.fork()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.pid:
			return

		code = 1

		try:
			if exc_type or exc_val or exc_tb:
				if exc_type == SystemExit:
					code = exc_val.code
				else:
					exc_info = exc_type, exc_val, exc_tb
					log.error("error in child process", exc_info=exc_info)
			else:
				code = 0
		finally:
			try:
				sys.stderr.flush()
				sys.stdout.flush()
			except:
				pass

			os._exit(code)

	def __nonzero__(self):
		return self.pid == 0

	def join(self):
		assert self.pid > 0

		pid, status = os.waitpid(self.pid, 0)
		if status:
			raise self.Error(status)

	class Error(Exception):
		def __init__(self, status):
			super(Fork.Error, self).__init__(status)

class Enum(object):

	def __init__(self, **kwargs):
		for key, value in kwargs.iteritems():
			setattr(self, key, value)

		self.__values = set(kwargs.values())

	def __contains__(self, value):
		return value in self.__values

Nonexistent = object()

def dict_get_default(dict, key, default_factory):
	value = dict.get(key, Nonexistent)
	if value is Nonexistent:
		value = default_factory()
		dict[key] = value
	return value
