from __future__ import absolute_import

import datetime

from .config import conf

class Site(object):
	__instances = {}

	def __new__(cls, name):
		x = cls.__instances.get(name)
		if x is None:
			x = object.__new__(cls)
			cls.__init__(x, name)
			cls.__instances[name] = x
		return x

	def __init__(self, name):
		offset = conf.getint("site", name)

		self.name = name
		self.offset = datetime.timedelta(hours=offset)

	def __str__(self):
		return self.name

	def current_datetime(self):
		return datetime.datetime.today() + self.offset

	def current_date(self):
		return self.current_datetime().date()
