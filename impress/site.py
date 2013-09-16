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
		self.name = name
		config = conf.get("site", name).strip().split()

		if len(config) == 1:
			self.dynamodb_table_name, = config
			self.offset = datetime.timedelta()
		else:
			self.dynamodb_table_name, offsetconfig = config
			self.offset = datetime.timedelta(hours=int(offsetconfig))

	def __str__(self):
		return self.name

	def current_datetime(self):
		return datetime.datetime.today() + self.offset
