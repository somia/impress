from __future__ import absolute_import

import datetime

from .. import interval as interface

class Interval(interface.Interval):
	basic_delta = datetime.timedelta(days=1)

	@classmethod
	def make_key(cls, start, delta):
		key = start.strftime("%Y%m%d")
		if delta != cls.basic_delta:
			key += "_%d" % delta.days
		return key

	@classmethod
	def parse(cls, key):
		if "_" in key:
			startstr, deltastr = key.split("_", 1)

			days = int(deltastr)
			delta = datetime.timedelta(days=days)
		else:
			startstr = key
			delta = cls.basic_delta

		year  = int(startstr[0:4])
		month = int(startstr[4:6])
		day   = int(startstr[6:8])
		start = datetime.datetime(year, month, day)

		return cls(start, delta)
