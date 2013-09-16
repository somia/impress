from __future__ import absolute_import

import datetime

from .. import interval as interface

class Interval(interface.Interval):
	basic_delta = datetime.timedelta(seconds=3600)

	@classmethod
	def make_key(cls, start, delta):
		key = start.strftime("%Y%m%d%H")
		if delta != cls.basic_delta:
			key += "_%d" % (delta.seconds // 3600)
		return key

	@classmethod
	def parse(cls, key):
		if "_" in key:
			startstr, deltastr = key.split("_", 1)

			hours = int(deltastr)
			delta = datetime.timedelta(seconds=hours * 3600)
		else:
			startstr = key
			delta = cls.basic_delta

		year  = int(startstr[0:4])
		month = int(startstr[4:6])
		day   = int(startstr[6:8])
		hour  = int(startstr[8:10])
		start = datetime.datetime(year, month, day, hour)

		return cls(start, delta)
