""" Merges days into months when they contain enough data.
"""
from __future__ import absolute_import

import datetime

from .. import pattern as interface

class TimelinePattern(interface.TimelinePattern):

	@staticmethod
	def merge(timeline):
		""" @type timeline: Timeline
		"""
		today = timeline.site.current_date()
		month = previous_month(today)
		begin = previous_month(month)  # skip last month - it might have just ended

		for date in reverse_month_range(begin, timeline.start()):
			delta = month_length(date)

			timeline.merge(date, delta)
			month = date

			date = previous_month(month)
			delta = month - date

def reverse_month_range(later_date, earlier_date):
	""" @type  later_date:   datetime.date
	    @type  earlier_date: datetime.date
	    @rtype               iterator(datetime.date)
	"""
	earlier_month = month_start(earlier_date)
	m = month_start(later_date)

	while m >= earlier_month:
		yield m
		m = previous_month(m)

def month_start(date):
	""" @type  date: datetime.date
	    @rtype       datetime.date
	"""
	return date.replace(day=1)

def month_length(date):
	""" @type  date: datetime.date
	    @rtype       datetime.timedelta
	"""
	return next_month(date) - date

def next_month(date, step=45):
	""" @type  date: datetime.date
	    @type  step: int
	    @rtype       datetime.date
	"""
	date = datetime.date(date.year, date.month, 1)
	date += datetime.timedelta(step)
	return month_start(date)

def previous_month(date):
	""" @type  date: datetime.date
	    @rtype       datetime.date
	"""
	return next_month(date, -15)
