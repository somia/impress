from __future__ import absolute_import

class Interval(object):

	def __init__(self, start, delta=None):
		""" @type start: datetime.datetime
		    @type delta: datetime.timedelta
		"""
		self.start = start
		self.delta = delta or self.basic_delta
		self.end = self.start + self.delta
		self.key = self.make_key(self.start, self.delta)

	def __str__(self):
		return self.key

	def __eq__(self, other):
		return self.start == other.start and self.delta == other.delta

	def __ne__(self, other):
		return self.start != other.start or self.delta != other.delta

	def __lt__(self, other):
		if self.start < other.start:
			return True

		if self.start > other.start:
			return False

		return self.delta > other.delta

	def __gt__(self, other):
		if self.start > other.start:
			return True

		if self.start < other.start:
			return False

		return self.delta < other.delta

	def __le__(self, other):
		if self.start < other.start:
			return True

		if self.start > other.start:
			return False

		return self.delta >= other.delta

	def __ge__(self, other):
		if self.start > other.start:
			return True

		if self.start < other.start:
			return False

		return self.delta <= other.delta
