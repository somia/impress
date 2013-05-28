""" General-purpose model with arbitrary items that may be incremented.
"""

from __future__ import absolute_import

from .. import model as interface

class CacheModel(interface.AbstractCacheModel):

	def add(self, params, time):
		""" @type params: dict
		    @type time:   datetime.time
		"""
		for itemkey, delta in params.iteritems():
			self.items[itemkey] = self.items.get(itemkey, 0) + delta

class TimelineModel(interface.AbstractTimelineModel):

	def merge(self, slot, other_slot):
		""" @type slot:       ModelSlot
		    @type other_slot: ModelSlot
		"""
		for itemkey, other_value in other_slot.modeldata.items.iteritems():
			self.items[itemkey] = self.items.get(itemkey, 0) + other_value
