from __future__ import absolute_import

from bisect import bisect_left
import json
import sys

from .config import log
from .registry import interval_type

class ModelSlot(object):

	def __init__(self, interval, model, items=None):
		""" @type interval: Interval
		    @type model:    module
		    @type items:    dict | None
		"""
		self.interval = interval
		self.modeldata = model.TimelineModel(items)

	def __str__(self):
		return str(self.interval)

	def __eq__(self, other):
		return self.interval == other.interval

	def __ne__(self, other):
		return self.interval != other.interval

	def __lt__(self, other):
		return self.interval < other.interval

	def __gt__(self, other):
		return self.interval > other.interval

	def __le__(self, other):
		return self.interval <= other.interval

	def __ge__(self, other):
		return self.interval >= other.interval

	def overlaps(self, other):
		""" @type  second: Interval
		    @rtype         bool
		"""
		return self.interval.end > other.interval.start

	def contains(self, other):
		""" @type  second: Interval
		    @rtype         bool
		"""
		return self.interval.end >= other.interval.end

	def merge(self, other):
		""" @type other: ModelSlot
		"""
		self.modeldata.merge(self, other)

	def update(self):
		""" @rtype bool
		"""
		return self.modeldata.update(self)

	def get(self):
		""" @rtype dict
		"""
		return self.modeldata.get()

class Timeline(object):

	def __init__(self, site, objkey, model):
		""" @type site:   Site
		    @type objkey: str
		    @type model:  module
		"""
		self.site = site
		self.objkey = objkey
		self.model = model
		self.slots = []
		self.updated = []
		self.removed = []

	def __nonzero__(self):
		return bool(self.slots)

	def warning(self, format, *args):
		log.warning("site %s key %s: " + format, self.site, self.objkey, *args)

	def modified(self):
		""" @rtype bool
		"""
		return bool(self.updated) or bool(self.removed)

	def add(self, key, items):
		""" @type key:   str
		    @type items: dict | list
		"""
		slot = ModelSlot(interval_type.parse(key), self.model, items)
		i = bisect_left(self.slots, slot)

		if i < len(self.slots) and self.slots[i] == slot:
			self.error("duplicate slot %s", slot)
			assert False
			return

		if i > 0:
			left = self.slots[i - 1]

			if left.overlaps(slot):
				if left.contains(slot):
					self.warning("slot %s contained in %s", slot, left)
					# they will be merged
				else:
					self.error("slot %s overlaps with %s", slot, left)
					assert False

		if i + 1 < len(self.slots):
			right = self.slots[i]

			if slot.overlaps(right):
				if slot.contains(right):
					self.warning("slot %s contains %s", slot, right)
					# they will be merged
				else:
					self.error("slot %s overlaps with %s", slot, right)
					assert False

		self.slots.insert(i, slot)

	def prepare(self):
		self.model.TimelineModel.prepare(self.slots)

	def start(self):
		""" @rtype datetime.datetime
		"""
		return self.slots[0].start

	def merge(self, start, delta):
		""" @type start: datetime.datetime
		    @type delta: datetime.timedelta
		"""
		slot = ModelSlot(interval_type(start, delta), self.model)

		merged = []

		i = bisect_left(self.slots, slot)

		if i > 0:
			left = self.slots[i - 1]

			if left.overlaps(slot):
				if left.contains(slot):
					self.warning("tried to create slot %s which is subset of %s", slot, left)
				else:
					# TODO: adjust date and delta and try again?
					self.warning("tried to create slot %s overlapping %s", slot, left)

				# don't touch anything
				return

		for n in xrange(len(self.slots) - i):
			right = self.slots[i + n]

			if not slot.contains(right):
				if slot.overlaps(right):
					# TODO: adjust delta and break?
					self.warning("tried to create slot %s overlapping %s", slot, right)
					return

				# out of reach
				break

			merged.append(right)

		if merged:
			if len(merged) == 1:
				# merging doesn't make sense
				return

			for s in merged:
				slot.merge(s)

			j = i + len(merged)
			removed = self.slots[i:j]
			self.slots[i:j] = [slot]

			if slot in removed:
				self.warning("updating slot %s", slot)
				removed.remove(slot)
				assert removed

			self.updated.append(slot)
			self.removed.extend(removed)

	def update(self):
		for slot in self.slots:
			if slot.update():
				if slot.get():
					if slot not in self.updated:
						self.updated.append(slot)
				else:
					self.removed.append(slot)

def merge(row, model, pattern, store, dump):
	""" @type  row:     Row
	    @type  model:   module
	    @type  pattern: module
	    @type  store:   bool
	    @type  dump:    bool
	    @rtype          bool
	"""
	timeline = Timeline(row.site, row.objkey, model)

	for key, items in row:
		timeline.add(key, items.copy())

	if timeline:
		timeline.prepare()

		pattern.TimelinePattern.merge(timeline)

		timeline.update()

		if timeline.modified():
			if dump:
				dump_mutation(row, timeline, sys.stdout)

			mutate(row, timeline, store)
			return True

	return False

def mutate(row, timeline, store):
	""" @type row:      Row
	    @type timeline: Timeline
	    @type store:    bool
	"""
	insert = {}
	remove = []

	for slot in timeline.updated:
		insert[slot.key] = slot.get()

	for slot in timeline.removed:
		remove.append(slot.key)

	if store:
		row.mutate(insert, remove)

def dump_mutation(row, timeline, file):
	""" @type row:      Row
	    @type timeline: Timeline
	    @type file:     file
	"""
	print >>file, "Key:", row.storekey

	updated = sorted((slot.key, slot.get(), row.slots[slot.key]) for slot in timeline.updated if slot.key in row.slots)
	inserted = sorted((slot.key, slot.get(), None) for slot in timeline.updated if slot.key not in row.slots)
	removed = sorted((slot.key, None, row.slots[slot.key]) for slot in timeline.removed)
	changed_keys = set(x[0] for x in updated + inserted + removed)
	unchanged = sorted((key, None, data) for key, data in row.slots.iteritems() if key not in changed_keys)

	for title, data in (("Updated", updated),
	                    ("Inserted", inserted),
	                    ("Removed", removed),
	                    ("Unchanged", unchanged)):
		if data:
			print >>file, "%s:" % title
			for key, new, old in data:
				print >>file, "  Slot: %s" % key
				for subtitle, values in (("Old", old), ("New", new)):
					if values is not None:
						print >>file, "    %s:" % subtitle
						for k, v in sorted(values.iteritems()):
							print >>file, "      %s: %s" % (k, json.dumps(v))

	print >>file
	file.flush()
