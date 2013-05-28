""" Model interface descriptions for documentation purposes and utility base
    classes.
"""

class CacheModel(object):
	""" Daily cache accumulation logic.
	"""

	def __init__(self, items=None):
		""" @type items: dict | None
		"""

	def upgrade(self):
		pass

	def add(self, params, time):
		""" @type params: dict | list
		    @type time:   datetime.time
		"""

class TimelineModel(object):
	""" Time slot merging logic.
	"""

	@staticmethod
	def prepare(models):
		""" @type models: list(ModelSlot)
		"""

	def __init__(self, items=None):
		""" @type items: dict | None
		"""

	def merge(self, slot, other_slot):
		""" @type slot:       ModelSlot
		    @type other_slot: ModelSlot
		"""

	def update(self, slot):
		""" @type  slot: ModelSlot
		    @rtype bool
		"""
		return False

class AbstractMixin(object):

	def __init__(self, items=None):
		""" @type items: dict
		"""
		self.items = items or {}

	def get(self):
		""" @rtype dict
		"""
		return self.items

class AbstractCacheModel(AbstractMixin, CacheModel):

	def upgrade(self):
		try:
			self.items
		except AttributeError:
			self.items = self.values
			del self.values

class AbstractTimelineModel(AbstractMixin, TimelineModel):
	pass
