""" Model and pattern implementation registry.
"""

from __future__ import absolute_import

import importlib

from .config import conf

class Registry(object):
	""" Maps object types to model and pattern modules based on
	    configuration.
	"""

	def __init__(self):
		self.reconfigure()

	def reconfigure(self):
		types = {}

		for name, config in conf.items("type"):
			objtypes, model_name, pattern_name = self.parse_type_config(config)

			for objtype in objtypes:
				model = importlib.import_module(model_name)
				pattern = importlib.import_module(pattern_name) if pattern_name else None

				types[objtype] = model, pattern

		self.types = types

	def get_model_and_pattern(self, objkey):
		""" @type  objkey: str
		    @rtype         module, module | None
		"""
		return self.types.get(self.parse_object_type(objkey), (None, None))

	def get_common_model(self, objkeys):
		""" @type  objkeys: sequence(str)
		    @rtype          module
		"""
		objtypes = set(self.parse_object_type(key) for key in objkeys)
		models = set(self.types[typ][0] for typ in objtypes)

		if len(models) == 1:
			model, = models
			return model
		else:
			raise ValueError("Incompatible object types: " + " ".join(objtypes))

	@staticmethod
	def parse_type_config(value):
		""" @type  value: str
		    @rtype        list(str), str, str | None
		"""
		tokens = value.strip().split()

		if len(tokens) not in (2, 3):
			raise ValueError("Bad type configuration: " + value)

		if len(tokens) == 2:
			return list(tokens[0]), tokens[1], None
		else:
			return list(tokens[0]), tokens[1], tokens[2]

	@staticmethod
	def parse_object_type(objkey):
		""" @type  objkey: str
		    @rtype         str
		"""
		tokens = objkey.split("_", 1)
		return tokens[0]

class IntervalProxy(object):
	__class = None

	def __get_class(self):
		if self.__class is None:
			module = importlib.import_module(conf.get("interval", "module"))
			self.__class = module.Interval
		return self.__class

	def __call__(self, *args, **kwargs):
		return self.__get_class()(*args, **kwargs)

	def __getattr__(self, name):
		return getattr(self.__get_class())

interval_type = IntervalProxy()
