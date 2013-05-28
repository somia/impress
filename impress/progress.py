from __future__ import absolute_import

from .config import conf, log

_enabled = False
_file = None

def _get_file():
	global _file

	if _file is None:
		_file = False

		name = conf.get("progress", "file", None)
		if name:
			try:
				_file = open(name, "a")
			except Exception as e:
				log.warning(e)

	return _file

def enable():
	global _enabled
	_enabled = True

def is_enabled():
	return _enabled and bool(_get_file())

def write(data=None, done=False):
	file = _get_file()
	if file:
		if data is not None:
			file.write("\r")
			file.write(str(data))

		if done:
			file.write("\n")

		file.flush()

def done(data=None):
	write(data, done=True)

class Counter(object):
	def __init__(self, total=None, interval=1, prefix=""):
		self.count = 0
		self.total = total
		self.interval = interval

		if self.total is None:
			self._format = prefix + "%d "
		else:
			self._format = prefix + "%" + str(len(str(self.total))) + "d / " + str(self.total) + " "

	def __int__(self):
		return self.count

	def __str__(self):
		return self._format % self.count

	def increment(self, value=1):
		self.count += value

		if (self.count % self.interval) < value:
			write(str(self))

	def poke(self):
		write(str(self))

	def done(self):
		done(str(self))

	def __iter__(self):
		return self(xrange(self.total - self.count))

	def __call__(self, iterable, done=True):
		for i in iterable:
			yield i
			self.increment()

		if done:
			self.done()
