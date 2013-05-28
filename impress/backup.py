import cPickle as pickle
import os

class NewBackup(object):
	""" Dump backup object.
	"""
	def __init__(self, obj):
		self.obj = obj
		self.data = None

	def dump(self, file):
		if self.data is not None:
			file.write(self.data)
		else:
			pickle.dump(self.obj, file)

	def dumps(self):
		if self.data is None:
			self.data = pickle.dumps(self.obj)
			del self.obj

		return self.data

class BackupData(object):
	""" Load backup from data string.
	"""
	def __init__(self, data, time):
		self.data = data
		self.time = time

	def load(self):
		return pickle.loads(self.data)

class BackupFile(object):
	""" Load backup from filesystem.
	"""
	def __init__(self, filename):
		self.time = os.stat(filename).st_mtime
		self.file = open(filename)

	def load(self):
		return pickle.load(self.file)
