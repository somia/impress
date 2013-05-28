import ConfigParser as configparser
import argparse
import gc
import logging
import logging.config
import operator
import sys

class Wrapper(object):
	__slots__ = ["_impl"]

	def __init__(self):
		self._impl = None

	def __getattr__(self, name):
		return getattr(self._impl, name)

class Conf(Wrapper):
	_args = []

	NO_DEFAULT = object()

	def set(self, section, option, value):
		if not self._impl.has_section(section):
			self._impl.add_section(section)

		self._impl.set(section, option, value)

	def get(self, section, option, default=NO_DEFAULT):
		try:
			return self._impl.get(section, option)
		except (configparser.NoSectionError, configparser.NoOptionError):
			if default is Conf.NO_DEFAULT:
				raise
			else:
				return default

	def getboolean(self, section, option, default=NO_DEFAULT):
		try:
			return self._impl.getboolean(section, option)
		except (configparser.NoSectionError, configparser.NoOptionError):
			if default is Conf.NO_DEFAULT:
				raise
			else:
				return default

conf = Conf()

class Log(Wrapper):
	pass

log = Log()

class LogFile(object):
	def __init__(self):
		self.buf = ""

	def write(self, data):
		lines = (self.buf + data).split("\n")
		if not lines:
			return

		self.buf = lines[-1]
		del lines[-1]

		for line in lines:
			log.debug(line)

	def flush(self):
		if self.buf:
			log.debug(self.buf)
			self.buf = ""

	def close(self):
		self.flush()

class AppendConfigArgument(argparse.Action):

	def __call__(self, parser, namespace, values, option_string=None):
		conf._args.append((self.dest, values))

def argument_parser(**kwargs):
	config_parser = argparse.ArgumentParser(add_help=False)
	config_parser.add_argument("-f", metavar="FILENAME", action=AppendConfigArgument, help="load config file")
	config_parser.add_argument("-c", metavar="SECTION.KEY=VALUE", action=AppendConfigArgument, help="set config option")

	return argparse.ArgumentParser(parents=[config_parser], **kwargs)

def configure(component, redirect_stderr=True):
	conf._component = component

	reconfigure()

	logconf = conf.get("logging", "config")
	logging.config.fileConfig(logconf)
	log._impl = logging.getLogger(component)

	if redirect_stderr:
		sys.stderr = LogFile()

def reconfigure():
	temp = Conf()
	temp._impl = configparser.SafeConfigParser()

	for opt, arg in conf._args:
		if opt == "c":
			key, value = arg.split("=", 1)
			if conf._component and "." not in key:
				section, option = conf._component, key
			else:
				section, option = key.split(".", 1)
			temp.set(section, option, value.strip())
		else:
			temp.read(arg)

	conf._impl = temp._impl

	gc.set_debug(reduce(operator.or_, (getattr(gc, "DEBUG_" + name.strip().upper()) for name in conf.get("gc", "debug", "").split()), 0))
