""" Main module for impress-service.  Maintains a Cache instance and provides
    Thrift access to it.
"""

from __future__ import absolute_import

import Queue as queue
import errno
import gc
import logging
import operator
import os
import select
import signal
import sys
import threading
import time

import signalfd

import thrift.server.TServer as thriftserver
import thrift.transport.TSocket as thriftsocket
import thrift.transport.TTransport as thrifttransport

import impress_thrift.ImpressCache as thriftapi

from .. import eventlog
from .. import util
from ..config import reconfigure, conf, log
from ..service import Main

def main(args):
	with Main(args, threading.Lock) as service:
		control = Control()

		addqueue = AddQueue(service)
		interface = Interface(addqueue, service)

		# Thrift setup

		processor = Processor(interface)
		transport = ServerSocket(port=conf.getint("thrift", "port"))
		server = thriftserver.TThreadPoolServer(processor, transport, daemon=True)
		server.setNumThreads(conf.getint("thrift", "threads"))

		# threads

		addqueue_thread = threading.Thread(target=addqueue.process)
		addqueue_thread.daemon = True

		service_thread = threading.Thread(target=server.serve)
		service_thread.daemon = True

		try:
			log.debug("starting add queue")
			addqueue_thread.start()

			log.info("starting thrift service")
			service_thread.start()

			# main loop

			while True:
				condition = control.sleep()

				if condition == control.Condition.Timeout:
					service.cache.flush()

				elif condition == control.Condition.Terminate:
					break

				elif condition == control.Condition.Hangup:
					log.info("reconfiguring")
					util.safe(reconfigure, error="reconfiguration failed")
					util.safe(registry.reconfigure, error="registry reconfiguration failed")

				elif condition == control.Condition.User1:
					if conf.getboolean("debug", "force_cache_rotation", False):
						service.cache.flush(force_rotate=True)
		finally:
			log.info("closing add queue")
			addqueue.close()

class AddQueue(object):
	""" Unbounded queue for add requests.
	"""
	def __init__(self, service):
		self.service = service
		self.queue = queue.Queue()
		self.closed = False

	def put(self, args):
		""" Append ImpressCache.add parameter tuple to the queue
		    (unless the queue is closed).
		"""
		if not self.closed:
			self.queue.put(args)

	def close(self):
		""" Closes the queue and blocks until the pending entries have
		    been processed.
		"""
		self.closed = True
		self.queue.join()

	def process(self):
		""" Process the queue forever.
		"""
		while True:
			args = self.queue.get()
			try:
				self.service.add(*args)
			except:
				log.exception("add")
			finally:
				self.queue.task_done()

class Interface(thriftapi.Iface):
	""" Implements the ImpressCache Thrift API.
	"""
	def __init__(self, addqueue, service):
		self.addqueue = addqueue
		self.service = service
		self.start_time = time.time()

	def add(self, *args):
		""" Append the add request to the add queue.  See service.Adder for the
		    actual implementation.
		"""
		self.addqueue.put(args)

	def get(self, *args):
		return self.service.get(*args)

	def aliveSince(self):
		return long(self.start_time)

	counters = {
		"gc.count0": lambda: gc.get_count()[0],
		"gc.count1": lambda: gc.get_count()[1],
		"gc.count2": lambda: gc.get_count()[2],
		"gc.objects": lambda: len(gc.get_objects()),
		"proc.vmsize": lambda: [int(line.split()[1]) * 1024 for line in open("/proc/%d/status" % os.getpid()) if line.startswith("VmSize:")][0],
	}

	def getCounter(self, key):
		return self.counters[key]()

	def getCounters(self):
		return { key: getter() for key, getter in self.counters.iteritems() }

	options = {
		"log.level": (
			lambda value: log.setLevel(getattr(logging, value.upper())),
			lambda: logging.getLevelName(log.getEffectiveLevel()),
		),
		"gc.enabled": (
			lambda value: getattr(gc, "disable" if value == "false" else "enable")(),
			lambda: str(gc.isenabled()).lower(),
		),
		"gc.collect": (
			lambda value: gc.collect(*[] if value == "full" else [int(value)]),
			lambda: "",
		),
		"gc.debug": (
			lambda value: gc.set_debug(reduce(operator.or_, (getattr(gc, "DEBUG_" + name.strip().upper()) for name in ("" if value == "none" else value).split()), 0)),
			lambda: str(gc.get_debug()),
		),
		"gc.thresholds": (
			lambda value: gc.set_threshold(*[int(x) for x in value.split()]),
			lambda: " ".join(str(x) for x in gc.get_threshold()),
		),
		"signal.raise": (
			lambda value: os.kill(os.getpid(), getattr(signal, value)) if value.startswith("SIG") else (lambda: None)(),
			lambda: "",
		),
	}

	def setOption(self, key, value):
		setter, getter = self.options[key]
		setter(value)

	def getOption(self, key):
		setter, getter = self.options[key]
		return getter()

	def getOptions(self):
		return { key: getter() for key, (setter, getter) in self.options.iteritems() }

class Control(object):
	""" Main loop support.
	"""

	Condition = util.Enum(
		Timeout     = 0,
		Terminate   = 1,
		Hangup      = 2,
		User1       = 3,
	)

	def __init__(self):
		self.fd = signalfd.init()
		self.interval = conf.getint("backup", "interval")

	def sleep(self):
		""" Try to sleep until the configured timeout.

		    @rtype Control.Condition
		"""
		start = time.time()

		while True:
			elapsed = time.time() - start
			timeout = self.interval - elapsed

			if timeout < 0:
				break

			try:
				rlist, wlist, xlist = select.select([self.fd], [], [], timeout)
			except select.error as e:
				if e.args[0] == errno.EINTR:
					continue
				else:
					raise

			if not rlist:
				return self.Condition.Timeout

			num = signalfd.read()
			if num is None:
				log.warning("EOF from signal fd")
				return self.Condition.Terminate

			if num == signal.SIGTERM:
				log.debug("SIGTERM received")
				return self.Condition.Terminate
			elif num == signal.SIGINT:
				log.debug("SIGINT received")
				return self.Condition.Terminate
			elif num == signal.SIGHUP:
				log.debug("SIGHUP received")
				return self.Condition.Hangup
			elif num == signal.SIGUSR1:
				log.debug("SIGUSR1 received")
				return self.Condition.User1
			elif num != signal.SIGCHLD:
				log.debug("signal %d ignored", num)

class ThriftWrap(object):
	""" Support for wrapping function calls so that any exceptions will be
	    reported to eventlog as ImpressThriftError events.
	"""
	@staticmethod
	def wrap(func, *args):
		try:
			return func(*args)
		except thrifttransport.TTransportException:
			raise
		except IOError:
			eventlog.logger.service_error(eventlog.ERROR_NETWORK)
			raise
		except:
			eventlog.logger.service_error(eventlog.ERROR_OTHER)
			raise

class Processor(thriftapi.Processor, ThriftWrap):
	""" Report Thrift I/O errors to eventlog.
	"""
	def process(self, *args):
		return self.wrap(super(Processor, self).process, *args)

class ServerSocket(thriftsocket.TServerSocket, ThriftWrap):
	""" Report Thrift accept errors to eventlog.
	"""
	def accept(self, *args):
		return self.wrap(super(ServerSocket, self).accept, *args)

if __name__ == "__main__":
	main(sys.argv[1:])
