from __future__ import absolute_import

import json
import signal
import sys
import time

import zmq

import signalfd

from .. import eventlog
from ..config import conf, log
from ..service import Main

class NoLock(object):

	def __enter__(self):
		pass

	def __exit__(self, *exc):
		pass

def main(args):
	signal_fd = signalfd.init()

	with Main(args, NoLock) as service:
		try:
			context = zmq.Context()

			try:
				socket = context.socket(zmq.SUB)
				socket.bind(conf.get("zeromq", "bind"))
				socket.setsockopt(zmq.SUBSCRIBE, b"")

				poller = zmq.Poller()
				poller.register(socket, zmq.POLLIN)
				poller.register(signal_fd, zmq.POLLIN)

				try:
					flush_interval = conf.getint("backup", "interval")
					flush_time = time.time()

					while True:
						timeout = flush_time + flush_interval - time.time()
						if timeout > 0:
							for x, mask in poller.poll(timeout * 1000):
								if mask & ~zmq.POLLIN:
									log.error("poll event: file=%r mask=0x%x", x, mask)

								if mask & zmq.POLLIN:
									if x == socket:
										handle_add(service, socket)
									elif x == signal_fd:
										if handle_signal(service):
											return
						else:
							service.flush()
							flush_time = time.time()
				finally:
					socket.close()
			finally:
				context.term()
		except:
			log.critical("terminated", exc_info=True)

def handle_add(service, socket):
	try:
		sitename, objkeys, params = socket.recv().split(None, 2)
		service.add(sitename, json.loads(objkeys), params)
	except:
		log.exception("service add")
		eventlog.logger.service_error(eventlog.ERROR_OTHER)

def handle_signal(service):
	num = signalfd.read()

	if num == signal.SIGINT:
		log.debug("SIGINT received")
		return True
	elif num == signal.SIGTERM:
		log.debug("SIGTERM received")
		return True
	elif num == signal.SIGHUP:
		log.info("reconfiguring")
		service.reconfigure()
	elif num == signal.SIGUSR1:
		log.debug("SIGUSR1 received")
		if conf.getboolean("debug", "force_cache_rotation", False):
			service.flush(force_rotate=True)
	elif num != signal.SIGCHLD:
		log.debug("signal %r ignored", num)

if __name__ == "__main__":
	main(sys.argv[1:])
