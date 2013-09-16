from __future__ import absolute_import

import json
import sys
import time

import zmq

from ..config import conf
from ..service import Main

class NoLock(object):

	def __enter__(self):
		pass

	def __exit__(self, *exc):
		pass

def main(args):
	with Main(args, NoLock) as service:
		context = zmq.Context()

		try:
			socket = context.socket(zmq.SUB)
			socket.bind(conf.get("zeromq", "bind"))
			socket.setsockopt(zmq.SUBSCRIBE, b"")

			try:
				flush_interval = conf.getint("backup", "interval")
				flush_time = time.time()

				while True:
					timeout = flush_time + flush_interval - time.time()
					if timeout > 0:
						if socket.poll(timeout=int(timeout * 1000)):
							handle(service, socket)
					else:
						service.cache.flush()
						flush_time = time.time()
			finally:
				socket.close()
		finally:
			context.term()

def handle(service, socket):
	sitename, objkeys, data = socket.recv().split(None, 2)
	service.add(sitename, json.loads(objkeys), data)

if __name__ == "__main__":
	main(sys.argv[1:])
