import multiprocessing
import os
import sys
import time
import unittest

import impress.util as impl

class safe(unittest.TestCase):

	def test_ok(self):
		def func(a, b, c, d=None):
			assert a == 1 and b == 2
			assert c == 3 and d == 4
			return 5

		ret = impl.safe(func, (1, 2), dict(c=3, d=4), error="message")
		assert ret == 5

	def test_bad_args(self):
		def func(arg):
			pass

		ret = impl.safe(func, default=6, error="message")
		assert ret == 6

class timing(unittest.TestCase):

	def test(self):
		with impl.timing() as timing:
			time.sleep(0.01)

		assert float(timing) > 0.01
		assert int(timing) == int(float(timing))
		assert str(timing) == str(float(timing))

class Fork(unittest.TestCase):

	def test_ok(self):
		parent_pid = os.getpid()

		pipe = os.pipe()

		with impl.Fork() as child:
			if child:
				os.write(pipe[1], "x")

		assert os.getpid() == parent_pid

		try:
			child.join()
		except child.Error:
			assert False

		assert os.read(pipe[0], 2) == "x"

	def test_exit_ok(self):
		with impl.Fork() as child:
			if child:
				sys.exit(0)

		try:
			child.join()
		except child.Error:
			assert False

	def test_exit_error(self):
		with impl.Fork() as child:
			if child:
				sys.exit(1)

		try:
			child.join()
		except child.Error:
			pass
		else:
			assert False

	def test_exception(self):
		with impl.Fork() as child:
			if child:
				raise Exception("message")

		try:
			child.join()
		except child.Error:
			pass
		else:
			assert False
