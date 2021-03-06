PYTHON		:= python
THRIFT		:= thrift

VERSION		:= 4

DESTDIR		:= 
PREFIX		:= /usr/local
SYSCONFDIR	:= /etc
BINDIR		:= $(PREFIX)/bin
CONFDIR		:= $(SYSCONFDIR)/impress$(VERSION)

default: build

thrift-api:: gen-py/impress_thrift/Impress.py

gen-py/impress_thrift/Impress.py: impress.thrift
	$(THRIFT) --gen py:new_style $^

run-thrift-daily:: thrift-api
	bin/service-thrift -f etc/daily.conf

run-thrift-hourly:: thrift-api
	bin/service-thrift -f etc/hourly.conf

run-zeromq-daily::
	bin/service-zeromq -f etc/daily.conf

run-zeromq-hourly::
	bin/service-zeromq -f etc/hourly.conf

build:: thrift-api
	$(PYTHON) setup.py build
	git describe --tags --always --long > build/version

check::
	$(PYTHON) -m unittest discover tests

install-lib::
	$(PYTHON) setup.py install --root=/$(DESTDIR) --prefix=$(PREFIX)

install-bin: impress-service-thrift impress-service-zeromq impress-support impress-tool

impress-%:: bin/%
	install -d $(DESTDIR)$(BINDIR)
	sed < $^ > $(DESTDIR)$(BINDIR)/$@ \
		-e "d,PYTHONPATH=.*," \
		-e "s,-f etc/,-f $(CONFDIR)/,g"
	chmod 755 $(DESTDIR)$(BINDIR)/$@

clean::
	rm -f impress/*.py[co] impress/*/*.py[co]
	rm -rf gen-py
	rm -rf build
