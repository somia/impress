PYTHON		:= python
THRIFT		:= thrift

VERSION		:= 4

DESTDIR		:= 
PREFIX		:= /usr/local
SYSCONFDIR	:= /etc
BINDIR		:= $(PREFIX)/bin
CONFDIR		:= $(SYSCONFDIR)/impress$(VERSION)

default: build

thrift:: gen-py/impress_thrift/Impress.py

gen-py/impress_thrift/Impress.py: impress.thrift
	$(THRIFT) --gen py:new_style $^

run:: thrift
	bin/service

build:: thrift
	$(PYTHON) setup.py build
	git describe --tags --always --long > build/version

check::
	$(PYTHON) -m unittest discover tests

install-lib::
	$(PYTHON) setup.py install --root=/$(DESTDIR) --prefix=$(PREFIX)

install-bin: impress-service impress-support impress-tool

impress-%:: bin/%
	install -d $(DESTDIR)$(BINDIR)
	sed < $^ > $(DESTDIR)$(BINDIR)/$@ \
		-e "d,PYTHONPATH=.*," \
		-e "s,CONFIG=.*,CONFIG=\"-f $(CONFDIR)/common.conf -f $(CONFDIR)/$(notdir $^).conf\","
	chmod 755 $(DESTDIR)$(BINDIR)/$@

clean::
	rm -f impress/*.py[co] impress/models/*.py[co] impress/patterns/*.py[co]
	rm -rf gen-py
	rm -rf build
