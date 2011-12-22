# Convenience to run tests and coverage.
# This probably won't work on Windows.

FLAGS=
TESTS=`find datafeed -name test_[a-z]\*.py`
NONTESTS=`find datafeed -name [a-z]\*.py ! -name test_\*.py`
PORT=8082
ADDRESS=localhost
PYTHON=python -Wignore

test:
	$(PYTHON) -m datafeed/tests/runtests $(FLAGS)

full_test:
	for i in $(TESTS); \
	do \
	  echo $$i; \
	  $(PYTHON) -m `dirname $$i`/`basename $$i .py` $(FLAGS); \
	done

test_datastore:
	$(PYTHON) -m datafeed/tests/test_datastore $(FLAGS)

test_imiguserver:
	$(PYTHON) -m datafeed/tests/test_imiguserver $(FLAGS)

test_server:
	$(PYTHON) -m datafeed/tests/test_server $(FLAGS)

test_client:
	$(PYTHON) -m datafeed/tests/test_client $(FLAGS)

test_exchange:
	$(PYTHON) -m datafeed/tests/test_exchange $(FLAGS)

test_s_google:
	$(PYTHON) -m datafeed/providers/tests/test_google $(FLAGS)

c cov cove cover coverage:
	python-coverage erase
	for i in $(TESTS); \
	do \
	  echo $$i; \
	  PYTHONPATH=. python-coverage run -p $$i; \
	done
	python-coverage combine
	python-coverage html -d "`pwd`/htmlcov" $(NONTESTS)
	python-coverage report -m $(NONTESTS)
	echo "open file://`pwd`/htmlcov/index.html"

serve:
	server.py --port $(PORT) --address $(ADDRESS)

debug:
	server.py --port $(PORT) --address $(ADDRESS) --debug

clean:
	rm -rf htmlcov
	rm -f `find . -name \*.pyc -o -name \*~ -o -name @* -o -name \*.orig`
