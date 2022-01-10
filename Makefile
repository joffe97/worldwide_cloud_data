VENV=./venv
PYTHON_DIR=$(VENV)/bin
PYTHON=$(PYTHON_DIR)/python
PIP=$(PYTHON_DIR)/pip

install:
	$(PIP) install -r requirements.txt

requirements.txt: $(VENV)/lib/python3.10/site-packages/ $(VENV)/lib64/python3.10/site-packages/
	touch requirements.txt
	mv requirements.txt requirements.txt.backup
	$(PIP) freeze > requirements.txt

test:
	$(PYTHON) -m unittest

