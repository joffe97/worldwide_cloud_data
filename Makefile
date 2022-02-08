VENV=./venv
PYTHON_DIR=$(VENV)/bin
PYTHON=$(PYTHON_DIR)/python
PIP=$(PYTHON_DIR)/pip

install:
	$(PIP) install -r ./wwclouds/requirements.txt

./wwclouds/requirements.txt: $(VENV)/lib/python3.10/site-packages/ $(VENV)/lib64/python3.10/site-packages/
	touch ./wwclouds/requirements.txt
	mv ./wwclouds/requirements.txt ./wwclouds/requirements.txt.backup
	$(PIP) freeze > ./wwclouds/requirements.txt

test:
	$(PYTHON) -m unittest

