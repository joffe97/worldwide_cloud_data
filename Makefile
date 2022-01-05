init: activate
	pip install -r requirements.txt

.PHONY: activate
	source ./venv/bin/activate

requirements: activate
	touch requirements.txt
	mv requirements.txt requirements.txt.backup
	pip freeze > requirements.txt

test: activate
	python -m unittest

