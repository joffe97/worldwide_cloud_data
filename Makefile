init: activate
	pip install -r requirements.txt

activate:
	./venv/bin/activate

requirements: activate
	pip freeze > requirements.txt

test: activate
	python -m unittest

