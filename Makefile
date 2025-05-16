install:
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install

clean:
	find . -path "*/*.pyc"  -delete
	find . -path "*/*.pyo"  -delete
	find . -type d -name  "__pycache__" -exec rm -r {} +
