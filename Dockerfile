FROM jupyter/pyspark-notebook

WORKDIR /app

COPY data-integration/pyproject.toml pyproject.toml

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install

COPY data-integration/ .

ENTRYPOINT [ "poetry", "run", "python3", "main.py"]

