FROM python:3.10.3-slim-buster

# stream output to console
ENV PYTHONBUFFERED=1

# install poetry inside the container
RUN pip install poetry==1.8.3

WORKDIR /app

# Set PYTHONPATH to include the /app/src directory
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

# copy the pyproject.toml and poetry.lock files into the container
COPY pyproject.toml poetry.lock /app/

# install python dependencies from the pyproject.toml file
RUN poetry install

# copy all the source code into the container
COPY . /app

CMD ["poetry", "run", "python", "src/main.py"]