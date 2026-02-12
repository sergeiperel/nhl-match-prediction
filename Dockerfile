FROM python:3.12-slim

RUN pip install --no-cache-dir poetry

COPY pyproject.toml poetry.lock ./

# Устанавливаем только зависимости, не проект, без venv
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

COPY app.py ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8089"]
