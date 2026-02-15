# NHL Match Prediction
ML system for predicting NHL match outcomes with automated data collection, model training, and Telegram bot interface.

## Higload Course Checkpoints
### Checkpoint 1

📌 Описание

Это базовый сервис на FastAPI, упакованный в Docker-образ. Проект подготовлен к запуску в изолированном контейнере.
Сервис запускается через uvicorn и доступен по порту 8089.


🏗 Сборка Docker-образа

Из корня проекта выполнить:

```
docker build -t nhl-match-prediction .
```

После успешной сборки появится образ nhl-match-prediction.

Проверить:

docker images

▶️ Запуск контейнера

```
docker run --rm -it -p 8089:8089 nhl-match-prediction
```

После запуска сервис будет доступен по адресу:

`http://localhost:8089`



🧱 Структура проекта
```
.
├── Dockerfile
├── pyproject.toml
├── poetry.lock
├── app.py
├── README.md
└── .dockerignore
```
