# NHL Match Prediction
ML system for predicting NHL match outcomes with automated data collection, model training, and Telegram bot interface.

## Higload Course Checkpoints
### Checkpoint 1

📌 Описание

Это базовый сервис на FastAPI, упакованный в Docker-образ. Проект подготовлен к запуску в изолированном контейнере.
Сервис запускается через uvicorn и доступен по порту 8089.


🔑 Доступ к данным (DVC)

Модель и база данных не хранятся в Git. Они загружаются из удалённого хранилища DVC. Прилагаю ключи доступа в комментариях к заданию для авторизации.

🚀 Подготовка к запуску проекта

1. Клонируем репозиторий.

```
git clone https://github.com/sergeiperel/nhl-match-prediction.git
cd nhl-match-prediction
```

2. Скачать модель и базу данных

```
dvc pull data/sql/nhl.db.dvc logs/logistic.dvc
```

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


### Checkpoint 2



🏗 Docker Compose сборка и запуск

```
docker-compose up
```

Предстоящие матчи можно увидеть по эндпойнту matches:

`http://localhost:8089/matches`

Или можно перейти по страничке `Ближайшие матчи` -> `Смотреть полный календарь`.


🧱 Структура проекта
```
.
├── Dockerfile
├── docker-compose.yaml
├── pyproject.toml
├── poetry.lock
├── app.py
├── README.md
└── .dockerignore
```
