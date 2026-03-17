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


### Checkpoint 3

#### Асинхронное обучение моделей

📌 Описание

В этом checkpoint-е обучение моделей вынесено в отдельную очередь задач с использованием Celery и Redis.
Теперь сервер (backend) не блокируется во время долгих вычислений - задачи выполняются асинхронно через worker.

Основные компоненты:
- Backend / Producer - принимает запросы от пользователя и ставит задачи в очередь.
- Worker / Consumer - слушает очередь, выполняет обучение модели и обновляет статусы в базе данных.
- Celery - библиотека для управления очередью задач.
- Redis - брокер сообщений и очередь задач.
- Tasks table - таблица в SQLite, где хранится статус каждой задачи: pending → in_progress → success/failure.

Новые эндпойнты:

1. POST /tasks/train_model


Запускает обучение модели:
```
{
  "model_type": "logistic" // или "random_forest"
}
```

Возвращает:
```
{
  "task_id": "b3db3ae4-c879-4e07-9846-18fb34fe62d6",
  "status": "pending"
}
```

2. GET /tasks/{task_id}

Получение статуса задачи:

{
  "id": "b3db3ae4-c879-4e07-9846-18fb34fe62d6",
  "task_type": "train_model",
  "status": "success",
  "created_at": "2026-03-17 11:27:19",
  "started_at": "2026-03-17 11:28:19",
  "finished_at": "2026-03-17 11:28:23",
  "result": "{'accuracy': 0.5679, 'roc_auc': 0.5917, 'log_loss': 0.6756}"
}


Запуск проекта с worker
```
docker-compose up --build
```


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
