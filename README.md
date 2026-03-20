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
dvc pull data/sql/nhl.db.dvc logs/logistic.dvc logs/random_forest.dvc
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
docker-compose up --build
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
- Tasks table - таблица в SQLite, где хранится статус каждой задачи: `pending → in_progress → success/failure`.

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
```
{
  "id": "b3db3ae4-c879-4e07-9846-18fb34fe62d6",
  "task_type": "train_model",
  "status": "success",
  "created_at": "2026-03-17 11:27:19",
  "started_at": "2026-03-17 11:28:19",
  "finished_at": "2026-03-17 11:28:23",
  "result": "{'accuracy': 0.5679, 'roc_auc': 0.5917, 'log_loss': 0.6756}"
}
```

Запуск проекта с worker и Flower
```
docker-compose up --build
```

- Backend: `http://localhost:8089`
- Flower (Celery monitoring): `http://localhost:5555`


Flower позволяет в реальном времени отслеживать:
- Состояние очередей
- Выполнение задач
- Историю выполненных и текущих задач
- Метрики Celery


### Checkpoint 4
#### Production deployment с nginx

📌 Описание

В этом checkpoint-е проект переведён в production-окружение с использованием nginx для отдачи статических файлов и reverse proxy к FastAPI backend.
Структура окружения через Docker Compose:
- frontend - nginx отдаёт статику (index.html, JS)
- backend - FastAPI приложение через Uvicorn/ASGI сервер, работающий в DEBUG=False
- redis - брокер сообщений для Celery
- worker - Celery worker для асинхронных задач
- flower - мониторинг очередей Celery

Архитектура:
- все API-запросы /api/* проксируются на backend.
- статика отдается напрямую через nginx для скорости.
- конфиг находится по пути configs/default.conf.

Запуск через Docker Compose:
```
docker-compose up --build
```

### Checkpoint 5
#### ☸️ Kubernetes (базовые абстракции)

📌 Описание

В этом checkpoint-е приложение развернуто в Kubernetes-кластере с использованием базовых абстракций:

- Deployment - управление жизненным циклом приложения
- Service (NodePort) - доступ к сервису извне кластера
- Minikube - локальный Kubernetes-кластер

Цель этапа - упаковать приложение в Kubernetes-ресурсы и показать стабильную работу сервиса.


🧱 Архитектура развертывания

В кластере развернут backend-сервис с ML-моделью:
- FastAPI приложение внутри Docker-контейнера
- Управляется Deployment’ом
- Доступен извне через Service типа NodePort

```
Пользователь → NodePort Service → Pod → FastAPI приложение
```

📁 Файлы Kubernetes находятся в каталоге:
```
k8s/
 ├── backend-deployment.yaml
 └── backend-service.yaml
 ```

⚙️ Перед запуском необходимо установить:
- Docker
- Minikube
- kubectl
- DVC (для загрузки модели и данных)


☸️ Запуск Kubernetes-кластера

Запустить Minikube:
```
minikube start
```

Проверить статус:
```
minikube status
```

🐳 Сборка Docker-образа внутри Minikube

Чтобы Kubernetes мог использовать локальный образ:
```
eval $(minikube docker-env)
docker build -t nhl-backend .
```

▶️ Развертывание приложения в Kubernetes
```
kubectl apply -f k8s/
```

Проверить ресурсы:
```
kubectl get deployments
kubectl get pods
kubectl get svc
```

Ожидается:
- Pod в состоянии Running
- Service типа NodePort

🌐 Доступ к сервису
1. Получить IP Minikube:
```
minikube ip
```

2. Открыть в браузере:
```
http://<MINIKUBE_IP>:<NODE_PORT>
```

Пример: `http://192.168.49.2:30007`

🔎 Проверка работы приложения

1. Swagger API: `/docs`
2. Страница предстоящих матчей: `/matches`
3. Endpoint предсказаний: `/predict_upcoming`


📈 Масштабирование приложения (Scaling)

Deployment позволяет динамически изменять количество запущенных экземпляров приложения (pod’ов), обеспечивая горизонтальное масштабирование.

Увеличение числа реплик:
```
kubectl scale deployment nhl-backend-deployment --replicas=3
```

Проверка количества pod’ов:
```
kubectl get pods
```

Ожидается запуск нескольких экземпляров приложения:
```
nhl-backend-deployment-xxxxx   Running
nhl-backend-deployment-yyyyy   Running
nhl-backend-deployment-zzzzz   Running
```

🧪 Проверка стабильности (управление через Deployment)

Kubernetes автоматически восстанавливает приложение при сбое.

Удалить pod:
```
kubectl delete pod <POD_NAME>
```

Новый pod будет создан автоматически.

🧹 Остановка и удаление ресурсов
```
kubectl delete -f k8s/
minikube stop
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
