# 07. PostgreSQL — мозок, який пам’ятає, де лежать файли
> **Сторінка в процесі доповнення**

> *«Хто з вас жодного разу не поклав базу даних у продакшені, нехай першим кине камінь у сисадміна».*
>
> — За мотивами Євангелія від Івана, 8:7

Уявіть спільний диск із тисячами відеофайлів. Дані зберігаються в S3, але звідки StudioFS знає, які файли є на диску, як називаються папки та що змінили ваші колеги?

Для цього й потрібен **PostgreSQL** — база спільних метаданих StudioFS. Налаштуймо її без зайвої серверної магії.

## Навіщо StudioFS PostgreSQL?

У системі три основні складники:

* **S3** зберігає вміст файлів.
* **PostgreSQL** координує спільну структуру диска та обмін змінами між клієнтами.
* **Локальна база й кеш** на кожному комп’ютері дають змогу працювати з файлами, не звертаючись постійно до сервера.

PostgreSQL — не файлове сховище й не посередник для кожного читання відео. Клієнти StudioFS працюють із локальними метаданими, а спільні зміни синхронізують у фоновому режимі.

Тому пам’ятайте: **файл, збережений на одному комп’ютері, не обов’язково одразу доступний на іншому**. Спершу дані мають завантажитися в S3, а метадані — синхронізуватися.

PostgreSQL також зберігає службові дані та ключі шифрування тому. Тож утратити його так само небажано, як і самі файли. До резервного копіювання ми повернемося окремо.

## Де розмістити PostgreSQL?

Найпростіші варіанти — **власний VPS** або **керований PostgreSQL у хмарі**.

На VPS ви контролюєте конфігурацію, доступ і резервні копії. У керованому (managed) сервісі частину обслуговування бере на себе провайдер, але потрібно перевірити, чи є доступ до службової бази `postgres` і право створювати об’єкти в базі `studiofs`. Поточна версія команди `studiofs init` працює саме з цими назвами.

Далі розглянемо просте розгортання на VPS за допомогою Docker Compose. Для невеликого проєкту чи тестового середовища цього достатньо, щоб почати.

## Практика: PostgreSQL 18 через Docker Compose

Вам знадобляться Ubuntu Server (або інший Linux-сервер), доступ через SSH, а також установлені Docker Engine і Docker Compose. Якщо Docker ще не встановлено, скористайтеся [офіційною інструкцією для Ubuntu](<https://docs.docker.com/engine/install/ubuntu/>). Встановлення Docker тут не розглядаємо.

Створіть каталог для PostgreSQL:

```bash
mkdir -p ~/studiofs-postgres/pgdata
cd ~/studiofs-postgres
```

У ньому створіть файл `compose.yaml`:

```yaml
services:
  postgres:
    image: postgres:18-alpine
    container_name: studiofs-pg-test
    restart: unless-stopped

    environment:
      POSTGRES_DB: studiofs
      POSTGRES_USER: studiofs
      POSTGRES_PASSWORD: studiofs
      PGDATA: /var/lib/postgresql/data

    ports:
      - "5432:5432"

    volumes:
      - type: bind
        source: ./pgdata
        target: /var/lib/postgresql/data

    command: >
      postgres
        -c shared_buffers=3GB
        -c effective_cache_size=8GB
        -c work_mem=16MB
        -c maintenance_work_mem=512MB
        -c max_connections=40
        -c superuser_reserved_connections=3
        -c synchronous_commit=on
        -c wal_buffers=16MB
        -c checkpoint_completion_target=0.9
        -c checkpoint_timeout=15min
        -c max_wal_size=4GB
        -c min_wal_size=1GB
        -c random_page_cost=1.1
        -c effective_io_concurrency=200
        -c default_statistics_target=100
        -c max_worker_processes=4
        -c max_parallel_workers_per_gather=1
        -c max_parallel_workers=2
        -c max_parallel_maintenance_workers=1
        -c huge_pages=try
        -c tcp_keepalives_idle=60
        -c tcp_keepalives_interval=10
        -c tcp_keepalives_count=6
        -c idle_in_transaction_session_timeout=120000
        -c statement_timeout=60000
        -c jit=off
        -c log_min_duration_statement=1000
        -c log_checkpoints=on
        -c log_lock_waits=on

    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U studiofs -d studiofs"]
      interval: 10s
      timeout: 5s
      retries: 5
```

Docker автоматично вибере варіант образу `postgres:18-alpine`, що відповідає архітектурі сервера, — наприклад, ARM64 або x86-64. Окремо нічого вказувати не потрібно. Наведену конфігурацію розраховано на сервер із достатнім запасом оперативної пам’яті. Якщо ваш VPS має менше ніж 8 ГБ RAM, насамперед зменште `shared_buffers` і `maintenance_work_mem`. Інші параметри також можна адаптувати до свого навантаження.

Образ `postgres:18-alpine` — компактний варіант PostgreSQL 18. Під час першого запуску він автоматично створить базу `studiofs` і користувача `studiofs` з паролем, указаним у Compose. Зверніть увагу: користувач, створений через `POSTGRES_USER`, має права суперкористувача PostgreSQL. Пароль `studiofs` у прикладі наведено лише для простоти; для реального розгортання обов’язково замініть його.

Значення `POSTGRES_USER`, `POSTGRES_PASSWORD` і `POSTGRES_DB` застосовуються лише під час першої ініціалізації порожньої бази. Якщо змінити їх у Compose пізніше, уже створені користувачі й бази даних не зміняться.

### Що з папкою `pgdata`?

Це звичайний каталог поруч із `compose.yaml`, у якому зберігатимуться всі дані PostgreSQL. Оскільки ми **явно вказали** `PGDATA`, bind mount спрямовано саме в цей каталог, хоча в офіційному образі PostgreSQL 18 типовий шлях до даних інший. Для нашого простого прикладу така схема підходить, але перед оновленням до наступної основної версії PostgreSQL перенесення даних доведеться спланувати окремо.

Зазвичай Docker сам налаштовує права доступу під час першого запуску. В Alpine-образі PostgreSQL процес бази працює від імені користувача з **UID/GID 70:70**. Якщо в журналі з’являється помилка `Permission denied` для `pgdata`, на звичайному Linux-сервері з Docker Engine зупиніть контейнер і змініть власника каталогу:

```bash
docker compose down
sudo chown -R 70:70 ./pgdata
```

Після цього запустіть контейнер повторно. Не відкривайте каталог для всіх командою `chmod 777`.

### Запускаємо й перевіряємо

```bash
docker compose up -d
docker compose ps
```

Дочекайтеся, доки контейнер отримає статус `healthy`. Якщо контейнер не запускається, перегляньте журнал:

```bash
docker compose logs --tail=50 postgres
```

Перевірмо, що потрібна база доступна, а пароль працює під час підключення через мережу Docker:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d studiofs -W \
  -c "SELECT current_database(), current_user;"
```

Коли з’явиться запит, введіть пароль користувача `studiofs`. У відповіді обидва значення мають дорівнювати `studiofs`.

І ще одна перевірка — доступ до службової бази, яку використовує поточна версія `studiofs init`:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d postgres -W \
  -c "SELECT 1;"
```

Очікуваний результат — `1`. Ці перевірки виконуються в мережі Docker; підключення з робочого комп’ютера перевіримо після налаштування VPN.

Щоб зупинити контейнер, використовуйте `docker compose down`, а щоб запустити знову — `docker compose up -d`. Дані залишаються в `./pgdata`.

**Не видаляйте папку** `pgdata`**!** У ній зберігаються метадані всієї файлової системи. Без резервної копії відновити роботу тому StudioFS може бути неможливо.

## Як StudioFS підключатиметься до бази?

Для підключення використовується **DSN** — рядок з адресою сервера, іменем користувача, паролем і назвою бази:

```text
postgresql://studiofs:studiofs@VPN_SERVER_ADDRESS:5432/studiofs
```

Замість `VPN_SERVER_ADDRESS` підставте приватну IP-адресу сервера в Tailscale або NetBird.

У цьому прикладі ми використовуємо VPN як приватний канал зв’язку. Проте **Tailscale сам по собі не закриває публічний порт**: запис `5432:5432` відкриває його на всіх мережевих інтерфейсах Docker-хоста. Тому переконайтеся, що правила firewall на VPS закривають порт 5432 для публічного інтернету. Налаштування доступу через VPN — тема наступної глави.

---

[Зміст](README.md) · [English](../en/07-postgresql-the-brain-behind-the-files.md) · [← Попередня](06-s3-a-home-for-your-files.md) · [Наступна →](08-tailscale-netbird-a-private-path-to-postgresql.md)
