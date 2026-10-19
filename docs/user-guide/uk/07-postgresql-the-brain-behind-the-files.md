# 07. PostgreSQL — мозок, який пам’ятає, де лежать файли

> *«Хто з вас жодного разу не поклав базу даних у продакшені, нехай першим кине камінь у сисадміна».*
>
> — За мотивами Євангелія від Івана, 8:7

Уявіть спільний диск із тисячами відеофайлів. Дані зберігаються в S3, але звідки StudioFS знає, які файли є на диску, як називаються папки та що змінили ваші колеги?

Для цього потрібен **PostgreSQL** — база спільних метаданих StudioFS. Налаштуємо її без зайвої серверної магії.

## Навіщо StudioFS PostgreSQL?

У системі три основні складники:

* **S3** зберігає вміст файлів.
* **PostgreSQL** координує спільну структуру диска та зміни між клієнтами.
* **Локальна база й кеш** на кожному комп’ютері допомагають працювати з файлами без постійного звернення до сервера.

PostgreSQL — не файлове сховище й не посередник для кожного читання відео. Клієнти StudioFS працюють із локальними метаданими, а спільні зміни синхронізують у фоновому режимі.

Тому пам’ятайте: **файл, збережений на одному комп’ютері, не обов’язково вже доступний на іншому**. Для цього потрібні завантаження даних у S3 та синхронізація метаданих.

PostgreSQL також зберігає службові дані та ключовий матеріал тому. Отже, втрачати його так само небажано, як і самі файли. До резервного копіювання повернемося окремо.

## Де розмістити PostgreSQL?

Найпростіші варіанти — **власний VPS** або **керований PostgreSQL у хмарі**.

На VPS ви контролюєте конфігурацію, доступ і резервні копії. У managed-сервісі частину обслуговування бере на себе провайдер, але потрібно перевірити доступ до службової бази `postgres` і право створювати об’єкти у базі `studiofs`. Поточна команда `studiofs init` працює саме з цими назвами.

Далі розглянемо просте розгортання на VPS через Docker Compose. Для невеликого проєкту або тестового середовища цього достатньо, щоб почати.

## Практика: PostgreSQL 18 через Docker Compose

Нам потрібні Ubuntu Server (або інший Linux-сервер), доступ через SSH та встановлені Docker Engine і Docker Compose. Якщо Docker ще немає, скористайтеся [офіційною інструкцією для Ubuntu](<https://docs.docker.com/engine/install/ubuntu/>). Встановлення Docker тут не розглядаємо.

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

Docker автоматично вибере відповідний варіант образу `postgres:18-alpine` для архітектури сервера — наприклад, ARM64 або x86-64. Нічого окремо вказувати не потрібно. Наведена конфігурація розрахована на сервер із достатнім запасом оперативної пам’яті. Якщо ваш VPS має менш ніж 8 ГБ RAM, зменште насамперед `shared_buffers` та `maintenance_work_mem`. Інші параметри також можна адаптувати до свого навантаження.

Образ `postgres:18-alpine` — компактний варіант PostgreSQL 18. На першому запуску він автоматично створить базу `studiofs` та користувача `studiofs` з паролем, указаним у Compose. Зверніть увагу: користувач, створений через `POSTGRES_USER`, має права суперкористувача PostgreSQL. У прикладі пароль `studiofs` наведено лише для простоти; для реального розгортання замініть його.

Значення `POSTGRES_USER`, `POSTGRES_PASSWORD` і `POSTGRES_DB` застосовуються лише під час першої ініціалізації порожньої бази. Їх зміна у Compose не змінює вже створених користувачів чи баз даних.

### Що з папкою `pgdata`?

Це звичайний каталог поруч із `compose.yaml`, у якому зберігатимуться всі дані PostgreSQL. Оскільки ми **явно вказали** `PGDATA`, bind mount спрямований саме в цей каталог, хоча типовий шлях даних в офіційному образі PostgreSQL 18 інший. Така схема придатна для нашого простого прикладу; перед оновленням на наступну основну версію PostgreSQL потрібно окремо спланувати перенесення даних.

Зазвичай Docker сам налаштовує права під час першого запуску. Для Alpine-образу PostgreSQL процес бази працює від користувача з **UID/GID 70:70**. Якщо в журналі з’являється `Permission denied` для `pgdata`, на звичайному Linux-сервері з Docker Engine можна зупинити контейнер і виправити власника:

```bash
docker compose down
sudo chown -R 70:70 ./pgdata
```

Після цього повторіть запуск. Не потрібно відкривати каталог для всіх через `chmod 777`.

### Запускаємо й перевіряємо

```bash
docker compose up -d
docker compose ps
```

Дочекайтеся, поки контейнер отримає статус `healthy`. Якщо контейнер не стартує, перегляньте журнал:

```bash
docker compose logs --tail=50 postgres
```

Перевіримо, що потрібна база доступна і пароль працює при підключенні через мережу Docker:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d studiofs -W \
  -c "SELECT current_database(), current_user;"
```

Після запиту введіть пароль користувача `studiofs`. У відповіді обидва значення мають бути `studiofs`.

І ще одна перевірка — доступ до службової бази, яку використовує поточний `studiofs init`:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d postgres -W \
  -c "SELECT 1;"
```

Очікуємо `1`. Це перевірки в мережі Docker; підключення з робочого комп’ютера перевіримо після налаштування VPN.

Для зупинки контейнера використовуйте `docker compose down`, для повторного запуску — `docker compose up -d`. Дані залишаються в `./pgdata`.

**Не видаляйте папку** `pgdata`**!** У ній зберігаються метадані всієї файлової системи. Без резервної копії відновити роботу тому StudioFS може бути неможливо.

## Як StudioFS підключатиметься до бази?

Для підключення використовується **DSN** — рядок з адресою сервера, користувачем, паролем і назвою бази:

```text
postgresql://studiofs:studiofs@VPN_SERVER_ADDRESS:5432/studiofs
```

Замість `VPN_SERVER_ADDRESS` буде приватна IP-адреса сервера в Tailscale або NetBird.

Для цього прикладу ми використовуємо VPN як приватний транспорт. Проте **Tailscale сам по собі не закриває публічний порт**: запис `5432:5432` публікує його на всіх інтерфейсах Docker-хоста. Тому переконайтеся, що порт 5432 закритий для публічного інтернету правилами firewall VPS. Налаштування доступу через VPN — тема наступної глави.

---

[Зміст](README.md) · [English](../en/07-postgresql-the-brain-behind-the-files.md) · [← Попередня](06-s3-a-home-for-your-files.md) · [Наступна →](08-tailscale-netbird-a-private-path-to-postgresql.md)

[Джерело в Linear](https://linear.app/valandreev/document/07-postgresql-the-brain-behind-the-files-922bc87af337) · Оновлення джерела: 2026-10-19T20:16:35.184Z · Український оригінал прийнято для перенесення.
