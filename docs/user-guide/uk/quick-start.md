# Швидкий старт StudioFS

[Зміст посібника](README.md) · [English](../en/quick-start.md)

Ця сторінка — найкоротший шлях від порожнього сервера до спільних дисків для вашої команди. Пояснень тут мінімум: якщо якийсь крок незрозумілий або щось пішло не так, відкрийте відповідну главу [повного посібника](README.md).

## Дані окремо, метадані окремо

StudioFS розділяє ваш диск на дві частини й зберігає їх у різних місцях:

- **Дані** — це вміст файлів: відео, звук, зображення, проєкти. Вони зберігаються в **S3-сховищі** у вигляді зашифрованих частин із технічними іменами. Відкривши bucket, ви не побачите там звичних папок чи файлу `interview.mov`.
- **Метадані** — це все, що перетворює набір частин на диск: назви файлів, структура папок, розміри, дати, відомості про те, з яких частин складається кожен файл, а також ключ шифрування тому. Спільна копія метаданих зберігається в **PostgreSQL**.

Кожен робочий комп’ютер має власну локальну копію метаданих і кеш потрібних даних, тому для перегляду папок і повторного відкриття файлів не доводиться чекати на сервер. Зміни спочатку зберігаються локально, а потім у фоновому режимі передаються: вміст — у S3, метадані — у PostgreSQL. Звідти їх отримують інші комп’ютери.

Звідси головне правило: **S3 і PostgreSQL працюють лише в парі.** Без метаданих набір об’єктів у S3 не вдасться перетворити назад на файли, а без S3 метадані — лише перелік назв. Тому резервні копії потрібні для обох.

## Що ви отримаєте наприкінці

Два диски StudioFS зі спільною базою метаданих:

- **`homesilo`** — дані зберігаються у власному сховищі **RustFS** за проксі **Caddy** на вашому сервері.
- **`cloudsilo`** — дані зберігаються в **Backblaze B2**.

```text
                       ┌── Tailscale ── PostgreSQL :5432        (метадані обох дисків)
Робочі комп’ютери ─────┼── HTTPS ────── RustFS + Caddy          (дані homesilo)
                       └── HTTPS ────── Backblaze B2            (дані cloudsilo)
```

Якщо потрібен лише один із дисків, пропустіть кроки для іншого.

## Що потрібно для початку

- **Сервер для PostgreSQL** — Linux-сервер або VPS із Docker і доступом через SSH.
- **Сервер для RustFS** — Linux-сервер із Docker, достатнім дисковим простором, доменом і відкритими портами 80/443. Може бути тим самим сервером, що й PostgreSQL.
- **Обліковий запис Backblaze** — для диска `cloudsilo`.
- **Робочі комп’ютери** — один або кілька комп’ютерів із Windows, macOS або Linux, на яких працюватимуть із файлами. Бажано, щоб на SSD було вільне місце для кешу.
- **Обліковий запис Tailscale** — для приватного доступу всіх комп’ютерів до PostgreSQL.

## 1. Tailscale на сервері

Зареєструйтеся в [Tailscale](https://tailscale.com/), а на сервері виконайте:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
tailscale ip -4
```

Запишіть адресу на кшталт `100.101.102.103` — далі вона позначається як `PG_TAILSCALE_IP`. Докладніше — у [главі 08](08-tailscale-netbird-a-private-path-to-postgresql.md).

## 2. PostgreSQL

```bash
mkdir -p ~/studiofs-postgres/pgdata && cd ~/studiofs-postgres
```

Створіть `compose.yaml` (замініть пароль на власний):

```yaml
services:
  postgres:
    image: postgres:18-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: studiofs
      POSTGRES_USER: studiofs
      POSTGRES_PASSWORD: CHANGE_ME_STRONG_PASSWORD
      PGDATA: /var/lib/postgresql/data
    ports:
      - "5432:5432"
    volumes:
      - ./pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U studiofs -d studiofs"]
      interval: 10s
      timeout: 5s
      retries: 5
```

```bash
docker compose up -d
docker compose ps        # дочекайтеся статусу healthy
```

**Закрийте порт 5432 для інтернету** у firewall провайдера VPS.

Про налаштування продуктивності й резервні копії [йдеться в главі 07](07-postgresql-the-brain-behind-the-files.md). **Не видаляйте папку `pgdata`**: у ній зберігаються метадані всіх дисків.

## 3. Сховище для homesilo: RustFS + Caddy

Вам знадобляться домен (наприклад, `s3.example.com`) з A-записом, що вказує на публічну IP-адресу сервера, і відкриті порти 80 та 443. RustFS можна використовувати й без домену, за адресою `IP:порт` (наприклад, `http://203.0.113.10:9000`), але це менш зручно: з’єднання не захищене HTTPS, а якщо IP-адреса сервера зміниться, endpoint доведеться оновлювати на всіх комп’ютерах.

RustFS і Caddy запускаються з одного файлу `compose.yaml`. Caddy приймає HTTPS-з’єднання, сам отримує та продовжує сертифікат Let’s Encrypt, а RustFS узагалі не доступний ззовні. Усі дані зберігаються в папках поруч із `compose.yaml`.

```bash
mkdir -p ~/rustfs && cd ~/rustfs
mkdir -p rustfs-data caddy-data caddy-config
sudo chown -R 10001:10001 rustfs-data   # RustFS у контейнері працює від UID/GID 10001
```

`compose.yaml`:

```yaml
services:
  rustfs:
    image: rustfs/rustfs:1.0.3
    restart: unless-stopped
    environment:
      RUSTFS_VOLUMES: /data
      RUSTFS_ADDRESS: 0.0.0.0:9000
      RUSTFS_CONSOLE_ENABLE: "true"
      RUSTFS_CONSOLE_ADDRESS: 0.0.0.0:9001
      RUSTFS_ACCESS_KEY: CHANGE_ME_ADMIN_KEY
      RUSTFS_SECRET_KEY: CHANGE_ME_ADMIN_SECRET
    ports:
      - "127.0.0.1:9001:9001"   # вебконсоль, лише для SSH-тунелю
    volumes:
      - ./rustfs-data:/data

  caddy:
    image: caddy:2.11
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - ./caddy-data:/data
      - ./caddy-config:/config
    depends_on:
      - rustfs
```

Поруч створіть `Caddyfile`, замінивши домен на свій:

```caddyfile
s3.example.com {
    reverse_proxy rustfs:9000
}
```

```bash
docker compose up -d
docker compose logs -f caddy   # дочекайтеся повідомлення про отриманий сертифікат; вихід — Ctrl+C
```

Більше нічого налаштовувати не потрібно: Caddy типово не обмежує розмір запиту, передає дані потоком і не змінює заголовок `Host`, від якого залежить підпис S3-запитів.

Щоб перевірити роботу, виконайте `curl -I https://s3.example.com/`. Команда має повернути відповідь від RustFS (зазвичай це `403`, бо запит без підпису, і це нормально), а не помилку з’єднання чи сертифіката.

Що де лежить:

| Папка | Вміст |
| -- | -- |
| `rustfs-data/` | Дані homesilo. Щоб зберігати їх на дисковому масиві, зробіть цю папку точкою монтування масиву або символьним посиланням на нього |
| `caddy-data/` | Сертифікати Let’s Encrypt. Не видаляйте цю папку, інакше Caddy запитуватиме сертифікати заново |
| `caddy-config/` | Службова конфігурація Caddy |

`rustfs/rustfs:1.0.3` — актуальний стабільний випуск RustFS на момент написання цієї сторінки. Перевірте [сторінку випусків](https://github.com/rustfs/rustfs/releases) і за потреби вкажіть новішу версію.

### Bucket, політика й ключ

Відкрийте вебконсоль RustFS через SSH-тунель, щоб не робити її доступною з інтернету:

```bash
ssh -L 9001:127.0.0.1:9001 user@your-server
```

Потім відкрийте в браузері `http://127.0.0.1:9001` і увійдіть з адміністративними ключами з `compose.yaml`. Для StudioFS адміністративні ключі не використовуйте: створіть окремого користувача, який має права лише на bucket та об’єкти.

1. Створіть приватний bucket `homesilo`. Цей крок можна пропустити: завдяки політиці нижче `studiofs init` створить bucket сам.
2. У розділі **Identity** створіть політику `studiofs` з таким JSON:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:ListAllMyBuckets",
           "s3:CreateBucket",
           "s3:GetBucketLocation",
           "s3:ListBucket",
           "s3:ListBucketMultipartUploads"
         ],
         "Resource": ["arn:aws:s3:::*"]
       },
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:AbortMultipartUpload",
           "s3:ListMultipartUploadParts"
         ],
         "Resource": ["arn:aws:s3:::*/*"]
       }
     ]
   }
   ```

   Перший блок дозволяє бачити й створювати bucket, а також переглядати перелік об’єктів у них. Другий — читати, записувати й видаляти об’єкти, зокрема великими частинами (multipart). Політика діє на **будь-який bucket** цього RustFS, тож для наступних дисків нічого переналаштовувати не доведеться. Видаляти самі bucket вона не дозволяє. Проте через такі широкі права тримайте в цьому RustFS лише дані StudioFS, а для інших завдань використовуйте окремий інстанс.
3. Там само створіть користувача `studiofs` із надійним секретним ключем і призначте йому політику `studiofs`. Ім’я користувача — це його Access Key, секретний ключ — Secret Key.

Значення для `init`:

| Параметр | Значення |
| -- | -- |
| `--endpoint` | `https://s3.example.com` |
| `--bucket` | `homesilo` |
| `--region` | `us-east-1` |
| `--access-key` | `studiofs` |
| `--secret-key` | секретний ключ цього користувача |

## 4. Сховище для cloudsilo: Backblaze B2

1. У панелі Backblaze створіть bucket із типом доступу **Private**. Назви bucket у B2 мають бути унікальними серед усіх користувачів, тож додайте до назви щось своє, наприклад `cloudsilo-yourstudio`. Не вмикайте Lifecycle-правила для автоматичного видалення.
2. У параметрах bucket знайдіть **Endpoint**, наприклад `s3.us-west-004.backblazeb2.com`. Частина між `s3.` і `.backblazeb2.com` — це region.
3. У розділі **Application Keys** створіть ключ, обмежений **цим bucket**, з доступом **Read and Write**. Одразу збережіть `keyID` і `applicationKey`: секретний ключ показують лише один раз.

| Параметр | Значення |
| -- | -- |
| `--endpoint` | `https://s3.us-west-004.backblazeb2.com` |
| `--bucket` | `cloudsilo-yourstudio` |
| `--region` | `us-west-004` |
| `--access-key` | `keyID` |
| `--secret-key` | `applicationKey` |

## 5. Перший комп’ютер

### Tailscale і доступ до бази

Встановіть Tailscale ([Windows](https://tailscale.com/docs/install/windows), [macOS](https://tailscale.com/docs/install/mac), [Linux](https://tailscale.com/docs/install/linux)) і увійдіть у той самий tailnet. Перевірте, чи доступний порт:

```powershell
# Windows
Test-NetConnection 100.101.102.103 -Port 5432
```

```bash
# macOS / Linux
nc -vz 100.101.102.103 5432
```

### Встановлення StudioFS

| Система | Драйвер | Докладніше |
| -- | -- | -- |
| Windows x64 | [WinFSP 2026+](https://github.com/winfsp/winfsp/releases) | [глава 10](10-install-studiofs-on-windows.md) |
| macOS | [macFUSE 5.4.0+](https://macfuse.github.io/) (потрібен дозвіл системного розширення й перезапуск) | [глава 11](11-install-studiofs-on-macos.md) |
| Linux | `fuse3` з репозиторію дистрибутива | [глава 12](12-install-studiofs-on-linux.md) |

Завантажте збірку для своєї системи зі [сторінки випусків](https://github.com/valandreev/studiofs-cli/releases), додайте `studiofs` до `PATH` і перевірте встановлення:

```bash
studiofs --version
```

На macOS і Linux підготуйте батьківську папку для монтування:

```bash
# macOS
sudo mkdir -p /Volumes/Studiofs && sudo chown "$(id -un):$(id -gn)" /Volumes/Studiofs
# Linux
sudo mkdir -p /mnt/StudioFS && sudo chown "$(id -un):$(id -gn)" /mnt/StudioFS
```

### Створення дисків

Запускайте команди від імені свого звичайного користувача, **без `sudo`**. Підставте власні значення з кроків 3 і 4 та виберіть блок для своєї оболонки. Обидва диски використовують той самий PostgreSQL.

**homesilo**, macOS / Linux:

```bash
studiofs init homesilo \
  --endpoint 'https://s3.example.com' \
  --bucket 'homesilo' \
  --region 'us-east-1' \
  --access-key 'studiofs' \
  --secret-key 'YOUR_RUSTFS_SECRET_KEY' \
  --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

**homesilo**, Windows PowerShell:

```powershell
studiofs init homesilo `
  --endpoint 'https://s3.example.com' `
  --bucket 'homesilo' `
  --region 'us-east-1' `
  --access-key 'studiofs' `
  --secret-key 'YOUR_RUSTFS_SECRET_KEY' `
  --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

**cloudsilo**, macOS / Linux:

```bash
studiofs init cloudsilo \
  --endpoint 'https://s3.us-west-004.backblazeb2.com' \
  --bucket 'cloudsilo-yourstudio' \
  --region 'us-west-004' \
  --access-key 'YOUR_B2_KEY_ID' \
  --secret-key 'YOUR_B2_APPLICATION_KEY' \
  --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

**cloudsilo**, Windows PowerShell:

```powershell
studiofs init cloudsilo `
  --endpoint 'https://s3.us-west-004.backblazeb2.com' `
  --bucket 'cloudsilo-yourstudio' `
  --region 'us-west-004' `
  --access-key 'YOUR_B2_KEY_ID' `
  --secret-key 'YOUR_B2_APPLICATION_KEY' `
  --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

Якщо все вдалося, ви побачите такі рядки:

```text
✓ Volume 'homesilo' initialized
✓ Volume 'cloudsilo' initialized
```

Якщо `init` завершився з помилкою, **не запускайте його повторно навмання й не погоджуйтеся на `CLEAR`**: спершу уважно прочитайте повідомлення про помилку (див. [главу 13](13-create-your-first-volume-studiofs-init.md)).

### Підключення

Кожен диск підключається окремою командою. Для щоденної роботи додайте параметр `-d` (фоновий режим). Без нього кожна команда займає окреме вікно термінала, зате помилки видно одразу.

```powershell
# Windows: вільні літери дисків
studiofs mount homesilo H: -d
studiofs mount cloudsilo K: -d
```

```bash
# macOS → /Volumes/Studiofs/homesilo і /Volumes/Studiofs/cloudsilo
studiofs mount homesilo /Volumes/Studiofs -d
studiofs mount cloudsilo /Volumes/Studiofs -d
# Linux → /mnt/StudioFS/homesilo і /mnt/StudioFS/cloudsilo
studiofs mount homesilo /mnt/StudioFS -d
studiofs mount cloudsilo /mnt/StudioFS -d
```

Створіть на кожному диску тестову папку, переконайтеся, що вона з’явилася, і видаліть її.

Щоб відключити диск, виконайте `studiofs umount H:` (Windows), `studiofs umount /Volumes/Studiofs/homesilo` (macOS) або `studiofs umount /mnt/StudioFS/homesilo` (Linux); для `cloudsilo` — так само. Докладніше — у главах [14](14-mount-your-volume-studiofs-mount.md) і [17](17-unplug-safely-studiofs-umount.md).

## 6. Решта комп’ютерів

На кожному новому комп’ютері встановіть Tailscale і перевірте порт 5432, а також встановіть драйвер і `studiofs`, як у кроці 5. Команда **`init` тут не потрібна**: диски вже створено.

**Додавати комп’ютер колеги до свого tailnet не обов’язково.** Достатньо надати йому доступ лише до сервера PostgreSQL:

1. У панелі Tailscale на сторінці **Machines** відкрийте меню сервера PostgreSQL і виберіть **Share**.
2. Надішліть колезі посилання-запрошення або запросіть його електронною поштою.
3. Колега встановлює Tailscale, входить у **власний** обліковий запис і приймає запрошення.

Після цього сервер з’явиться в мережі колеги з тією самою адресою `100.101.102.103`. Колега бачитиме лише цей сервер, а не інші ваші пристрої, і ви будь-коли зможете відкликати доступ. Порт 5432 перевіряють так само, як у кроці 5.

```text
studiofs discover homesilo --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
studiofs discover cloudsilo --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

`discover` завантажує лише метадані. Ключі S3 підтягуються з конфігурації диска, тож вводити їх не потрібно. Далі виконайте ту саму команду `mount`, що й на першому комп’ютері:

```powershell
studiofs mount homesilo H: -d
studiofs mount cloudsilo K: -d
```

```bash
studiofs mount homesilo /Volumes/Studiofs -d    # macOS
studiofs mount cloudsilo /Volumes/Studiofs -d
studiofs mount homesilo /mnt/StudioFS -d        # Linux
studiofs mount cloudsilo /mnt/StudioFS -d
```

Перше підключення на новому комп’ютері може тривати деякий час: StudioFS завантажує всі метадані диска. Самі файли при цьому не завантажуються — їхній вміст надходить із S3, коли ви їх відкриваєте.

**Перевірте синхронізацію в обидва боки:** створіть файл `hello-from-a.txt` на першому комп’ютері й відкрийте його на другому, а потім навпаки. Маленький файл має з’явитися майже одразу.

Виконуйте `discover` і `mount` від імені одного й того самого користувача, бо локальний стан зберігається в `~/.studiofs`. Після перезапуску повторно виконувати `discover` не потрібно — достатньо `mount`. Докладніше — у [главі 16](16-connect-a-second-computer-studiofs-discover.md).

## Що далі

- Резервне копіювання PostgreSQL **і** S3 разом — [глави 07](07-postgresql-the-brain-behind-the-files.md) та [24](24-snapshots-a-time-machine-for-your-volume.md).
- Як працюють синхронізація та кеш — [глави 19](19-sync-bad-wi-fi-and-other-surprises.md) і [20](20-cache-why-is-the-cloud-using-my-ssd.md).
- Якщо щось не працює — [глава 28](28-troubleshooting-when-the-cat-stops-purring.md).

---

[Зміст](README.md) · [English](../en/quick-start.md)
