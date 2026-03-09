# Примеры использования (Examples)

## 1. Создание нового виртуального диска (Инициализация)

Инициализация диска с использованием локального MinIO и PostgreSQL для метаданных:

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \
  --bucket mybucket \
  --access-key minioadmin \
  --secret-key minioadmin \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```

## 2. Подключение (Mount) диска

### На ОС Windows

Подключить как диск Z:

```bash
studiofs mount myvolume Z:
```

### На Linux / macOS

Подключить в локальную директорию:

```bash
mkdir -p /mnt/studio
studiofs mount myvolume /mnt/studio
```

## 3. Фоновый режим (Daemon Mode)

Подключить диск в фоновом режиме (терминал не будет заблокирован):

```bash
studiofs mount myvolume Z: -d
```

Для отключения диска в фоновом режиме:

```bash
studiofs umount Z:
```

## 4. Подключение существующего диска на новом клиенте (Discover)

Если диск уже был проинициализирован на другом компьютере, вам не нужно вызывать `init`. Достаточно получить последние метаданные:

```bash
studiofs discover myvolume \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```
После этого диск можно монтировать:
```bash
studiofs mount myvolume Y:
```

## 5. Очистка старых объектов (Janitor)

Посмотреть, какие объекты больше не используются (dry-run):

```bash
studiofs janitor myvolume --dry-run
```

Запуск реальной очистки (удалить до 500 старых объектов):

```bash
studiofs janitor myvolume --max-deletes 500
```
