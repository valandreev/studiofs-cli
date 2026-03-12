# Отчет и План: Metadata Overhaul (Local-First Architecture)

## Вводные Данные и Целевая Архитектура
План базируется на отчете [260311-meta-improvements-report.md](file:///e:/dev/studiofs-cli/docs/done/260311-meta-improvements-report.md), бэклоге (SFS-12..SFS-20) и ключевом архитектурном ограничении:
* **"Магия" для пользователя:** видимые компоненты — только `studiofs-cli`, S3 и Postgres.
* **Local-First & Eventual Consistency:** CLI всегда работает с локальной базой метаданных (`redb`). При отсутствии связи с S3 (читаем из локального кэша L2/L1) и PosteSQL (копим локальные транзакции/эвенты) клиент должен продолжать работать в рамках доступного. `Redb` остается фундаментальной частью клиента.

Замена `Redb` не требуется, так как она идеально подходит под задачу быстрого локального key-value транзакционного стораджа для оффлайн-работы.

---

## Ревизия Бэклога (Что нам дает максимальный и оптимальный результат)

### 🚀 Высший Приоритет (Runtime Ускорения над Redb)
Хотя `redb` быстр (особенно на NVMe), миллионы `readdir` и `stat`, а также трансляция путей (Path Walking), могут упираться в I/O и блокировки. Чтобы сделать VFS по-настоящему "молниеносным" в локальном режиме, нужны In-Memory кеши:
1. **[SFS-13] Dentry cache (Snapshot Cache)** — O(1) поиск дочерних узлов в RAM.
2. **[SFS-15] Path shortcut cache** — мгновенный резолв полного пути до inode без обхода дерева за O(depth).
3. **[SFS-20] Directory stat batching** — после `readdir` мы забираем метрики всех файлов одним батчем в RAM, спасая `ls -l` и Finder/Explorer от шторма одиночных I/O.
4. **[SFS-16] Metadata hotset promotion** — держать самые горячие (по частоте доступа) ветки INodes полностью в RAM.

### 🔀 Пул Отложенных/Переосмысленных Задач
* **[SFS-17] Adaptive chunked index (B+Tree)** — пока откладываем. `redb` сам использует B-Tree внутри. Громоздить еще одно B-Tree поверх таблиц `Redb` стоит только если мы реально упремся в лимиты сканирования одной огромной директории.
* **[SFS-18] O(1) rename** — в текущей модели Local-First это сложная операция, затрагивающая иерархию. Реализуем в последнюю очередь, если переименования папок профилировщик покажет как bottleneck.
* **[SFS-14, SFS-19] Prefetch/Sequence aware** — классная оптимизация для рендера, но это "тюнинг", а не фундамент.

### 🍏 Функциональные Базовые Задачи и macOS Совместимость
* **[SFS-8] macOS: deleted files remain in the local cache**
  * **Статус:** Высокий приоритет (Bug).
  * **Причина:** При работе на Mac кэш (RAM/NVMe) замусоривается удаленными файлами. В рамках переработки метаданных, когда мы будем ревизовать `pending_deletions` и Local-First Janitor, нужно явно инициировать очистку блоков из локального L1/L2 кэша сразу при удалении файла в `Redb` (дожидаясь или игнорируя S3).
* **[SFS-10] Block macOS metadata files (.DS_Store and AppleDouble `._*`)**
  * **Статус:** Оставить как неявное улучшение VFS-фильтрации.
  * **Причина:** Не хотим гонять мусор в Postgres/S3. На уровне `open`/`create` в VFS мы должны перехватывать эти файлы и отклонять/скрывать их, имитируя успешное создание только локально или прозрачно дропая.
* **[SFS-11] Block macOS system metadata directories (.Spotlight-V100, .Trashes, .fseventsd)**
  * **Статус:** Аналогично SFS-10.
  * **Причина:** Декоративная блокировка мусорных папок Finder'а.
* **[SFS-12] Native xattr + Resource fork**
  * **Статус:** Оставить. Мастхэв для нативной работы под macOS. Полностью ложится в текущую схему `Redb`, и решает проблему AppleDouble (так как macOS будет писать resource forks прямо в xattr, а не в файлы `._*`, что автоматически закрывает половину SFS-10).

---

## План Реализации: ACL, Snapshots и Janitor (Local-First Design)

### 1. Этап: ACL Roles (No Access / RO / RW)
Модель: Ролевая доступовая система без жесткой security-изоляции. **[SFS-22]**
* **Схема (Redb/PG):** Добавляем таблицы `acl_rules (ino, principal, access)`.
* **VFS Runtime:** Кешируем Effective ACL (вычисленные с учетом наследования родителя) в Inode Cache.
* **Eventual Consistency:** Если в оффлайне юзер создает папку, `Redb` локально назначает дефолтный RW ACL создателю. При появлении интернета эвент сливается в Postgres.

### 2. Этап: Snapshots в условиях Eventual Consistency
Снапшоты (Snapshots) — самая сложная часть, так как они должны быть глобальными, но клиент может уйти в оффлайн. **[SFS-23]**
* **Local Design:** В `Redb` добавляется сущность `Snapshot` (id, created_at, root_ino, revision/checkpoint_ref). Снапшот создается МГНОВЕННО локально (фиксируя текущий state).
* **Global Sync:** Асинхронно эвент уходит в PG. Клиент 2 увидит его только когда синхронизирует эвенты из PG в свою локальную `Redb`.
* **Read-Only Mount:** Снапшот монтируется как отдельный root.

### 3. Этап: Snapshot-Aware Janitor & Distributed Lease
Важнейшее требование: Janitor из Клиента 1 не должен удалить блоки, которые закрепил за собой Снапшот от Клиента 2. **[SFS-24]**
1. **Pins Mechanism:** `pending_deletions` в `Redb` не удаляют блок физически из S3, пока не сверятся с Глобальным состоянием (Postgres). 
2. **Lease System:** Janitor перед выходом в S3 **обязан** получить распределенный lock в Postgres (через `pg_advisory_lock` или отдельную маленькую таблицу `leases`), синхронизировать все снапшоты (стянуть из PG свежий pin-set) и только потом чистить S3.
3. *Оффлайн Janitor в S3 не ходит*, он только копит `pending_deletions` локально. Это гарантирует безопасность.

### 4. Этап: Format Version & Debug Tools
То, что вы упомянули как "нужно сделать сразу": **[SFS-21]**
* Добавляем поле `format_version` в [FormatInfo](file:///e:/dev/studiofs-cli/crates/studiofs-meta/src/schema.rs#386-410) (и в `Redb`, и в `PG`).
* Пишем утилиты `dump` / `load` для дампа/восстановления metadata через PostgreSQL (source of truth) в JSON/Binary.

---

## Резюме: Порядок Работы
1. **Schema Hooks [SFS-21]:** Добавление `format_version` и CLI для dump/load (`studiofs dump`, `studiofs load`). (done)
2. **Apple M1/macOS Hygiene (SFS-8, 10, 11, 12):**
   * Вырезаем macOS системные папки (`.Spotlight-V100` и т.д.) и `.DS_Store` на уровне VFS-фильтра.
   * Форсируем трансляцию Resource Forks в xattr.
   * Чиним баг с не-удаляющимся L2-кэшем при `unlink` файла (SFS-8).
3. **ACL Layer [SFS-22]:** `Redb` Tables -> Runtime Evaluator -> Тесты локального enforcement + sync.
4. **RAM Optimization (Hot Tier):** Включение Inode Cache (SFS-16), Dentry Cache (SFS-13), Path Shortcut Cache (SFS-15) и Directory stat batching (SFS-20).
5. **Snapshots Core [SFS-23]:** Структуры в БД, логика заморозки ссылок на объекты. Слой монтирования Read-Only Tree.
6. **Cluster Janitor [SFS-24]:** Остановка локального Janitor'а при оффлайне, реализация `Lock` в Postgres, сверка Pinned Blocks (через PG) перед запросами в S3.
