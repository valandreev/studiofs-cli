# 31. Локальний scratch-шар та `.studiofsignore`

DCC-програми та операційні системи постійно створюють тимчасові файли: автозбереження, проміжні `.tmp`-файли, резервні копії `.blend1`, кеші мініатюр, тимчасові теки рендеру та sidecar-файли macOS AppleDouble (`._*`). Завантаження та реплікація таких файлів через PostgreSQL і S3 витрачають канал і місце в сховищі на дані, які не потрібні іншим робочим станціям.

StudioFS вирішує це за допомогою **локального scratch-шару**, керованого файлами `.studiofsignore`.

## Як працює scratch-шар

- **Спільні правила, локальні дані**: файли `.studiofsignore` розміщуються в корені тому або в будь-якій теці. Самі файли правил є звичайними відстежуваними файлами та реплікуються на всі клієнти, тоді як файли, що підпадають під правила, залишаються лише на робочій станції, яка їх створила.
- **Синтаксис `.gitignore`**: підтримуються коментарі `#`, заперечення `!`, кінцевий `/` для тек, початковий `/` для прив’язки до теки правила та шаблони `**`. Глибші `.studiofsignore` перевизначають вищі.
- **Вбудоване правило `._*`**: файли AppleDouble `._*` потрапляють у scratch за замовчуванням із найнижчим пріоритетом. Додайте `!._*`, щоб відстежувати їх.
- **Файли правил завжди відстежуються**: `.studiofsignore` ніколи не ігнорується. Якщо шаблон збігається з `.studiofsignore`, StudioFS ігнорує його для самого файлу правил і попереджає у `studiofs status`.
- **Липка класифікація**: клас запису (tracked або scratch) визначається під час створення. Додавання правила не переміщує вже відстежувані файли (їх показує `studiofs status`). Видалення правила автоматично промоутить scratch-записи з відстежуваною батьківською текою.
- **Атомарне збереження без копіювання**: перейменування scratch-файлу (наприклад, `shot.exr.tmp`) у відстежуване ім’я (`shot.exr`) промоутить його на місці без копіювання даних.
- **Стійкість до збоїв і виключення зі знімків**: scratch-дані переживають розмонтування та аварійні зупинки на клієнті, локальний GC вважає їх живими, вони ніколи не рахуються як dirty або pending upload і виключаються зі знімків тому та S3 `janitor`.

## Приклад `.studiofsignore` для DCC-програм

Покладіть цей файл у корінь тому StudioFS або налаштуйте окремо для потрібних підтек:

```gitignore
# ==============================================================================
# Приклад .studiofsignore для DCC-пайплайнів у StudioFS
# ==============================================================================

# --- Загальні системні та тимчасові файли ---
*.tmp
*.temp
*.bak
*.swp
*.swo
*~
.DS_Store
Thumbs.db
desktop.ini

# --- Autodesk Maya ---
.mayaSwatches/
*.ma.swatches
*.mb.swatches
incrementalSave/
autosave/
*.autosave
*_tmp.ma
*_tmp.mb
mayaRenderLog.txt

# --- Blender ---
*.blend1
*.blend2
*.blend3
*.blend@
*_autosave.blend
/tmp/

# --- SideFX Houdini ---
backup/
*.hip.bak
*.hipnc.bak
*.hiplc.bak
*_bak*.hip
*_bak*.hipnc
*_bak*.hiplc
tex_cache/
sim_scratch/
*.bgeo.sc.tmp

# --- Foundry Nuke ---
*.nk~
*.nk.autosave
.nuke/
diskcache/
localcache/
viewer_cache/

# --- Adobe Photoshop ---
~$*.psd
~$*.psb
*.psd.tmp
*.psb.tmp
Photoshop Temp*
Adobe Photoshop AutoRecover/
```

## Перевірка та керування scratch-простором

### Стан scratch у `studiofs status`

`studiofs status <volume>` показує окрему секцію **Scratch** як для змонтованого тому, так і в офлайн-режимі:

```bash
studiofs status myvolume
```

### Команди `studiofs scratch`

Усі команди `studiofs scratch` працюють і через control API змонтованого тому, і офлайн напряму проти локальної бази `redb` (передайте `--volume <name>` для офлайн-режиму та `--cache-dir <dir>` за потреби):

```bash
# Список scratch-записів або осиротілих піддерев
studiofs scratch list --volume myvolume
studiofs scratch list --volume myvolume --orphaned

# Очищення scratch (на змонтованому томі пропускає відкриті файли)
studiofs scratch clean --volume myvolume --older-than 7d
studiofs scratch clean --volume myvolume --older-than 24h renders/

# Ручний promote scratch-шляху у відстежуване сховище
studiofs scratch promote --volume myvolume -- renders/big_cache

# Відновлення осиротілого scratch-запису
studiofs scratch restore --volume myvolume 1700000000000/renders/autosave.tmp
studiofs scratch restore --volume myvolume --to recovered/autosave.tmp 1700000000000/renders/autosave.tmp
```

### Перевірка цілісності через `studiofs fsck`

```bash
studiofs fsck myvolume
studiofs fsck myvolume --repair
```
