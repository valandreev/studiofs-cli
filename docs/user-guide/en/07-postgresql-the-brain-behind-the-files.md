# 07. PostgreSQL: the brain that remembers where files are
> **Work in progress**

> *“Let the one among you who has never put a database into production cast the first stone at the sysadmin.”*
>
> — Inspired by the Gospel of John, 8:7

Imagine a shared drive with thousands of video files. The data is stored in S3, but how does StudioFS know which files are on the drive, what the folders are called, and what your colleagues have changed?

That is what **PostgreSQL** is for: the shared metadata database for StudioFS. Let’s set it up without unnecessary server magic.

## Why does StudioFS need PostgreSQL?

The system has three main parts:

* **S3** stores file contents.
* **PostgreSQL** coordinates the shared drive structure and changes between clients.
* The **local database and cache** on each computer help you work with files without constantly contacting the server.

PostgreSQL is not file storage or a relay for every video read. StudioFS clients work with local metadata and synchronize shared changes in the background.

So remember: **a file saved on one computer is not necessarily available on another yet**. The data has to be uploaded to S3 and the metadata synchronized first.

PostgreSQL also stores system data and key material for the volume. So losing it is just as undesirable as losing the files themselves. We will return to backups separately.

## Where should you host PostgreSQL?

The simplest options are **your own VPS** or **managed PostgreSQL in the cloud**.

On a VPS, you control configuration, access, and backups. With a managed service, the provider handles some maintenance, but you need to check access to the system database `postgres` and permission to create objects in the `studiofs` database. The current `studiofs init` command uses these names.

Next, we will look at a simple deployment on a VPS using Docker Compose. This is enough to get started with a small project or test environment.

## In practice: PostgreSQL 18 with Docker Compose

You will need Ubuntu Server (or another Linux server), SSH access, and Docker Engine and Docker Compose installed. If Docker is not installed yet, follow the [official instructions for Ubuntu](<https://docs.docker.com/engine/install/ubuntu/>). We will not cover Docker installation here.

Create a directory for PostgreSQL:

```bash
mkdir -p ~/studiofs-postgres/pgdata
cd ~/studiofs-postgres
```

Create a `compose.yaml` file in it:

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

Docker automatically selects the appropriate `postgres:18-alpine` image for the server architecture, such as ARM64 or x86-64. There is nothing else you need to specify. The configuration above assumes a server with plenty of RAM. If your VPS has less than 8 GB of RAM, reduce `shared_buffers` and `maintenance_work_mem` first. You can adjust the other settings for your workload, too.

The `postgres:18-alpine` image is a compact version of PostgreSQL 18. On the first launch, it automatically creates the `studiofs` database and a `studiofs` user with the password specified in Compose. Note that a user created with `POSTGRES_USER` has PostgreSQL superuser privileges. The `studiofs` password in this example is for simplicity only; replace it for a real deployment.

The `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` values are used only when initializing an empty database for the first time. Changing them in Compose does not change users or databases that have already been created.

### What about the `pgdata` folder?

This is a regular directory next to `compose.yaml` where all PostgreSQL data will be stored. Because we **explicitly set** `PGDATA`, the bind mount points to this directory even though the default data path in the official PostgreSQL 18 image is different. This layout is suitable for our simple example; before upgrading to the next major PostgreSQL version, plan a separate data migration.

Docker usually sets permissions itself during the first launch. In the PostgreSQL Alpine image, the database process runs as **UID/GID 70:70**. If the log shows `Permission denied` for `pgdata`, you can stop the container and fix the owner on a regular Linux server with Docker Engine:

```bash
docker compose down
sudo chown -R 70:70 ./pgdata
```

Then start it again. You do not need to open the directory to everyone with `chmod 777`.

### Start it and check

```bash
docker compose up -d
docker compose ps
```

Wait until the container reaches the `healthy` status. If it does not start, inspect the log:

```bash
docker compose logs --tail=50 postgres
```

Let’s check that the required database is available and the password works over the Docker network:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d studiofs -W \
  -c "SELECT current_database(), current_user;"
```

Enter the `studiofs` user’s password when prompted. Both values in the response should be `studiofs`.

One more check: access to the system database used by the current `studiofs init` command:

```bash
docker compose run --rm --no-deps postgres \
  psql -h postgres -U studiofs -d postgres -W \
  -c "SELECT 1;"
```

We expect `1`. These checks run over the Docker network; we will check the connection from a workstation after configuring the VPN.

To stop the container, use `docker compose down`; to start it again, use `docker compose up -d`. The data remains in `./pgdata`.

**Do not delete the** `pgdata` **folder!** It stores the metadata for the entire file system. Without a backup, it may be impossible to restore the StudioFS volume.

## How will StudioFS connect to the database?

The connection uses a **DSN**—a string with the server address, user, password, and database name:

```text
postgresql://studiofs:studiofs@VPN_SERVER_ADDRESS:5432/studiofs
```

Replace `VPN_SERVER_ADDRESS` with the server’s private IP address in Tailscale or NetBird.

For this example, we use a VPN as the private transport. However, **Tailscale by itself does not close the public port**: the `5432:5432` mapping publishes it on every interface of the Docker host. Make sure port 5432 is blocked from the public internet by your VPS firewall. Configuring VPN access is the subject of the next chapter.

---

[Contents](README.md) · [Українська](../uk/07-postgresql-the-brain-behind-the-files.md) · [← Previous](06-s3-a-home-for-your-files.md) · [Next →](08-tailscale-netbird-a-private-path-to-postgresql.md)

[Linear source](https://linear.app/valandreev/document/07-postgresql-the-brain-behind-the-files-922bc87af337) · Source updated: 2026-10-19T20:16:35.184Z · Ukrainian original approved for translation.
