# StudioFS Quick Start

[Guide contents](README.md) · [Українська](../uk/quick-start.md)

This page is the shortest path from an empty server to shared drives for your team. Explanations are kept to a minimum: if a step is unclear or something goes wrong, open the matching chapter of the [full guide](README.md).

## Data Here, Metadata There

StudioFS splits your drive into two parts and keeps them in different places:

- **Data** is the content of your files: video, audio, images, projects. It lives in **S3 storage** as encrypted pieces with technical names. If you open the bucket, you will not find familiar folders or a file called `interview.mov`.
- **Metadata** is everything that turns those pieces into a drive: file names, folder structure, sizes, dates, which pieces make up each file, and the volume's encryption key. The shared copy of the metadata is stored in **PostgreSQL**.

Every workstation keeps its own local copy of the metadata and a cache of the data it needs, so browsing folders and reopening files does not wait for the server. Changes are accepted locally first and then sent in the background: content to S3, metadata to PostgreSQL. Other computers pick them up from there.

Hence the main rule: **S3 and PostgreSQL only work as a pair.** Without metadata, the objects in S3 cannot be turned back into files, and without S3 the metadata is just a list of names. Back up both.

## What You Will Have at the End

Two StudioFS drives sharing one metadata database:

- **`homesilo`**: data in your own **RustFS** storage behind **Caddy** on your server.
- **`cloudsilo`**: data in **Backblaze B2**.

```text
                       ┌── Tailscale ── PostgreSQL :5432        (metadata for both drives)
Workstations ──────────┼── HTTPS ────── RustFS + Caddy          (homesilo data)
                       └── HTTPS ────── Backblaze B2            (cloudsilo data)
```

If you only need one of the drives, skip the steps for the other.

## What You Need to Start

- **A server for PostgreSQL**: a Linux server or VPS with Docker and SSH access.
- **A server for RustFS**: a Linux server with Docker, enough disk space, a domain, and ports 80/443 open. It can be the same server as PostgreSQL.
- **A Backblaze account** for the `cloudsilo` drive.
- **Client computers**: one or more Windows, macOS, or Linux computers where people will work with files. Free SSD space for the cache is recommended.
- **A Tailscale account** for private access from every computer to PostgreSQL.

## 1. Tailscale on the Server

Sign up for [Tailscale](https://tailscale.com/) and run on the server:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
tailscale ip -4
```

Write down the address, something like `100.101.102.103`; below it is referred to as `PG_TAILSCALE_IP`. Details: [chapter 08](08-tailscale-netbird-a-private-path-to-postgresql.md).

## 2. PostgreSQL

```bash
mkdir -p ~/studiofs-postgres/pgdata && cd ~/studiofs-postgres
```

Create `compose.yaml` (replace the password with your own):

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
docker compose ps        # wait for the healthy status
```

**Close port 5432 to the internet** in your VPS provider's firewall.

Performance tuning and backups: [chapter 07](07-postgresql-the-brain-behind-the-files.md). **Do not delete `pgdata`**: it holds the metadata of every drive.

## 3. Storage for homesilo: RustFS + Caddy

You need a domain, for example `s3.example.com`, with an A record pointing to the server's public IP address, and ports 80 and 443 open. Without a domain you can also reach RustFS at `IP:port`, for example `http://203.0.113.10:9000`, but it is less convenient: the connection has no HTTPS, and if the server's IP address changes, the endpoint has to be updated on every computer. RustFS and Caddy run from a single `compose.yaml`: Caddy accepts HTTPS connections and obtains and renews the Let's Encrypt certificate by itself, while RustFS is not published to the outside at all. All data lives in folders next to `compose.yaml`.

```bash
mkdir -p ~/rustfs && cd ~/rustfs
mkdir -p rustfs-data caddy-data caddy-config
sudo chown -R 10001:10001 rustfs-data   # RustFS runs as UID/GID 10001 inside the container
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
      - "127.0.0.1:9001:9001"   # web console, SSH tunnel only
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

Next to it, create a `Caddyfile` with your own domain:

```caddyfile
s3.example.com {
    reverse_proxy rustfs:9000
}
```

```bash
docker compose up -d
docker compose logs -f caddy   # wait for the certificate message, exit with Ctrl+C
```

Nothing else needs configuring: by default Caddy does not limit request size, streams data, and keeps the `Host` header intact, which S3 request signatures depend on. Check: `curl -I https://s3.example.com/` should return a response from RustFS (usually `403` without a signature, which is fine), not a connection or certificate error.

What lives where:

| Folder | Contents |
| -- | -- |
| `rustfs-data/` | homesilo data. To keep it on a disk array, make this folder the array's mount point or a symbolic link to it |
| `caddy-data/` | Let's Encrypt certificates. Do not delete it, or Caddy will request them again |
| `caddy-config/` | Caddy's internal configuration |

`rustfs/rustfs:1.0.3` is the current stable RustFS release at the time of writing; check the [releases page](https://github.com/rustfs/rustfs/releases) and use a newer one if needed.

### Bucket, Policy and Key

Open the RustFS web console through an SSH tunnel instead of exposing it to the internet:

```bash
ssh -L 9001:127.0.0.1:9001 user@your-server
```

Then open `http://127.0.0.1:9001` in a browser and sign in with the admin keys from `compose.yaml`. StudioFS does not use the admin keys: we create a separate user with bucket and object permissions only.

1. Create a private bucket `homesilo`. You can skip this step: thanks to the policy below, `studiofs init` creates the bucket itself.
2. In the **Identity** section, create a policy `studiofs` with this JSON:

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

   The first block allows seeing and creating buckets and listing the objects in them. The second allows reading, writing, and deleting objects, including in large parts (multipart). The policy applies to **any bucket** on this RustFS, so additional drives need no reconfiguration. It does not allow deleting buckets. Keep only StudioFS data on this RustFS, or use a separate instance for other work.
3. In the same section, create a user `studiofs` with a strong secret key and assign it the `studiofs` policy. The user name is its Access Key, and the secret key is its Secret Key.

Values for `init`:

| Parameter | Value |
| -- | -- |
| `--endpoint` | `https://s3.example.com` |
| `--bucket` | `homesilo` |
| `--region` | `us-east-1` |
| `--access-key` | `studiofs` |
| `--secret-key` | that user's secret key |

## 4. Storage for cloudsilo: Backblaze B2

1. In the Backblaze dashboard, create a **Private** bucket. B2 bucket names are globally unique, so add something of your own, for example `cloudsilo-yourstudio`. Do not enable lifecycle rules for automatic deletion.
2. In the bucket settings, find the **Endpoint**, for example `s3.us-west-004.backblazeb2.com`. The part between `s3.` and `.backblazeb2.com` is the region.
3. Under **Application Keys**, create a key restricted to **this bucket** with **Read and Write** access. Save the `keyID` and `applicationKey` right away: the secret is shown only once.

| Parameter | Value |
| -- | -- |
| `--endpoint` | `https://s3.us-west-004.backblazeb2.com` |
| `--bucket` | `cloudsilo-yourstudio` |
| `--region` | `us-west-004` |
| `--access-key` | `keyID` |
| `--secret-key` | `applicationKey` |

## 5. The First Computer

### Tailscale and Database Access

Install Tailscale ([Windows](https://tailscale.com/docs/install/windows), [macOS](https://tailscale.com/docs/install/mac), [Linux](https://tailscale.com/docs/install/linux)) and sign in to the same tailnet. Check the port:

```powershell
# Windows
Test-NetConnection 100.101.102.103 -Port 5432
```

```bash
# macOS / Linux
nc -vz 100.101.102.103 5432
```

### Installing StudioFS

| System | Driver | Details |
| -- | -- | -- |
| Windows x64 | [WinFSP 2026+](https://github.com/winfsp/winfsp/releases) | [chapter 10](10-install-studiofs-on-windows.md) |
| macOS | [macFUSE 5.4.0+](https://macfuse.github.io/) (requires allowing the system extension and a restart) | [chapter 11](11-install-studiofs-on-macos.md) |
| Linux | `fuse3` from your distribution's repository | [chapter 12](12-install-studiofs-on-linux.md) |

Download the build for your system from the [releases page](https://github.com/valandreev/studiofs-cli/releases), put `studiofs` on your `PATH`, and check:

```bash
studiofs --version
```

On macOS and Linux, prepare the parent folder for mounting:

```bash
# macOS
sudo mkdir -p /Volumes/Studiofs && sudo chown "$(id -un):$(id -gn)" /Volumes/Studiofs
# Linux
sudo mkdir -p /mnt/StudioFS && sudo chown "$(id -un):$(id -gn)" /mnt/StudioFS
```

### Creating the Drives

Run as your regular user, **without `sudo`**. Substitute your values from steps 3 and 4 and pick the block for your shell. Both drives use the same PostgreSQL.

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

Success looks like these lines:

```text
✓ Volume 'homesilo' initialized
✓ Volume 'cloudsilo' initialized
```

If `init` fails, **do not rerun it blindly and do not agree to `CLEAR`**: read the error first ([chapter 13](13-create-your-first-volume-studiofs-init.md)).

### Mounting

Each drive is mounted with its own command. For everyday work add `-d` (background mode); without it each command occupies its own terminal window, but errors are visible right away.

```powershell
# Windows: free drive letters
studiofs mount homesilo H: -d
studiofs mount cloudsilo K: -d
```

```bash
# macOS → /Volumes/Studiofs/homesilo and /Volumes/Studiofs/cloudsilo
studiofs mount homesilo /Volumes/Studiofs -d
studiofs mount cloudsilo /Volumes/Studiofs -d
# Linux → /mnt/StudioFS/homesilo and /mnt/StudioFS/cloudsilo
studiofs mount homesilo /mnt/StudioFS -d
studiofs mount cloudsilo /mnt/StudioFS -d
```

Create a test folder on each drive, make sure it appears, and delete it.

Unmounting: `studiofs umount H:` / `studiofs umount /Volumes/Studiofs/homesilo` / `studiofs umount /mnt/StudioFS/homesilo`, and the same for `cloudsilo`. Details: chapters [14](14-mount-your-volume-studiofs-mount.md) and [17](17-unplug-safely-studiofs-umount.md).

## 6. The Other Computers

On each new computer: Tailscale plus the port 5432 check, the driver, and `studiofs`, as in step 5. **No `init` is needed**: the drives already exist.

**You do not have to add a colleague's computer to your tailnet.** Sharing just the PostgreSQL server with them is enough:

1. In the Tailscale admin console, on the **Machines** page, open the PostgreSQL server's menu and choose **Share**.
2. Send your colleague the invite link or invite them by email.
3. Your colleague installs Tailscale, signs in to **their own** account, and accepts the invite.

The server then appears in their network with the same `100.101.102.103` address. Your colleague sees only this server, not the rest of your devices, and you can revoke access at any time. The port 5432 check is the same as in step 5.

```text
studiofs discover homesilo --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
studiofs discover cloudsilo --meta 'postgresql://studiofs:YOUR_PASSWORD@100.101.102.103:5432/studiofs'
```

`discover` downloads only the metadata; the S3 keys come from the drive's configuration, so you do not need to enter them. Then use the same `mount` as on the first computer:

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

The first mount on a new computer can take a little while: StudioFS pulls in all of the drive's metadata. The files themselves are not downloaded; their content comes from S3 when you open them.

**Check both directions:** create `hello-from-a.txt` on the first computer and open its content on the second, then the other way round. A small file should appear almost immediately.

Run `discover` and `mount` as the same user: the local state lives in `~/.studiofs`. You do not need to rerun `discover` after a restart; `mount` is enough. Details: [chapter 16](16-connect-a-second-computer-studiofs-discover.md).

## What Next

- Backups: PostgreSQL **and** S3 together, see [chapters 07](07-postgresql-the-brain-behind-the-files.md) and [24](24-snapshots-a-time-machine-for-your-volume.md).
- How sync and the cache work: [chapters 19](19-sync-bad-wi-fi-and-other-surprises.md) and [20](20-cache-why-is-the-cloud-using-my-ssd.md).
- Something is not working: [chapter 28](28-troubleshooting-when-the-cat-stops-purring.md).

---

[Contents](README.md) · [Українська](../uk/quick-start.md)
