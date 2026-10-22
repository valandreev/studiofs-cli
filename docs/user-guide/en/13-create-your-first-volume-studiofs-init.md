# 13. Create Your First Volume — studiofs init

> **English translation draft; review pending.** This text has not yet been approved.

S3 already has room for your files, PostgreSQL is ready to remember their structure, and StudioFS is installed. Now we will bring these components together in a **volume**—a separate StudioFS drive with its own name, settings, and encryption key.

That is what `studiofs init` does. It creates a new volume, but does not yet attach it as a drive in your system. The drive letter or mount directory comes in the next step. We can plan the housewarming, but let us hold off on moving in the furniture.

## What needs to be ready

Run the command on the computer where you will start working with the volume, under your usual user account. Local metadata is created for that user; running through `sudo` may leave it in another user's home directory.

You need the S3 API address, a private bucket, a region, and dedicated access keys from [chapter 06](06-s3-a-home-for-your-files.md). You will also need a PostgreSQL DSN—a connection string containing the address, user, and password. The checks in [chapter 09](09-check-your-infrastructure.md) must pass from this particular computer.

Remote PostgreSQL must be reachable through a private route, such as Tailscale or NetBird. **Do not expose port 5432 to the entire internet.** Installing a VPN alone does not close a public port: check the port binding and the server's firewall rules.

Choose a short, clear name, such as `studio-projects`. This is the volume name, not the bucket name or its future drive letter. Latin letters, numbers, and hyphens are convenient to start with. Do not enter a path: `/`, `\`, and `:` are forbidden in the name. StudioFS will also use this name in the object prefix: `studio-projects/objects/`.

## The creation command

First, you can check the available options in your installed version:

```text
studiofs init --help
```

Below is a **template**: replace all uppercase values with your own. Addresses ending in `.invalid` deliberately do not point to real services. The command is shown on one line so you can adapt it for Bash, PowerShell, or another shell without different line continuation characters:

```text
studiofs init studio-projects --endpoint 'https://S3_API_HOST.invalid' --bucket 'YOUR_PRIVATE_BUCKET' --region 'YOUR_S3_REGION' --access-key 'YOUR_ACCESS_KEY_ID' --secret-key 'YOUR_SECRET_ACCESS_KEY' --meta 'postgresql://YOUR_PG_USER:YOUR_URL_ENCODED_PASSWORD@PRIVATE_PG_HOST.invalid:5432/studiofs?sslmode=require'
```

| Part of the command | What to supply |
| -- | -- |
| `studio-projects` | The new volume name. This is a positional argument, with no separate flag. |
| `--endpoint` | The S3 API address, not the control panel or an individual object. For remote storage, use `https://`. |
| `--bucket` | The exact name of the bucket for file contents. |
| `--region` | The region from your provider's settings. If omitted, it defaults to `us-east-1`; for cloud storage, specify it explicitly. |
| `--access-key` | The Access Key ID of your dedicated StudioFS key. |
| `--secret-key` | That key's Secret Access Key. |
| `--meta` | The PostgreSQL server DSN. This option is required. |

In the example, `PRIVATE_PG_HOST.invalid` stands for your server's private hostname. If you use TLS, the name must match the server certificate. `sslmode=require` requires TLS; the server must support it, and its certificate must pass trust and hostname validation. Omitting this parameter does not make TLS mandatory: the default mode allows a fallback to an unencrypted PostgreSQL connection if the server does not support TLS.

If you followed the earlier chapters with PostgreSQL without TLS inside a secure VPN, do not copy `sslmode=require` blindly. For a connection deliberately set up this way, you can explicitly specify `sslmode=disable`, keeping the server accessible only through the private network. The VPN protects the network route, while TLS protects the PostgreSQL connection itself; these are separate settings.

In a URL, a password containing characters such as `@`, `:`, `/`, or `#` must be correctly encoded as a URL component. Quotes around the DSN protect the string from the shell, but do not perform that encoding. A password written incorrectly can easily turn into a “strange server address.”

**Before inserting real secrets, take care of command history.** This template passes keys and the password as process arguments: they may remain in shell history or be visible to other users through process inspection tools. Use a trusted computer and configure history handling for your shell before running it. Do not save the completed command in a repository, chat, or screenshot. Substituting a variable may keep the secret out of the history text itself, but will not hide the expanded process argument.

## What happens after you run it

StudioFS first connects to the PostgreSQL maintenance database **`postgres`** and checks whether the **`studiofs`** database exists. If it does not, StudioFS tries to create it. The database name is fixed: putting another name in the DSN will not create a separate database for this volume—StudioFS rewrites it to the required name.

The user therefore needs access to `postgres` and, if `studiofs` does not exist yet, permission to create it. An administrator can help prepare the database in advance; in that case, database creation permission is not needed for this step. Within `studiofs` itself, you need permission to create the internal schema and work with its data.

Next, the command checks the S3 bucket. If the check fails, it tries to create the bucket. This may require the additional `CreateBucket` permission. A message about attempting creation **does not prove the bucket is missing**: incorrect keys, an endpoint, or permissions may have caused the initial failure. A bucket that has already been created and is accessible avoids the need for permission to create it.

After the checks, StudioFS prepares shared metadata, checks whether a volume with that name already exists, and creates the local database, root directory, and encryption key for the new volume. The initial state is published to PostgreSQL. Future file data will go into S3 under the volume's prefix; an empty volume is not expected to show familiar user folders there.

By default, the local database is at `~/.studiofs/meta/studio-projects/studio-projects.db`, where `~` is the user's home directory. If you have configured `STUDIOFS_CLIENT_ROOT`, that directory will be the root instead. The command shows the exact path at the end.

The settings contain credentials, and PostgreSQL also stores the volume's key material. Do not assume the local database or shared metadata is safe to publish simply because file contents are encrypted.

## How to tell whether it worked

Wait for the command to finish without an error. At the end, you should see:

```text
✓ Volume 'studio-projects' initialized
```

Alongside it will be the volume UUID, block size, endpoint and bucket, the label `Metadata backend: Postgres`, the volume ID, and the path to local metadata. Check that the name and bucket are the ones you intended. A successful PostgreSQL connectivity message alone is not enough: initialization continues after that.

You can also inspect the status:

```text
studiofs status studio-projects
```

Expect information about your volume, rather than a `Volume ... not found` message. This is a useful check of local state, but it does not yet prove that uploading and reading a file through S3 works. We will do that practical check after mounting.

## If you need to try again

`init` creates a new volume and **is not a safe command for “updating settings.”** After an error, first read it and establish which step the command reached.

* **S3 error:** check the endpoint, region, keys, and access to the existing bucket. Do not grant administrator permissions at random just because a message mentions creating it.
* **PostgreSQL error:** check the private route, password, TLS, and access to both databases—`postgres` and `studiofs`. A database creation error may mean an administrator needs to help prepare it in advance.
* **The volume already exists in PostgreSQL:** do not run `destroy`, even if the error text suggests it. This is not a routine retry step. If the volume already exists and you need to attach another computer, use `studiofs discover`, covered in [chapter 16](16-connect-a-second-computer-studiofs-discover.md).
* **The prompt `Do you want to CLEAR it and start fresh? [y/N]:`:** StudioFS has found a local metadata directory and is offering to delete it. Unless you are certain it contains only an unwanted remnant of a failed test, press Enter or type `n`. Answering `y` deletes the directory; it does not simply repeat the check.

Do not assume that every failed initialization automatically undoes all preceding actions. A database, bucket, local files, or a volume record may already have been created. If the final message is missing but the name is already taken or a clearing prompt appears, save a redacted copy of the error text and investigate the state before proceeding. Manually deleting metadata or creating another volume over the same prefix is a poor substitute for diagnosis.

Once the command has completed successfully, there is no need to repeat `init`. The volume is created; in the next chapter, we will attach it to your computer's file system.

---

[Contents](README.md) · [Українська](../uk/13-create-your-first-volume-studiofs-init.md) · [← Previous](12-install-studiofs-on-linux.md) · [Next →](14-mount-your-volume-studiofs-mount.md)

[Linear source](https://linear.app/valandreev/document/13-create-your-first-volume-studiofs-init-af054dd6f1d4)
