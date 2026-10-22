# 09. Check Your Infrastructure

> **English translation draft for editorial review.** Not yet approved.

The bucket is created, PostgreSQL is running, and the VPN shows a green indicator. Can we install StudioFS now? Almost. A green indicator is pleasant company, but it has no idea whether our database password is correct.

Before installing, we will check the whole path **from the workstation where we will actually work with files**. A successful connection from the VPS itself does not prove that the editing workstation can reach the same server. Better to find out now than during the first copy of footage from a shoot.

## First, addresses and routing

Gather the settings from the three previous chapters. For S3, you need the endpoint, bucket, region, and key pair; for PostgreSQL, the address, port, user, password, and an agreed TLS setup. Keep secrets in a password manager. An ordinary note only needs to say where to find them.

Make sure the VPN is connected and the workstation has a route to the database. For Tailscale, repeat:

```bash
tailscale status
tailscale ping 100.101.102.103
```

Here and below, `100.101.102.103` is the example from the previous chapter: replace it with your server's address. For NetBird, check the connection status and access to the required host in your client.

If you use a server name, test the connection **using that exact name**. Success with an IP address does not confirm that DNS is configured correctly on the workstation. For TLS, the name must also match the server's certificate.

You can check TCP port availability in PowerShell:

```powershell
Test-NetConnection 100.101.102.103 -Port 5432
```

On macOS or Linux, if `nc` is installed:

```bash
nc -vz 100.101.102.103 5432
```

We expect `TcpTestSucceeded : True` or a successful TCP connection message. If you do not get that, first check the VPN, route, firewall, and PostgreSQL port publishing. Changing the password will not open a closed port.

## PostgreSQL: an actual login, as well as a reachable port

For the next check, you need **the `psql` client on the workstation**. It is part of the PostgreSQL client tools; you do not need to install a local database server. If `psql` cannot be found, install the client or add its directory to `PATH`.

First, decide how the connection is protected. This matters both for the test and for the `--meta` setting we will use in StudioFS later.

For a server with TLS configured, use its actual DNS name and `sslmode=require`. In this mode, StudioFS requires TLS and validates the server certificate, including the hostname match. The current client uses a bundled set of trusted root certificates; a self-signed certificate or private certificate authority needs a separate compatibility check. Do not assume that adding a certificate to the operating system automatically configures StudioFS.

The simple Docker example in chapter 07 does not configure PostgreSQL TLS separately. If the entire path to the database runs through a protected private network and you deliberately rely on the VPN, you can explicitly choose `sslmode=disable`. This disables **PostgreSQL TLS**, not VPN encryption. Do not use this arrangement for an unprotected connection over the internet; if you reach the database through a gateway, account for the segment from the gateway to the database too.

If no mode is specified, the current StudioFS prefers TLS but can fall back to unencrypted transport when the server does not support TLS. So the absence of an error does not mean the connection is encrypted.

To test our VPN example, run this as a single command:

```bash
psql "host=100.101.102.103 port=5432 user=studiofs dbname=studiofs sslmode=disable connect_timeout=10" -W -c "SELECT current_database(), current_user;"
```

Enter the password when prompted: it will not appear in the command text. We expect the `studiofs` database and your username. If you changed the user in the earlier examples, the response will be different too.

For a TLS server, replace `host` with its DNS name and `sslmode=disable` with `sslmode=require`. Remember that `psql` and StudioFS use different TLS clients: a successful test does not yet confirm that StudioFS will trust the same certificate.

The current `studiofs init` also connects to the `postgres` maintenance database. Test it using the same routing and security settings:

```bash
psql "host=100.101.102.103 port=5432 user=studiofs dbname=postgres sslmode=disable connect_timeout=10" -W -c "SELECT 1;"
```

We expect `1`. If `studiofs` has not been created yet, the first command will report a missing database. An administrator can create it in advance; otherwise, the user running `init` needs permission to create a database. In an existing `studiofs` database, the user needs permission to create service objects and work with them afterward. Permission to connect alone is not enough: agree on these privileges with the administrator, especially for managed PostgreSQL.

Do not run `studiofs init` just to test a password. This command creates state for a new volume and may create a database and bucket. We will get to it after installing the application.

## S3: testing with a small file

Signing in to the provider's control panel confirms access to the panel. What we need is for **the dedicated StudioFS keys** to be able to list the bucket and write, read, and delete objects.

You can check this with an S3 client that supports your endpoint. Below is an option using **AWS CLI**, a separate application that also works with many S3-compatible services. If it is not installed yet, follow the [AWS CLI installation instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html). This is a helper tool for testing, not a StudioFS dependency.

Create a dedicated profile:

```bash
aws configure --profile studiofs-preflight
```

At the prompts, enter the Access Key ID, Secret Access Key, and region from the provider's control panel. You can leave the output format at its default. **AWS CLI saves the keys in local configuration and credentials files**: use your own operating system account and do not add these files to a project or send them in conversations.

In the examples below, replace the endpoint, bucket, and region with your own values. Use the S3 API endpoint, rather than the web control panel address. Remote storage requires HTTPS.

```bash
aws --profile studiofs-preflight --endpoint-url https://s3.example.com --region us-east-1 s3api list-objects-v2 --bucket studiofs-projects-demo --max-keys 1
```

A successful response without an access error is a good start; an empty bucket is fine too. But listing does not test writing.

In your current local directory, create a small text file named `studiofs-connection-test.txt` containing any recognizable phrase, with no private data. Choose **a new, unused key outside the volume directories** for the test object. For example, replace `UNIQUE-ID` in all the following commands with a unique value of your own. Writing to an existing key may replace someone else's object.

```bash
aws --profile studiofs-preflight --endpoint-url https://s3.example.com --region us-east-1 s3api put-object --bucket studiofs-projects-demo --key _preflight/UNIQUE-ID/connection.txt --body studiofs-connection-test.txt
```

Now download it again under a different local name:

```bash
aws --profile studiofs-preflight --endpoint-url https://s3.example.com --region us-east-1 s3api get-object --bucket studiofs-projects-demo --key _preflight/UNIQUE-ID/connection.txt studiofs-connection-test-downloaded.txt
```

Open the downloaded file and check the phrase. Finally, remove **only the test object you created**:

```bash
aws --profile studiofs-preflight --endpoint-url https://s3.example.com --region us-east-1 s3api delete-object --bucket studiofs-projects-demo --key _preflight/UNIQUE-ID/connection.txt
```

If needed, repeat the listing with `--prefix _preflight/UNIQUE-ID/` to check the result. In a versioned bucket, deletion may leave the previous version behind: that is storage behavior, not a StudioFS error.

All four operations must succeed with the same keys. If the access policy restricts permitted prefixes, success under `_preflight/` does not prove access to the future `<volume-name>/objects/`. Agree with the administrator on permissions for the actual StudioFS working prefix; do not fix an access denial by making the bucket public.

## If a check fails

A name resolution error usually points us toward DNS or an incorrect address. A connection timeout points toward routing, the firewall, or an unavailable service. A PostgreSQL authentication failure points toward the user, password, and server access rules; a missing database message points toward creating it.

For S3, `AccessDenied` means you need to check the keys and access policy. Request signature errors may call for checking the region, endpoint, secret key, and system time. A certificate error calls for checking the address and certificate trust, rather than disabling protection at random.

When asking for help, share the name of the failed operation and a sanitized error message. Do not send the secret key, password, or a full DSN containing a password.

## What to keep before installation

Instead of screenshots containing secrets, save a short working note:

| What to record | Example or explanation |
| --- | --- |
| S3 endpoint, bucket, region | Exact values from the provider's control panel |
| Where the keys are stored | Entry name in the password manager |
| PostgreSQL host, port, user | Private address or DNS name and the agreed user |
| Databases | Access to `postgres`, preparation status of `studiofs` |
| Connection protection | TLS with `sslmode=require` or a deliberately chosen VPN setup |
| Test results | Which workstation passed the S3 operations and SQL queries, and when |

The public PostgreSQL port must remain closed as configured in the previous chapter. For S3, check that the bucket is private and that automatic deletion rules will not remove objects belonging to the future volume.

Now we know the workstation can reach both services and has the required credentials. This is not yet a mounted drive test: we will do that after installation and volume creation, using small files before moving a real project. Next, choose the chapter for your system: [Windows](10-install-studiofs-on-windows.md), [macOS](11-install-studiofs-on-macos.md), or [Linux](12-install-studiofs-on-linux.md).

---

[Contents](README.md) · [Українська](../uk/09-check-your-infrastructure.md) · [← Previous](08-tailscale-netbird-a-private-path-to-postgresql.md) · [Next →](10-install-studiofs-on-windows.md)

[Linear source](https://linear.app/valandreev/document/09-check-your-infrastructure-de24b263ec39)
