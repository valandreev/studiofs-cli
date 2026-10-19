# 08. Tailscale: A Private Path to PostgreSQL

Imagine some team members working from home, others in a studio, and the PostgreSQL server sitting in a data center. All these computers have internet access. Does that mean PostgreSQL needs to be available to the entire internet too?

Fortunately, no. It is better not to try.

In the previous chapter, we prepared PostgreSQL—the place where StudioFS stores shared metadata. Now we need to give workstations access to that database without inviting the rest of the world in. To do that, we will create **a private network using Tailscale** (or NetBird).

## Why do we need a VPN if we have a password?

PostgreSQL usually runs on TCP port `5432`. You could open it on the server, add a password, and hope nobody else takes an interest in your database. But automated scanners will find the open port, and an access configuration mistake can be very expensive.

A **VPN (Virtual Private Network)** offers a different approach: computers receive addresses on a separate private network and can communicate over an encrypted connection. For StudioFS, it looks like this:

```text
Workstation A ──┐
               ├── Tailscale ── PostgreSQL :5432
Workstation B ──┘

Workstations ────── HTTPS ──────── S3 storage
```

There is an important detail here: **the VPN is primarily for private access to PostgreSQL**. StudioFS clients usually fetch video contents directly from S3 over HTTPS, rather than routing them through the database server.

Tailscale and NetBird build these networks on WireGuard. They handle tunnel setup, device discovery, and operation on networks with NAT. We will not have to manually connect every participant's IP addresses—not bad for a task with “network” in its name.

## Choosing your route

For a first setup, we recommend **Tailscale**: it has a simple client, a clear administration console, and convenient diagnostic commands. **NetBird** is a fully fledged alternative, but historically the StudioFS developers have used Tailscale.

Below, we cover the main scenario: PostgreSQL runs on your Linux server or VPS, and you can connect to it over SSH. Workstations can run Windows, macOS, or Linux.

## Setting up Tailscale

### 1. Adding the server to the private network

Sign up for [Tailscale](https://tailscale.com/) and create your own private network—a *tailnet*. Then install Tailscale on the PostgreSQL server.

For common Linux distributions, the official instructions offer:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

The first command downloads and runs Tailscale's installation script. If you prefer not to execute a remote script directly, use the installation instructions for your distribution's package manager: [Tailscale for Linux](https://tailscale.com/docs/install/linux).

The second command connects the server to your tailnet. Tailscale may display a link you need to open in a browser to confirm sign-in.

After authorization, run:

```bash
tailscale status
tailscale ip -4
```

The first command shows connected devices; the second shows **this server's** Tailscale IPv4 address. For example:

```text
100.101.102.103
```

Also check that the server appears on the **Machines** page in the Tailscale administration console.

### 2. Connecting workstations

On Windows, install [Tailscale for Windows](https://tailscale.com/docs/install/windows); on macOS, install [Tailscale for macOS](https://tailscale.com/docs/install/mac). For Linux, follow the same instructions as for the server.

Sign in to **the same tailnet**. This does not necessarily mean sharing one account across the team: an administrator can invite individual users. What matters is that their devices have access to the required private network.

Then check from the workstation:

```bash
tailscale status
tailscale ping 100.101.102.103
```

Replace the example address with your server's actual VPN address. A successful `tailscale ping` means the devices can exchange traffic through Tailscale.

## Checking PostgreSQL availability

Now we need to make sure that the private network provides access to PostgreSQL's TCP port, as well as to the server itself.

First, check connectivity from the workstation again:

```bash
tailscale ping 100.101.102.103
```

Then check the TCP port. In **PowerShell on Windows**:

```powershell
Test-NetConnection 100.101.102.103 -Port 5432
```

On **macOS or Linux**, if `nc` is installed:

```bash
nc -vz 100.101.102.103 5432
```

On Windows, look for `TcpTestSucceeded : True`; on macOS or Linux, look for a successful TCP connection message. This means the port is reachable, **but does not yet confirm that the PostgreSQL username, password, or access permissions are correct**.

Finally, check that **the VPS's public IP address does not accept connections on port 5432**. Review the provider's and server's firewall rules and, if possible, test port availability from a device **outside your VPN**. One failed test does not replace reviewing network rules.

## If PostgreSQL runs in a managed cloud service

Sometimes PostgreSQL runs in Amazon RDS, Google Cloud SQL, or another provider's service rather than on your VPS. You do not have access to its operating system and cannot simply install Tailscale on it.

This is where **a private network gateway** helps—a small virtual machine on the same private network as the database.

```text
Workstation
    │
 Tailscale
    │
Gateway in the provider's private network
    │
PostgreSQL private IP address :5432
```

The gateway does not store StudioFS files or replace PostgreSQL. It only forwards permitted network traffic.

For **Tailscale**, use a *subnet router*: install Tailscale on the gateway, enable IP forwarding, advertise a route to the database's private IP address (preferably to a single host only, `/32`), approve the route in the **Machines** console, and allow the required clients access through tailnet rules.

On Linux clients, accepting routes may also be necessary:

```bash
sudo tailscale set --accept-routes=true
```

The gateway **must itself have private access to the database**, and the provider's network rules must allow connections to TCP port 5432 **from the gateway**, rather than from the entire internet.

With typical routing using address translation (SNAT/masquerade), PostgreSQL sees the gateway's IP address rather than an individual laptop's VPN address. Account for this in the provider's allowlist and PostgreSQL access rules.

This scenario is more complex than installing the VPN directly on your own server. Follow your cloud provider's private networking instructions and the [Tailscale subnet routers guide](https://tailscale.com/docs/features/subnet-routers).

There is no need to route all internet traffic through the gateway: a route to PostgreSQL's private address is enough to reach it.

## What do we have after this chapter?

The workstation now has a private route to PostgreSQL. You know the server's VPN address, have checked the required port's availability, and have made sure public TCP port 5432 stays closed.

In the next chapter, we will check all the prepared infrastructure together, then finally move on to installing StudioFS.

---

[Contents](README.md) · [Українська](../uk/08-tailscale-netbird-a-private-path-to-postgresql.md) · [← Previous](07-postgresql-the-brain-behind-the-files.md) · [Next →](09-check-your-infrastructure.md)

[Linear source](https://linear.app/valandreev/document/08-tailscale-netbird-a-private-path-to-postgresql-165dab39a508) · Source updated: 2026-10-19T21:08:26.630Z · English translation; review pending.
