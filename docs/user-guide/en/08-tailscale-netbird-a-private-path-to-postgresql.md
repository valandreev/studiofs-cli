# 08. Tailscale: a private path to PostgreSQL
> **Work in progress**

Imagine someone on your team is working from home, someone else is in the studio, and the PostgreSQL server is in a data center. All these computers have internet access. Does that mean PostgreSQL should be accessible to the entire internet, too?

Fortunately, no. And it is better not to try.

In the previous section, we prepared PostgreSQL—the place where StudioFS stores shared metadata. Now we need to give workstations access to the database without inviting the rest of the world in. To do this, we will create a **private network with Tailscale** (or NetBird).

## Why do we need a VPN if we have a password?

PostgreSQL usually works over TCP port `5432`. You could open it on the server, add a password, and hope nobody else is interested in your database. But automated scanners will find an open port, and a mistake in the access settings can be very costly.

A **VPN (Virtual Private Network)** takes a different approach: computers get addresses on a separate private network and can communicate over an encrypted connection. For StudioFS, it looks like this:

```text
Workstation A ─────────┐
                        ├── Tailscale ── PostgreSQL :5432
Workstation B ─────────┘

Workstations ───────────── HTTPS ─────── S3 storage
```

There is an important detail here: **the VPN is primarily for private access to PostgreSQL**. StudioFS clients usually get video file contents directly from S3 over HTTPS, rather than “pumping” them through the database server.

Tailscale and NetBird build these networks on WireGuard. They handle tunnel setup, device discovery, and working on networks with NAT. We will not have to connect every team member’s IP addresses by hand—not bad for a task with the word “network” in its name.

## Choose your route

For the first setup, we recommend **Tailscale**: it has a simple client, an easy-to-understand control panel, and convenient diagnostic commands. **NetBird** is a full-fledged alternative, but historically the StudioFS developers have used Tailscale.

Below we will cover the main scenario: PostgreSQL runs on your Linux server or VPS, and you can connect to it over SSH. Workstations may run Windows, macOS, or Linux.

## Set up Tailscale

### 1. Add the server to your private network

Sign up for [Tailscale](<https://tailscale.com/>) and create your own private network, called a *tailnet*. Then install Tailscale on the server running PostgreSQL.

For common Linux distributions, the official instructions suggest:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

The first command downloads and runs the Tailscale installation script. If you do not want to run a remote script directly, follow the installation instructions for your distribution’s package manager: [Tailscale for Linux](<https://tailscale.com/docs/install/linux>).

The second command connects the server to your tailnet. Tailscale may display a link that you need to open in a browser to confirm sign-in.

After authorizing, run:

```bash
tailscale status
tailscale ip -4
```

The first command shows connected devices; the second shows **this server’s** IPv4 address in Tailscale. For example:

```text
100.101.102.103
```

Also check that the server appears on the **Machines** page in the Tailscale control panel.

### 2. Connect the workstations

On Windows, install [Tailscale for Windows](<https://tailscale.com/docs/install/windows>); on macOS, install [Tailscale for macOS](<https://tailscale.com/docs/install/mac>). For Linux, use the same instructions as for the server.

Sign in to the **same tailnet**. This does not necessarily mean using one shared account for the whole team: an administrator can invite individual users. What matters is that their devices can access the required private network.

Then check from a workstation:

```bash
tailscale status
tailscale ping 100.101.102.103
```

Replace the example address with your server’s actual VPN address. A successful `tailscale ping` means the devices can exchange traffic through Tailscale.

## Check PostgreSQL access

Now we need to make sure that the private network gives access not only to the server, but also to PostgreSQL’s TCP port.

First, check the connection again from a workstation:

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

On Windows, look for `TcpTestSucceeded : True`; on macOS or Linux, look for a message saying the TCP connection succeeded. This means the port is reachable, **but does not yet confirm the PostgreSQL username, password, or access permissions are correct**.

Finally, check that the VPS’s **public IP address does not accept connections on port 5432**. Review the firewall rules at your provider and on the server, and, if possible, check the port from a device **outside your VPN**. One failed test is not a substitute for checking the network rules.

## If PostgreSQL runs in a managed cloud service

Sometimes PostgreSQL runs not on your VPS but in Amazon RDS, Google Cloud SQL, or another provider’s service. You do not have access to its operating system, so you cannot simply install Tailscale on it.

A **private network gateway** can help: it is a small virtual machine in the same private network as the database.

```text
Workstation
     │
  Tailscale
     │
Gateway in the provider's private network
     │
Private IP address of PostgreSQL :5432
```

The gateway does not store StudioFS files or replace PostgreSQL. It only forwards permitted network traffic.

For **Tailscale**, use a *subnet router*: install Tailscale on the gateway, enable IP forwarding, advertise a route to the database’s private IP address (preferably just one host, `/32`), approve the route in the **Machines** control panel, and allow the required clients through your tailnet access rules.

On Linux clients, you may also need to accept routes:

```bash
sudo tailscale set --accept-routes=true
```

The gateway must **have private access to the database itself**, and the provider’s network rules must allow connections over TCP port 5432 **from the gateway**, not from the entire internet.

With typical routing that uses source NAT (SNAT/masquerade), PostgreSQL sees the gateway’s IP address rather than the individual laptop’s VPN address. Account for this in the provider’s address allowlist and PostgreSQL access rules.

This scenario is more complicated than installing a VPN directly on your own server. Follow your cloud provider’s instructions for private networks, as well as the [Tailscale subnet routers guide](<https://tailscale.com/docs/features/subnet-routers>).

You do not need to route all internet traffic through the gateway: to access PostgreSQL, a route to its private address is enough.

## What have we accomplished?

Now a workstation has a private route to PostgreSQL. You know the server’s VPN address, checked that the required port is reachable, and confirmed that public TCP port 5432 remains closed.

In the next section, we will check all the prepared infrastructure together and then finally move on to installing StudioFS.

---

[Contents](README.md) · [Українська](../uk/08-tailscale-netbird-a-private-path-to-postgresql.md) · [← Previous](07-postgresql-the-brain-behind-the-files.md) · [Next →](09-check-your-infrastructure.md)

[Linear source](https://linear.app/valandreev/document/08-tailscale-netbird-a-private-path-to-postgresql-165dab39a508) · Source updated: 2026-10-19T21:08:26.630Z · Ukrainian original approved for translation.
