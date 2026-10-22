# StudioFS in 20 seconds

**StudioFS is an open-source, self-hosted alternative to LucidLink for collaborating on media projects. Whether your team is in one office or spread across continents, you can work with shared files on Windows, macOS, and Linux as if they were on a regular drive. You do not need to download terabytes of footage in advance: StudioFS reads the parts you need from S3, caches them locally, and syncs changes between computers in the background.**

*“We'll build our own LucidLink — with blackjack and hookers!”*

*A hello to Bender from Futurama. He was going to build his own amusement park. We decided to start with a file system.*

---

# 01. Welcome to StudioFS!
> **Work in progress**

Imagine your team is working on one film, but everyone is in a different place. The editor is at home, the colorist is in the studio, and the VFX artist is drunk on a beach in Bali. Meanwhile, there are several terabytes of footage: camera originals, proxies, EXRs, 3D scenes, renders.

Usually, you have to download files, copy folders, send new versions around, and figure out who has the latest project. It gets especially fun when someone is already working from an old copy.

**StudioFS aims to give the whole team one shared file space**, wherever everyone happens to be.

## What does that look like for you?

Connect StudioFS and you will see a regular drive in Windows, or a folder on macOS or Linux. Open your media in DaVinci Resolve, Flame, or another application. It works the way you are used to.

You do not have to download large files in full. StudioFS fetches the parts you need from storage and keeps them in a local cache. Changes are saved on your computer first, then sent to the cloud and made available to your colleagues after synchronization.

So **a shared drive, yes; instant synchronization, no**. And if two people change the same file at the same time, StudioFS cannot automatically merge their edits. The good news is that a well-designed media pipeline helps avoid these situations.

## Why not just use LucidLink?

LucidLink inspired this idea: remote files presented as a local drive, convenient for media production. But we wanted **an open system that could be deployed entirely on our own infrastructure**.

That is why StudioFS uses S3-compatible storage for file contents, PostgreSQL for shared metadata, and a local SSD for the cache. You choose and control the storage and server.

StudioFS is not yet a complete replacement for LucidLink in features or maturity. It is under active development, so for now, do not trust important projects to it without independent backups. Still, by the time we opened the repository to everyone, we had already completed a couple of real projects with StudioFS. So, it works.

## What’s next?

In the next chapter, we will briefly look at how S3, PostgreSQL, and the cache come together as one drive. Then we will set everything up in practice.

---

[Contents](README.md) · [Українська](../uk/01-welcome-to-studiofs.md) · [Next →](02-how-does-this-thing-work.md)
