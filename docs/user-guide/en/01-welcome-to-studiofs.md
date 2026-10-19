# StudioFS in 20 Seconds

**StudioFS is an open-source, self-hosted alternative to LucidLink for collaborating on media projects. One office or different continents—it makes no difference.** On Windows, macOS, and Linux, you can work with shared files as if they were on a regular drive. No need to download terabytes of footage first: StudioFS reads the parts of files it needs from S3, caches them locally, and synchronizes changes between computers in the background.

*“We'll build our own LucidLink — with blackjack and hookers!”*

*A nod to Bender from Futurama. He was going to build his own theme park. We decided to start with a file system.*

---

# 01. Welcome to StudioFS!

Imagine your team working on the same film, with everyone in a different place. The editor is at home, the colorist is in the studio, and the VFX artist is drunk on a beach in Bali. There are several terabytes of material: originals, proxies, EXRs, 3D scenes, and renders.

Usually, that means downloading files, copying folders, sending new versions around, and figuring out who has the current project. It gets especially fun when someone is already working from an old copy.

**The idea behind StudioFS is to give the whole team one shared file space**, wherever everyone happens to be.

## How does it work for you?

Connect StudioFS, and you see a regular drive in Windows or a folder on macOS or Linux. Open your media in DaVinci Resolve, Flame, or another application. Everything works the way you are used to.

Large files do not have to be downloaded in full. StudioFS fetches the parts it needs from storage and keeps them in a local cache. Changes are saved on your computer first, then sent to the cloud and made available to colleagues after synchronization.

So, **a shared drive—yes; instant synchronization—no**. And if two people change the same file at the same time, StudioFS cannot automatically merge their edits. The good news is that a well-designed media pipeline helps avoid these situations.

## Why not just LucidLink?

LucidLink inspired the idea in the first place: remote files presented as a local drive, convenient for media production. But we wanted **an open system that could be deployed entirely on our own infrastructure**.

That is why StudioFS uses S3-compatible storage for file contents, PostgreSQL for shared metadata, and a local SSD for caching. You choose and control the storage and server.

It is not yet a complete replacement for LucidLink in features or maturity. StudioFS is under active development, so for now, it is better not to entrust important projects to it without independent backups. Still, by the time we opened the repository to everyone, we had already completed a couple of real projects using StudioFS. So yes, it works.

## What is next?

In the next chapter, we will briefly explain how S3, PostgreSQL, and the cache become a single drive. Then we will set it all up in practice.

---

[Contents](README.md) · [Українська](../uk/01-welcome-to-studiofs.md) · [Next →](02-how-does-this-thing-work.md)

[Linear source](https://linear.app/valandreev/document/01-welcome-to-studiofs-d3339c52ea1d) · Source updated: 2026-10-17T20:17:53.593Z · English translation; review pending.
