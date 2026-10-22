# 18. Working with Real Creative Projects

Your test file already opens on the second computer. Now you want to connect the footage for a music video, a Blender scene, or a Nuke shot and get some work done. Start with a copy of a small project: a deadline is a poor time to introduce a new drive to your pipeline.

StudioFS gives applications file access to shared assets. But that does not guarantee that every version of DaVinci Resolve, Nuke, or Blender will behave the same way on every platform. Test your particular application, plugins, and workflow. The approaches below are a cautious starting point, not a certification of compatibility.

## What goes on the volume, and what stays local?

A creative project usually contains several different kinds of data. Call all of it simply “the project folder,” and you can easily send three terabytes of temporary cache to the cloud by accident.

| Data | Where to start |
| -- | -- |
| Source video, audio, images, textures, geometry | Shared folders on StudioFS; keep independent backups of the originals |
| Scene and compositing files, project exports | Separate versions on the volume, with one person responsible for editing each file |
| The application's working project database | Local storage or a server option supported by the application itself |
| Temporary files, preview cache, intermediate renders | A local SSD, with a separate folder for each workstation |
| Proxies and finished results for handoff | On the volume after checking them; you can initially generate them locally |

**An application's project database is not the same thing as its media files.** Do not move a working database to StudioFS just because the application lets you choose a folder. For collaboration through a database, use a mechanism supported by the application itself. StudioFS's PostgreSQL stores file-system metadata; it does not automatically become a Resolve project server.

The application's own cache is also different from the StudioFS cache. The former may contain decoded data, simulation results, or previews; the latter holds local file-system data. Allow space for both and for locally generated results. Do not manually clear the StudioFS cache as though it were an ordinary folder of temporary renders.

## DaVinci Resolve: keep media and the project separate

For your first test, leave the Resolve project library in a location you already trust, and put copies of a few clips and some audio on StudioFS. Create a test project, import those files, and keep the render cache and temporary output on a local SSD.

Test more than playback from the start of a clip. Jump to the middle of a long recording, scrub quickly through the timeline, open several sources, and make a short export. Then close and reopen the project. If you use proxies, also test switching back to the originals before the final export.

To hand an edit over to another computer, use Resolve's supported project export with an agreed media folder structure, or Resolve's own collaboration tools. A shared drive alone does not merge two editors' changes.

## Nuke: the comp, dependencies, and frames

Put a test `.nk` comp and its required assets in a clear folder structure. Start with a local SSD for Nuke's disk cache. Test reading video, textures, and image sequences, jumping between frames far apart, and a small render through Write.

Paths must work on every machine. A Windows drive letter and a Linux mount point, for example, will differ. Use relative paths where your project supports them, or configure path mapping through the application or your pipeline. StudioFS does not rewrite paths inside the comp.

Save each new version of the comp as a separate file, such as `shot_020_comp_v003.nk`. Give each render version its own folder, too. Two artists writing frames with identical names into one folder will get a shared result. The only question is whether it is the result they wanted.

## Blender: the scene is only the beginning

A `.blend` file may reference textures, libraries, audio, simulation caches, and other external data. Moving the scene alone is not enough: it may open on another computer without the assets it needs.

For a trial run, copy the scene together with its dependencies, check the paths, and open it on the second machine. Check textures, linked libraries, and a few frames of animation, then make a small render. Relative paths are convenient if the folder structure stays intact; use resource packing only for data types Blender actually supports packing.

Initially, send active simulations and temporary renders to a local SSD. If the team needs a finished simulation cache, transfer the complete set into its own version folder and check that another machine can read it. Do not change the set while a colleague is using it.

## Image sequences and large media files

A large video and a folder containing tens of thousands of EXRs place different demands on the system. For video, the amount of data needed and how quickly it can be fetched matter. Sequences add numerous file opens and directory listings. A successful test of one format does not test the other.

StudioFS fetches the parts of large files it needs and caches what it reads locally; readahead may fetch additional data. Opening something for the first time and playing it again can therefore feel noticeably different. Test both cases with representative material on your usual internet connection. There is no universal “it handles this many 4K streams” figure: codec, bitrate, decoding, SSD, network, and the storage itself all affect the result.

For a sequence, check the first, middle, and last frames, the range, and gaps in the numbering. For an important handoff, compare file lists, sizes, and checksums. The first EXR appearing in a folder does not mean the whole shot is ready.

## Proxies: less data, the same agreements

If the originals are too heavy for comfortable work, create proxies using your application or pipeline. StudioFS does not transcode video or generate proxies itself. Smaller files reduce transfer volume, but they still have to be uploaded and become available to colleagues.

Keep originals and proxies in separate folders with clearly matching names. Agree on frame rate, timecode, and other settings your edit needs. On the second machine, check that the application finds the right files, and before the final render, check that it uses the intended originals. Proxies save you unnecessary traffic, but they cannot save you from mixed-up takes.

## “Saved” does not yet mean “ready to pick up”

When an application reports that it has saved, that does not confirm the new version is available on another computer. StudioFS accepts changes locally first, then uploads data to S3 and publishes the version in the background. The other machine still has to apply that update. Closing a file, or even requesting `fsync`, does not wait for publication to colleagues.

A colleague may not see a new file at all yet; for an existing file, they may see the previous published version. And one file being ready does not make the entire set ready: a comp, textures, and a thousand frames are not published through one shared save operation.

Hand work over like this:

1. Finish writing and close the files in the application. Create a separate version of the scene or comp, and put output in that version's own folder.
2. Check StudioFS status and wait for background transfers to finish without errors. We will cover status and activity in more detail in [chapter 22](22-settings-status-and-activity.md).
3. On the second machine, open the new version specifically, check its dependencies, and verify that the output is complete. For frame sets, compare the expected file list rather than just the folder name.
4. After checking, tell your colleague the exact version and hand over responsibility for editing. Do not change that version's files during the handoff.

If the second machine cannot see the result, do not delete the local cache or reinstall the client at random. Check the connection and transfer errors; keep local data until you establish the state of your changes. To disconnect the volume, use [safe unmounting](17-unplug-safely-studiofs-umount.md).

## A small rehearsal before a big project

Take a copy of a representative shot: a few clips, a short sequence, a scene with textures. Keep an independent backup. Go through the whole process: opening, editing, saving a new version, rendering, and handing it over to a second computer. Test your usual autosaves, plugins, and path mapping between operating systems.

If a step is too slow or unreliable, keep it on a local drive and use StudioFS to transfer checked assets and completed versions. Expand your workflow gradually as testing confirms your scenario. And do not entrust the only copy of a client's project to a new setup—even if your test Blender cube already renders flawlessly.

In the next chapter, we will look at what happens to synchronization when the network starts misbehaving.

---

[Contents](README.md) · [Українська](../uk/18-working-with-real-creative-projects.md) · [← Previous](17-unplug-safely-studiofs-umount.md) · [Next →](19-sync-bad-wi-fi-and-other-surprises.md)

[Linear source](https://linear.app/valandreev/document/18-working-with-real-creative-projects-97b051e24905)
