# 18. What StudioFS Is Suited For, and Where to Be Careful
> **Work in progress**

StudioFS was created and tested for media production: working with shared video, audio, images, frame sequences, and creative project assets. This is our main use case, and the one for which we test filesystem behavior first.

That does not mean other applications or types of workload will not work. But we have not tested them to the same degree as media production, and they are not currently our main optimization priority. So do not treat compatibility with an arbitrary application or its performance as guaranteed: what matters is your particular workflow, application version, and operating system.

## Use caution with

**Relational databases.** StudioFS has not been tested as storage for such databases' working files, so it may be a poor choice. You can try it, but start in a test environment and do not risk your only copy of the data.

The PostgreSQL server StudioFS uses for filesystem metadata runs as a separate server component. That is not the same as placing a database's working files on a mounted volume.

**AI workloads.** We have not yet tested StudioFS with AI tasks. They may work, but we currently have no results to rely on when promising compatibility or performance.

These limitations do not mean such scenarios are impossible. They mean their behavior should be tested separately, and that optimizing for general and specialized workloads remains a lower priority than media production.

## Optimizations for creative applications

We plan to focus first on improving StudioFS behavior in DCC applications—tools for editing, compositing, 3D, and other stages of media creation.

StudioFS already has a mechanism for pre-reading sequences of files with numbered names, such as EXR frames **plate_0100.exr**, **plate_0101.exr**, and so on. It is intended to speed up reading such sequences. This is a targeted optimization; its actual effect depends on the scenario and application.

Other optimizations for creative applications are still in progress. We will share them as they become ready.

In the next chapter, we will look at what happens to synchronization when the network starts misbehaving.

---

[Contents](README.md) · [Українська](../uk/18-studiofs-workloads.md) · [← Previous](17-unplug-safely-studiofs-umount.md) · [Next →](19-sync-bad-wi-fi-and-other-surprises.md)
