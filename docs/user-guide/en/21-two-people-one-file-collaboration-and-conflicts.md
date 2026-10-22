# 21. Two People, One File: Collaboration and Conflicts
> **Work in progress**

A shared drive is convenient because the editor, colorist, and compositor can all see the same media. But sharing access to a file does not mean editing its contents together. If two people open the same scene, each application works with its own state, and StudioFS does not know which version the director has approved.

The risks of editing in parallel are not unique to StudioFS; other distributed file systems have them too. Sharing access alone does not merge parallel changes to a file's contents. The specific behavior and available locking mechanisms depend on the system and on whether the application supports them.

**For a working file, agree that one person at a time will make changes.** Work in parallel on different scenes, shots, or separate versions. That is easier than figuring out after the deadline whose dragon lost its tail.

A clearly organized workflow helps avoid unnecessary conflicts.

## What happens when people race each other?

| Situation | Risk | How to organize the work |
| -- | -- | -- |
| Two people change the same file | StudioFS does not merge their edits into one scene; the version you need may stop being available under the shared name | Agree on one person making changes, or work in separate files |
| Two people create a file with the same name | The file name alone will not preserve two independent results | Use unique names with a version number and author |
| One person renames a file while another edits it | An open file and its name in a folder are different things; the old path may no longer work | Rename only after everyone has agreed to close the file |
| One person deletes a file while another is working on it | An open scene does not guarantee that the file still exists at that path | Delete only after everyone has finished working with it |
| One person moves media while another opens the project | References to textures, clips, or caches may point to old paths | Plan the move separately and check dependencies |

Do not rely on “whoever clicks Save last wins.” The order of local saves and the order in which changes are published are different things. Do not expect an automatic “conflicted copy,” either: that is no way to preserve both results. Create the versions you need yourself.

## What about file locking?

StudioFS has a shared locking mechanism through PostgreSQL. When a client knows about another client's exclusive lock, it rejects new conflicting changes, including writes, renames, and deletions. Reading remains possible.

However, this is not a universal ban on editing in every application. Whether locks are used depends on the application and how it interacts with the file system on your platform. A write already in progress may also finish according to the rules for an open file. The fact that someone opened a scene does not prove that it is locked for the whole team.

If your workflow relies on locking, test it first on two computers with the specific application and a temporary test file. Check any lock files created by the application separately too. You still need an agreement about handing over work.

## If both of you have already saved your changes

**First, stop any further writes to the shared file.** Do not turn this into a repeat-saving tournament: that will only make the version you need harder to find.

If different versions of the scene are still open in both applications, use Save As to save each under a unique name on an ordinary local drive, outside the StudioFS volume. Check these copies before closing the applications. For projects with external dependencies, save the required media too, or use the application's project-packaging tools.

Then compare the versions in the application itself, choose a starting point, and transfer the edits you need manually or with the application's own tools. Save the agreed result as a new version and hand it over using the procedure above. StudioFS will not make the creative decisions needed to combine two scenes for you.

If the file was renamed or deleted while you were working, do not close the only open version until you have saved a separate copy. The state visible in the application may still be useful even if the original name has disappeared from the folder. If the version you need is no longer available in an open application or a separate copy, check the application's autosaves and independent backups. Do not count on recovery just because “it was in the cloud.”

## If changes are not getting through

Check the PostgreSQL and S3 connections on both computers, as well as StudioFS status. Once access is restored, wait for the transfer and open the agreed version again on the second computer. Until then, do not hand over editing or rename the file while your colleague is working on it.

Do not delete the cache, recreate the volume, or run cleanup to “nudge” synchronization along: the only copies of changes that have not yet been published may still be stored locally. First save the results you can access separately. If the problem persists, record the file name, sequence of actions, and error messages for diagnosis.

## What is next?

Your team now has a simple rule: one person making changes at a time, clear versions, and a handoff check from another computer. In the next chapter, we will look at StudioFS settings, status, and activity so you can understand whether data is still being transferred and when to investigate a delay.

---

[Contents](README.md) · [Українська](../uk/21-two-people-one-file-collaboration-and-conflicts.md) · [← Previous](20-cache-why-is-the-cloud-using-my-ssd.md) · [Next →](22-settings-status-and-activity.md)

[Linear source](https://linear.app/valandreev/document/21-two-people-one-file-collaboration-and-conflicts-0f767280fde1)
