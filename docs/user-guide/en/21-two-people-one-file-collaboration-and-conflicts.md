# 21. Two People, One File: Collaboration and Conflicts

A shared drive is convenient because the editor, colorist, and compositor all see the same media. But sharing access to a file does not mean editing its contents together. If two people open the same scene, each application works with its own state, and StudioFS has no idea which version the director has already approved.

**Agree on one person making changes to a working file at a time.** Work in parallel on different scenes, shots, or separate versions. That is easier than figuring out after the deadline whose dragon lost its tail.

## Why does my colleague still see the old version?

When you click Save, your computer accepts the changes first. StudioFS then uploads the data to S3 in the background, publishes the update, and passes metadata to other computers through PostgreSQL. These steps take time.

So a successful save in your application does not prove that your colleague can already read the new file. Even closing the file does not, by itself, confirm publication. And if your colleague keeps the scene open, its state in the application's memory may differ from what is already on disk.

For colleagues, updated contents are published as a complete file version once the necessary data is ready. But that does not mean the entire project updates at once: the scene, textures, and supporting files may become available at different times.

## What happens when people race each other?

| Situation | Risk | How to organize the work |
| -- | -- | -- |
| Two people change the same file | StudioFS does not merge their edits into one scene; the version you need may stop being available under the shared name | One person making changes, or a separate file for each person |
| Two people create files with the same name | One name will not preserve two independent results | Unique names that include a version number and author |
| One person renames a file while another edits it | An open file and its name in a folder are different things; the old path may no longer work | Agree to close the file before renaming it |
| One person deletes a file while another works on it | An open scene does not guarantee that the file still exists at that path | Delete only after everyone has finished working with it |
| One person moves media while another opens the project | References to textures, clips, or caches may point to old paths | Plan the move separately and check dependencies |

Do not rely on “whoever clicks Save last wins.” The order of local saves and the order in which changes are published are different things. Do not expect an automatic “conflicted copy,” either: that is no way to preserve both results. Create the versions you need yourself.

## What about file locking?

StudioFS has a mechanism for coordinating locks between clients through PostgreSQL. When a client knows about another client's exclusive lock, it rejects new conflicting changes, including writes, renames, and deletions. Reading remains possible.

However, this does not universally prevent editing in every application. Whether locks are used depends on the application and how it interacts with the file system on your platform. Writes already in progress may also finish according to the rules for open files. Someone opening a scene does not, by itself, prove that the scene is locked for the whole team.

If your pipeline relies on locking, test it on two computers first, using the actual application and an unimportant test file. Any lock files the application creates itself should also be tested separately. You still need an agreement about handing over work.

## How do you hand a scene over without surprises?

Before starting, both computers must be connected to the same StudioFS volume and have access to PostgreSQL and S3. For your first test, use a copy of a small scene, and keep a separate, independent backup of any important project.

1. **Agree on who is editing now.** For example, Olena prepares the scene while Maksym only reads the media and does not save over that scene.
2. **Save a new version under a separate name.** For example, `кадр_042_комп_v017_олена.nk`. Use the next available number instead of overwriting a version your colleague has already started working on. If the application supports packaging a project with its dependencies, use that when needed.
3. **Close the file in the application and leave StudioFS running.** Allow the background transfer to finish. Synchronization status helps you spot activity or errors, but the final handoff check happens on your colleague's computer.
4. **Check the result on the second computer.** Maksym finds that exact `v017`, opens it afresh, and checks for a recognizable latest edit and the required media. A name appearing in a folder listing does not, on its own, confirm that the scene is ready to work on.
5. **Confirm the handoff.** After checking, Maksym confirms that he has received the version and saves his subsequent changes as `v018`. Olena stops writing to the version she has handed over.

The handoff succeeds when your colleague opens the agreed version, sees the expected edit, and can read its dependencies. If the file is not visible yet or will not open, the handoff is not complete—even if “done” has already landed in the chat.

## If both of you have already saved changes

**First, stop any further writes to the shared file.** Do not turn it into a repeat-saving tournament: that will only make the version you need harder to find.

If different versions of the scene are still open in both applications, use Save As to save each one under a unique name on a regular local drive, outside the StudioFS volume. Check these copies before closing the applications. For projects with external dependencies, save the necessary media too, or use the application's project-packaging tools.

Then compare the versions in the application itself, choose a starting point, and transfer the edits you need manually or with the application's own tools. Save the agreed result as a new version and hand it over using the procedure above. StudioFS will not make the creative decisions needed to combine two scenes for you.

If the file was renamed or deleted while you were working, do not close the only open version until you have saved a separate copy. The state visible in the application may still be useful even if the original name has disappeared from the folder. If the version you need is no longer available in an open application or a separate copy, check the application's autosaves and independent backups. Do not count on recovery just because “it was in the cloud.”

## If changes are not getting through

Check the connection to PostgreSQL and S3 on both computers, along with StudioFS status. Once access is restored, wait for the transfer and try opening the agreed version on the second computer again. Until then, do not hand over editing or rename the file while your colleague is working on it.

Do not delete the cache, recreate the volume, or run a cleanup to “nudge” synchronization along: the only copies of changes that have not yet been published may still be stored locally. Save the results you can access separately first. If the problem persists, record the file name, sequence of actions, and error messages for diagnosis.

## What is next?

Your team now has a simple rule: one person making changes, clear versions, and a handoff check from another computer. In the next chapter, we will look at StudioFS settings, status, and activity—so you can tell when it is still working and when it is time to investigate a delay.

---

[Contents](README.md) · [Українська](../uk/21-two-people-one-file-collaboration-and-conflicts.md) · [← Previous](20-cache-why-is-the-cloud-using-my-ssd.md) · [Next →](22-settings-status-and-activity.md)

[Linear source](https://linear.app/valandreev/document/21-two-people-one-file-collaboration-and-conflicts-0f767280fde1)
