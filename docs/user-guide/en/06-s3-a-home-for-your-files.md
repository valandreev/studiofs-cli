# 06. S3: A Home for Our Files

In the previous chapter, we decided where our data would live: on our own server or in a provider's cloud. Now it is time to prepare a place for it.

Suppose you have 8 TB of source media, renders, and projects. We usually think of storage as a large drive with folders. S3 works differently: it stores **objects**, rather than a familiar file system. And that is exactly what StudioFS needs.

## What do we actually put in S3?

Amazon S3 is an object storage service, and the S3 API is an interface for working with that kind of storage. Other services support this API too. So “S3-compatible storage” does not necessarily mean Amazon.

S3 has **buckets**—containers for objects. In our case, a bucket is where StudioFS uploads file contents. File names, folder structure, and information about changes will be coordinated by PostgreSQL, which we will prepare in the next chapter.

Do not be surprised if, after copying a video into StudioFS, you cannot find an object named `interview.mov` in the bucket. StudioFS stores data in parts under its own technical names and encrypts it before uploading. **A bucket is not a backup of an ordinary folder structure or a place to edit files manually.**

This also means that without the corresponding PostgreSQL metadata, the collection of objects in S3 will not turn back into your drive. To keep your projects safe, you will later need backups of **both** components.

## Creating a bucket

Every provider uses different button names, but the sequence is almost the same.

1. Open your S3 service's control panel and find the option to create a bucket.
2. Give it a name, such as `studiofs-projects-demo`. Use lowercase Latin letters, numbers, and hyphens. The name must meet your provider's rules and uniqueness requirements.
3. Choose a region if the service allows it. Ideally, it should be geographically close to the main workstations, but also consider the team's location and data transfer costs.
4. Keep the bucket **private**. StudioFS does not need public access to objects.
5. Create the bucket and record its name and S3 endpoint. Backblaze usually shows the address in the bucket settings; in AWS, it depends on the region.

For now, do not enable automatic object deletion rules (lifecycle expiration) unless you understand their consequences. StudioFS manages which objects belong to the file system itself. If a provider deletes a needed object on its own schedule, PostgreSQL cannot magically bring its contents back.

**Do you have to create the bucket manually?** No. `studiofs init` may try to create it automatically if its availability check fails. This requires the appropriate permissions. But creating the bucket in advance is better: you control its settings and do not have to grant StudioFS unnecessary permissions.

## Creating dedicated keys

The storage now exists. All that remains is to tell it who is allowed to work with it.

Connecting StudioFS to S3 requires a pair of keys:

* **Access Key ID**—the access identifier.
* **Secret Access Key**—the secret used to sign requests.

Create **a dedicated key for StudioFS**, rather than using the main administrator keys for your entire storage service.

Normal operation requires permission to list objects in the bucket, read them, upload them, and delete them. In AWS terms, these include `s3:ListBucket` for the bucket itself and `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` for its objects.

Do not give the key access to every bucket in your account unless it needs it. Save the Secret Access Key somewhere secure immediately after creating it: some services show it only once.

**Important:** do not paste secrets into conversations, screenshots, or public bug reports. When creating a volume, StudioFS uses and stores access settings in the volume configuration; do not treat local configuration or PostgreSQL as encrypted secret storage. If you later revoke the key at the provider, existing clients will lose access to S3 until their access is correctly updated.

## Five values StudioFS will need

Once the bucket and keys are ready, collect five settings:

* **Endpoint**—the S3 API address. Use `https://` for a remote service. This is neither the control panel URL nor a link to an individual file.
* **Bucket**—the exact name of the container you created, such as `studiofs-projects-demo`.
* **Region**—the region the provider uses for S3 requests and request signing.
* **Access Key ID**—the identifier of the key you created.
* **Secret Access Key**—the secret part of that key.

Example addresses and regions:

| Service | Endpoint (example) | Region (example) |
| -- | -- | -- |
| Amazon S3 | `https://s3.eu-central-1.amazonaws.com` | `eu-central-1` |
| Backblaze B2 | `https://s3.us-west-004.backblazeb2.com` | `us-west-004` |
| Local RustFS | `http://127.0.0.1:9000` | `us-east-1` |

In the current StudioFS, these values are passed to `studiofs init` through `--endpoint`, `--bucket`, `--region`, `--access-key`, and `--secret-key`. If you omit `--region`, StudioFS uses `us-east-1` by default. For cloud storage, explicitly specify the region supplied by your provider.

## A few rules to save your nerves

**Do not make the bucket public.** StudioFS clients use keys for access, not anonymous links. Choose HTTPS for remote S3; plain HTTP is appropriate only where you have deliberately arranged a secure test or private connection.

**Do not modify StudioFS objects manually.** Their technical names and contents are not intended for manual management. Deleting “mysterious junk” in a bucket may turn out to mean deleting part of a media file you need.

**Do not confuse encryption with preservation.** StudioFS automatically encrypts data before sending it to S3, but this does not protect against accidental bucket deletion, lost metadata, or an incorrect retention policy. HTTPS transport encryption serves yet another purpose.

**Remember geography.** If your users are in Europe and storage is on another continent, requests may have noticeably higher latency. The local cache helps, but it does not repeal the physics of networking.

We have now prepared a home for the data: a private bucket, dedicated keys, an endpoint, and a region. Next comes **PostgreSQL**, which will handle the shared file structure and changes between computers. After that, we just need to install StudioFS and combine S3 and PostgreSQL into a volume—and our future cloud drive will be ready to use.

---

[Contents](README.md) · [Українська](../uk/06-s3-a-home-for-your-files.md) · [← Previous](05-choose-your-setup.md) · [Next →](07-postgresql-the-brain-behind-the-files.md)

[Linear source](https://linear.app/valandreev/document/06-s3-a-home-for-your-files-99d3dc8ea8c1) · Source updated: 2026-10-19T20:18:03.120Z · English translation; review pending.
