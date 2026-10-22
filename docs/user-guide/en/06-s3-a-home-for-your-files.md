# 06. S3: a home for our files
> **Work in progress**

In the previous section, we decided where our data will live: on our own server or in a cloud provider’s storage. Now it is time to prepare a place for it.

Suppose you have 8 TB of source footage, renders, and projects. We usually imagine storage as a large drive full of folders. S3 works differently: it stores **objects**, not a familiar file system. And that is exactly what StudioFS needs.

## What are we actually putting in S3?

Amazon S3 is an object storage service, and the S3 API is an interface for working with that kind of storage. Other services support this API, too. So “S3-compatible storage” does not necessarily mean Amazon.

S3 has **buckets**, which are containers for objects. In our case, this is where StudioFS will upload file contents. PostgreSQL, which we will prepare in the next section, will coordinate file names, folder structure, and change information.

Do not be surprised if, after copying a video into StudioFS, you cannot find an object named `interview.mov` in the bucket. StudioFS stores data in pieces under its own technical names and encrypts it before upload. **A bucket is not a backup of your regular folder structure or a place to edit files by hand.**

This also means that without the corresponding PostgreSQL metadata, the set of objects in S3 will not turn back into your drive. To preserve your projects, you will need backups of **both** components.

## Create a bucket

Each provider uses different button names, but the steps are nearly the same.

1. Open your S3 service’s control panel and find the option to create a bucket.
2. Choose a name, such as `studiofs-projects-demo`. Use lowercase Latin letters, digits, and hyphens. The name must meet your provider’s rules and uniqueness requirements.
3. Choose a region, if the service allows it. Ideally, it should be geographically close to the main work locations, but also consider where your team is and the cost of data transfer.
4. Keep the bucket **private**. StudioFS does not need public access to objects.
5. Create the bucket and write down its name and S3 endpoint. Backblaze usually shows the address in the bucket settings; with AWS, it is determined by the region.

For now, do not enable automatic object deletion rules (lifecycle expiration) unless you understand the consequences. StudioFS manages which objects belong to the file system. If a provider deletes a required object on its own schedule, PostgreSQL cannot magically restore its contents.

**Do you have to create the bucket manually?** No. `studiofs init` can try to create a bucket automatically if its availability check fails. This requires the appropriate permissions. But it is better to create the bucket in advance: that way you control its settings and do not need to give StudioFS extra permissions.

## Create separate keys

The storage is ready. Now we need to tell it who is allowed to work with it.

StudioFS needs a pair of keys to connect to S3:

* **Access Key ID** — the access identifier.
* **Secret Access Key** — the secret used to sign requests.

Create a **separate key for StudioFS** instead of using the main administrator keys for your entire storage account.

For normal operation, it needs permission to list objects in the bucket and to read, upload, and delete objects. In AWS terms, that includes `s3:ListBucket` for the bucket itself and `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` for its objects.

Do not give the key access to every bucket in your account unless you need to. Save the Secret Access Key in a secure place as soon as you create it: some services show it only once.

**Important:** Do not paste secrets into chats, screenshots, or public bug reports. When creating a volume, StudioFS uses and stores the access settings in the volume configuration; do not treat the local configuration or PostgreSQL as a store for encrypted secrets. If you later revoke the key with your provider, existing clients will lose access to S3 until their access is updated correctly.

## Five values StudioFS will need

When the bucket and keys are ready, collect these five settings:

* **Endpoint** — the S3 API address. Use `https://` for a remote service. This is not the URL of a management page or a link to an individual file.
* **Bucket** — the exact name of the bucket you created, such as `studiofs-projects-demo`.
* **Region** — the region your provider uses for S3 requests and their signatures.
* **Access Key ID** — the identifier of the key you created.
* **Secret Access Key** — the secret part of that key.

Example endpoints and regions:

| Service | Endpoint (example) | Region (example) |
| -- | -- | -- |
| Amazon S3 | `https://s3.eu-central-1.amazonaws.com` | `eu-central-1` |
| Backblaze B2 | `https://s3.us-west-004.backblazeb2.com` | `us-west-004` |
| Local RustFS | `http://127.0.0.1:9000` | `us-east-1` |

In the current version of StudioFS, pass these values to `studiofs init` with the `--endpoint`, `--bucket`, `--region`, `--access-key`, and `--secret-key` options. If you omit `--region`, StudioFS uses `us-east-1` by default. For cloud storage, be sure to specify the region supplied by your provider.

## A few rules that will save you trouble

**Do not make the bucket public.** StudioFS clients use keys for access, not anonymous links. Choose HTTPS for remote S3; plain HTTP is appropriate only when you have deliberately set up a secure test or private connection.

**Do not change StudioFS objects by hand.** Their technical names and contents are not meant for manual management. Deleting “mysterious junk” in the bucket might delete part of a media file you need.

**Do not confuse encryption with preservation.** StudioFS automatically encrypts data before sending it to S3, but that does not protect you from accidentally deleting the bucket, losing metadata, or using an incorrect retention policy. HTTPS encryption serves a different purpose, too.

**Keep geography in mind.** If your users are in Europe and storage is on another continent, requests may have noticeably higher latency. The local cache helps, but it cannot repeal the laws of physics.

We have prepared a home for the data: a private bucket, separate keys, an endpoint, and a region. The next step is **PostgreSQL**, which will manage the shared file structure and changes between computers. After that, we will install StudioFS, combine S3 and PostgreSQL into one volume, and our future cloud drive will be ready to use.

---

[Contents](README.md) · [Українська](../uk/06-s3-a-home-for-your-files.md) · [← Previous](05-choose-your-setup.md) · [Next →](07-postgresql-the-brain-behind-the-files.md)

[Linear source](https://linear.app/valandreev/document/06-s3-a-home-for-your-files-99d3dc8ea8c1) · Source updated: 2026-10-19T20:18:03.120Z · Ukrainian original approved for translation.
