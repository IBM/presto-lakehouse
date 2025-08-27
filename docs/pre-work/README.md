# Prerequisite

This workshop uses the [Docker](https://docs.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) CLI tools to set up a Presto cluster, a local REST server on top of a PostgreSQL database, and a MinIO s3 object storage instance. We recommend [Podman](https://podman.io/), which is a rootless - and hence more secure - drop-in replacement for Docker. [Install Podman](https://podman.io/docs/installation) and ensure that `podman` has been successfully `alias`'ed to `docker` in your working environment.

## Clone the workshop repository

Various parts of this workshop will require the configuration files from the workshop repository. Use the following command to download the whole repository:

```bash
git clone https://github.com/IBM/presto-lakehouse.git
cd presto-lakehouse
```

Alternatively, you can [download the repository as a zip file](https://codeload.github.com/IBM/presto-lakehouse/zip/refs/heads/main), unzip it and change into the `presto-lakehouse` main directory.

## Download the required jars (for Hudi portion only)

We need to include some additional jars to the Spark container so that we can take advantage of Hudi and s3 functionality.

Download the jars from the command line:

```bash
curl -sSL https://github.com/IBM/presto-lakehouse/releases/download/0.1.0/jars.tar.gz | tar -zxvf - -C src/conf
```

You may need to include `sudo` in the final command depending on the permissions granted in the `src/conf` directory, e.g.: `sudo tar -xvzf jars.tar.gz`.

Alternatively, you can download the zipped jar files [directly from the latest release of the repo](https://github.com/IBM/presto-lakehouse/releases/tag/0.1.0), unzip the folder, and manually move them into the `src/conf/jars` path.

## Optional: Join the Presto community

If you are working on this lab and run into issues, you can reach out on the [Presto Slack](https://communityinviter.com/apps/prestodb/prestodb). The `#presto-iceberg-connector`, `#presto-hudi-connector` or `#presto-deltalake` channels are a good place to start. We'll do our best to help troubleshoot with you there! Even if you don't need any help with this workshop, we encourage you to join. Slack is the best place to meet other Presto engineers and users.

If you're interested in contributing code or documentation to Presto, we'd love to have you! Start at the [Presto GitHub repo](https://github.com/prestodb/presto).
