# minio-statemap

Convert [MinIO](https://github.com/minio/minio) trace data to the
[statemap](https://github.com/joyent/statemap) format for make benefit the
glorious distributed storage.

## Usage

### Start up a MinIO cluster

Although minio-statemap technically works with standalone MinIO instances it is
much more interesting to see distributed traces.

In this example we'll look at a ten-node ten-disk MinIO cluster running with
docker-compose on a Mac.

```
docker-compose up
```

### Generate serial load

minio-statemap only works with serial workloads. This generally makes sense when
we think about how statemaps and distributed MinIO work. We'll discuss this more
in the 'Why it Works' section.

We can use a great cross-protocol tool like manta-chum to generate load to the
MinIO cluster.

```
./chum -t s3:localhost
```

For more information on manta-chum, see
[its GitHub repository](https://github.com/joyent/manta-chum).

### Collect a MinIO trace

We'll use the `mc` tool to generate the trace for us. Make sure you have
configured the `mc` tool to point to your MinIO cluster.

Let this command run for a few seconds and then kill it. The output will be in
the `my_trace` file.

```
mc admin trace -a --json min0 > my_trace
```

### Convert the trace data to the statemap format

`minio-statemap` prints to stdout, so redirect to the file of your choice.

```
./minio-statemap -i my_trace > minio_statemap_data
```

### Convert the statemap data to a statemap SVG

Find the [statemap tool](https://github.com/joyent/statemap) and invoke it
with the MinIO statemap data file.

```
./statemap minio_statemap_data > minio.svg
```

Open minio.svg in an SVG viewer (like FireFox) and start exploring!

### Example

An example statemap rendered using this tool can be found
[here](./examples/minio.svg).

## Why it Works

It doesn't, really. `minio-statemap` was created for a singular use case: figure
out why MinIO has such high latency when under very little load. A secondary
question we wanted answered was: how does MinIO work at a high level?
`minio-statemap` answers these questions by displaying high-level RPC
information for each member of the cluster.

MinIO servers are highly concurrent. A given server instance can have
hundreds of threads serving hundreds of simultaneous user requests.
Unfortunately the MinIO trace data doesn't provide much granularity in identites
that we could use as entities. The most granular entity we can use is the
host that serviced a given request.

By serializing the workload we don't have to worry that an entity is changing
states beneath us, which makes it possible to create a statemap. Although it
would be great if we could make a really rich statemap that shows what each
MinIO thread in each instance is busy doing at any given time this is not
possible using an unmodified MinIO tracing API (and we would have to make
`statemap` better at displaying this sort of information too).
