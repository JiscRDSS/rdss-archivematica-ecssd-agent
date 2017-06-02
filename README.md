## rdss-archivematica-ecssd-agent

This is a clone of `ecssd_agent` (see [Service Discovery for Amazon ECS using DNS](https://aws.amazon.com/blogs/compute/service-discovery-for-amazon-ecs-using-dns/] for more details) that creates `A` records instead of `SVR` records. It is limited to one single replica per service - this is a compromise until we can set up a real service discovery solution, preferably one that doesn't require changes in the application, e.g. sidecar proxy.
