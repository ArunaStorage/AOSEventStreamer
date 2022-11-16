# AOSEventStreamer

This is the event streaming server for the AOS system.

## Status

Events can be emitted for each resource type. Querying is currently only possible for projects and collections.

## Deployment

### Environment variable

| Parameter                                          | Environment variable       | default |
| -------------------------------------------------- | -------------------------- | ------- |
| Token for internal authorization                   | INTERNAL_EVENT_TOKEN       | \*      |
| Hostname of the nats server                        | NATS_HOST                  | \*      |
| Port of the nats server                            | NATS_PORT                  | \*      |
| Endpoint for the internal event service            | EVENT_SERVICE              | \*      |
| Endpoint for the internal authorization service    | AUTHZ_SERVICE              | \*      |
| Bind address for the internal event emitter server | INTERNAL_EVENT_SERVER_HOST | \*      |
| Bind address for the public event server           | PUBLIC_EVENT_SERVER_HOST   | \*      |

## Tests

Units tests are available whereever possible.
Most however do require the presence of backends. The are implemented in the storage_test_server module. The tests based on these functions are implemented in the e2e repository.

Currently many special cases are missing and have to be added before the first production release.
