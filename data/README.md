# data/

The task dataset is **confidential** and is intentionally **not committed**
(see `.gitignore`). This directory is kept in the repo only so the Docker
Compose bind-mounts have a valid path.

Before running anything, place the two provided files here:

```
data/users.csv          # the MySQL "users" table
data/user_events.csv    # the event stream replayed into Kafka
```

`python -m azki seed` will additionally generate `data/orders/` from the
purchase events — those are also git-ignored.
