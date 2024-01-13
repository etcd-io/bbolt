
<hr>

## v1.4.0-alpha.0(2024-01-12)

### BoltDB
- [Improve the performance of hashmapGetFreePageIDs](https://github.com/etcd-io/bbolt/pull/419)
- [Improve CreateBucketIfNotExists to avoid double searching the same key](https://github.com/etcd-io/bbolt/pull/532)
- [Support Android platform](https://github.com/etcd-io/bbolt/pull/571)
- [Record the count of free page to improve the performance of hashmapFreeCount](https://github.com/etcd-io/bbolt/pull/585)
- [Add logger to bbolt](https://github.com/etcd-io/bbolt/issues/509)
- [Support moving bucket inside the same db](https://github.com/etcd-io/bbolt/pull/635)
- [Support inspecting database structure](https://github.com/etcd-io/bbolt/pull/674)

### CMD
- [Add `surgery clear-page-elements` command](https://github.com/etcd-io/bbolt/pull/417)
- [Add `surgery abandon-freelist` command](https://github.com/etcd-io/bbolt/pull/443)
- [Add `bbolt version` command](https://github.com/etcd-io/bbolt/pull/552)
- [Add `bbolt inspect` command](https://github.com/etcd-io/bbolt/pull/674)
- [Add `--no-sync` option to `bbolt compact` command](https://github.com/etcd-io/bbolt/pull/290)
