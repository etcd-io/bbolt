
<hr>

## v1.5.0(2026-06-21)
There isn't any production code change since v1.5.0-rc.0. Only golang patch version is bumped.

<hr>

## v1.5.0-rc.0(2026-05-27)

### BoltDB
- Add support for data file size limit, see [PR/929](https://github.com/etcd-io/bbolt/pull/929) and [PR/1210](https://github.com/etcd-io/bbolt/pull/1210)
- [Remove the unused txs list](https://github.com/etcd-io/bbolt/pull/973)
- [Add option `NoStatistics` to make the statistics optional](https://github.com/etcd-io/bbolt/pull/977)
- [Move panic handling from goroutine to the parent function](https://github.com/etcd-io/bbolt/pull/1153)
- [Recover from panics in tx.check](https://github.com/etcd-io/bbolt/pull/1164)
- [freelist/hashmap: batch contiguous IDs when merging free spans](https://github.com/etcd-io/bbolt/pull/1179)

<hr>
