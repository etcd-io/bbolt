# Introduction to bbolt command line

`bbolt` provides a command line utility for inspecting and manipulating bbolt database files. To install bbolt command-line please refer [here](https://github.com/etcd-io/bbolt#installing)

**Note**: [etcd](https://github.com/etcd-io/etcd) uses bbolt as its backend storage engine. In this document, we take etcd as an example to demonstrate the usage of bbolt commands. Refer to [install etcd](https://etcd.io/docs/v3.5/install/) for installing etcd.

1. Start a single member etcd cluster with this command below:

    ```bash
    $etcd
    ```

    It will create a directory `default.etcd` by default under current working directory, and the directory structure will look like this:

    ```bash
    $tree default.etcd
    default.etcd
    └── member
        ├── snap
        │   └── db // this is bbolt database file
        └── wal
            └── 0000000000000000-0000000000000000.wal

    3 directories, 2 files
    ```

2. Put some dummy data using [etcdctl](https://github.com/etcd-io/etcd/tree/main/etcdctl).
3. Stop the etcd instance. Note a bbolt database file can only be opened by one read-write process, because it is exclusively locked when opened.

## Usage

- `bbolt command [arguments]`

### help

- help will print information about that command

  ```bash
  $bbolt help

  The commands are:

      version     prints the current version of bbolt
      bench       run synthetic benchmark against bbolt
      buckets     print a list of buckets
      check       verifies integrity of bbolt database
      compact     copies a bbolt database, compacting it in the process
      dump        print a hexadecimal dump of a single page
      get         print the value of a key in a bucket
      info        print basic info
      keys        print a list of keys in a bucket
      help        print this screen
      page        print one or more pages in human readable format
      pages       print list of pages with their types
      page-item   print the key and value of a page item.
      stats       iterate over all pages and generate usage stats
      surgery     perform surgery on bbolt database
  ```

- you can use `help` with any command: `bbolt [command] -h` for more information about command.

## Analyse bbolt database with bbolt command line

### version

`version` print the current version information of bbolt command-line.

Usage:
```
$ bbolt version -h
print the current version of bbolt

Usage:
  bbolt version [flags]

Flags:
  -h, --help   help for version
```

Example:
  
  ```bash
  $bbolt version
  bbolt version: 1.3.7
  Go Version: go1.21.6
  Go OS/Arch: darwin/arm64
  ```

### info

`info` print the basic information about the given Bbolt database.

Usage:
```
$ bbolt info -h
prints basic information about the bbolt database.

Usage:
  bbolt info <bbolt-file> [flags]

Flags:
  -h, --help   help for info
```

Example:

```bash
$bbolt info ~/default.etcd/member/snap/db
Page Size: 4096
```

- **note**: page size is given in bytes
- Bbolt database is using page size of 4KB

### buckets

`buckets` print a list of buckets of Bbolt database is currently having. Find more information on buckets [here](https://github.com/etcd-io/bbolt#using-buckets)

Usage:
```
$ bbolt buckets -h
print a list of buckets in bbolt database

Usage:
  bbolt buckets <bbolt-file> [flags]

Flags:
  -h, --help   help for buckets
```

Example:

```bash
$bbolt buckets ~/default.etcd/member/snap/db
alarm
auth
authRoles
authUsers
cluster
key
lease
members
members_removed
meta
```

- It means when you start an etcd, it creates these `10` buckets using bbolt database.

### check

`check` opens a database at a given `[PATH]` and runs an exhaustive check to verify that all pages are accessible or are marked as freed. It also verifies that no pages are double referenced.

Usage:
```
$ bbolt check -h
verify integrity of bbolt database data

Usage:
  bbolt check <bbolt-file> [flags]

Flags:
      --from-page uint   check db integrity starting from the given page ID
  -h, --help             help for check
```

Example:

```bash
$bbolt check ~/default.etcd/member/snap/db
ok
```

- It returns `ok` as our database file `db` is not corrupted.

### stats

To gather essential statistics about the bbolt database: `stats` performs an extensive search of the database to track every page reference. It starts at the current meta page and recursively iterates through every accessible bucket.

Usage:
```
$ bbolt stats -h
print stats of bbolt database

Usage:
  bbolt stats <bbolt-file> [flags]

Flags:
  -h, --help   help for stats
```

Example:

  ```bash
  $bbolt stats ~/default.etcd/member/snap/db
  Aggregate statistics for 10 buckets

  Page count statistics
      Number of logical branch pages: 0
      Number of physical branch overflow pages: 0
      Number of logical leaf pages: 0
      Number of physical leaf overflow pages: 0
  Tree statistics
      Number of keys/value pairs: 11
      Number of levels in B+tree: 1
  Page size utilization
      Bytes allocated for physical branch pages: 0
      Bytes actually used for branch data: 0 (0%)
      Bytes allocated for physical leaf pages: 0
      Bytes actually used for leaf data: 0 (0%)
  Bucket statistics
      Total number of buckets: 10
      Total number on inlined buckets: 10 (100%)
      Bytes used for inlined buckets: 780 (0%)
  ```

### inspect

`inspect` inspect the structure of the database.

Usage:
```
$ bbolt inspect -h
inspect the structure of the database

Usage:
  bbolt inspect <bbolt-file> [flags]

Flags:
  -h, --help   help for inspect
```

Example:
```bash
$ ./bbolt inspect ~/default.etcd/member/snap/db
{
    "name": "root",
    "keyN": 0,
    "buckets": [
        {
            "name": "alarm",
            "keyN": 0
        },
        {
            "name": "auth",
            "keyN": 2
        },
        {
            "name": "authRoles",
            "keyN": 1
        },
        {
            "name": "authUsers",
            "keyN": 1
        },
        {
            "name": "cluster",
            "keyN": 1
        },
        {
            "name": "key",
            "keyN": 1285
        },
        {
            "name": "lease",
            "keyN": 2
        },
        {
            "name": "members",
            "keyN": 1
        },
        {
            "name": "members_removed",
            "keyN": 0
        },
        {
            "name": "meta",
            "keyN": 3
        }
    ]
}
```

### pages

Pages prints a table of pages with their type (meta, leaf, branch, freelist).
- The `meta` will store the metadata information of database.
- The `leaf` and `branch` pages will show a key count in the `items` column.
- The `freelist` will show the number of free pages, which are free for writing again.
- The `overflow` column shows the number of blocks that the page spills over into.

Usage:
```
$ bbolt pages -h
print a list of pages in bbolt database

Usage:
  bbolt pages <bbolt-file> [flags]

Flags:
  -h, --help   help for pages
```

Example:

  ```bash
  $bbolt pages ~/default.etcd/member/snap/db
  ID       TYPE       ITEMS  OVRFLW
  ======== ========== ====== ======
  0        meta       0
  1        meta       0
  2        free
  3        leaf       10
  4        freelist   2
  5        free
  ```

### page

Page prints one or more pages in human readable format.

Usage:
```
$ bbolt page -h
page prints one or more pages in human readable format.

Usage:
  bbolt page <bbolt-file> [pageid...] [flags]

Flags:
      --all                  List all pages (only skips pages that were considered successful overflow pages)
      --format-value string  Output format one of: auto|ascii-encoded|hex|bytes|redacted. Applies to values on the leaf page. (default "auto")
  -h, --help                 help for page
```

Example:

  ```bash
  $bbolt page ~/default.etcd/member/snap/db 3
  Page ID:    3
  Page Type:  leaf
  Total Size: 4096 bytes
  Overflow pages: 0
  Item Count: 10

  "alarm": <pgid=0,seq=0>
  "auth": <pgid=0,seq=0>
  "authRoles": <pgid=0,seq=0>
  "authUsers": <pgid=0,seq=0>
  "cluster": <pgid=0,seq=0>
  "key": <pgid=0,seq=0>
  "lease": <pgid=0,seq=0>
  "members": <pgid=0,seq=0>
  "members_removed": <pgid=0,seq=0>
  "meta": <pgid=0,seq=0>
  ```

  - It prints information of page `page ID: 3`

### page-item

page-item prints a page item's key and value.

Usage:
```
$ bbolt page-item -h
print a page item key and value in a bbolt database

Usage:
  bbolt page-item [options] <bbolt-file> pageid itemid [flags]

Flags:
      --format string   Output format one of: auto|ascii-encoded|hex|bytes|redacted (default "auto")
  -h, --help            help for page-item
      --key-only        Print only the key
      --value-only      Print only the value
```

Example:

  ```bash
  $bbolt page-item --key-only ~/default.etcd/member/snap/db 3 7
  "members"
  ```

  - It returns the key as `--key-only` flag is passed of `pageID: 3` and `itemID: 7`

### dump

Dump prints a hexadecimal dump of one or more given pages.

Usage:
```
$ bbolt dump -h
prints a hexadecimal dump of one or more pages of bbolt database.

Usage:
  bbolt dump <bbolt-file> pageid [pageid...] [flags]

Flags:
  -h, --help   help for dump
```

### keys

Print a list of keys in the given bucket.

Usage:
```
$ bbolt keys -h
print a list of keys in the given (sub)bucket in bbolt database

Usage:
  bbolt keys <bbolt-file> <buckets> [flags]

Flags:
  -f, --format string   Output format one of: auto|ascii-encoded|hex|bytes|redacted (default "auto")
  -h, --help            help for keys
```

Example 1:

  ```bash
  $bbolt keys ~/default.etcd/member/snap/db meta
  confState
  consistent_index
  term
  ```

  - It list all the keys in bucket: `meta`

Example 2:

  ```bash
  $bbolt keys ~/default.etcd/member/snap/db members
  8e9e05c52164694d
  ```

  - It list all the keys in `members` bucket which is a `memberId` of etcd cluster member.
  - In this case we are running a single member etcd cluster, hence only `one memberId` is present. If we would have run a `3` member etcd cluster then it will return a `3 memberId` as `3 cluster members` would have been present in `members` bucket.

### get

Print the value of the given key in the given bucket.

Usage:
```
$ bbolt get -h
get the value of a key from a (sub)bucket in a bbolt database

Usage:
  bbolt get PATH [BUCKET..] KEY [flags]

Flags:
      --format string        Output format one of: auto|ascii-encoded|hex|bytes|redacted (default: auto) (default "auto")
  -h, --help                 help for get
      --parse-format string  Input format one of: ascii-encoded|hex (default "ascii-encoded")
```

Example 1:

  ```bash
  $bbolt get --format=hex ~/default.etcd/member/snap/db meta term
  0000000000000004
  ```

  - It returns the value present in bucket: `meta` for key: `term` in hexadecimal format.

Example 2:

  ```bash
  $bbolt get ~/default.etcd/member/snap/db members 8e9e05c52164694d
  {"id":10276657743932975437,"peerURLs":["http://localhost:2380"],"name":"default","clientURLs":["http://localhost:2379"]}
  ```

  - It returns the value present in bucket: `members` for key: `8e9e05c52164694d`.

### compact

Compact opens a database at given `[Source Path]` and walks it recursively, copying keys as they are found from all buckets, to a newly created database at `[Destination Path]`. The original database is left untouched.

Usage:
```
$ bbolt compact -h
creates a compacted copy of the database from source path to the destination path, preserving the original.

Usage:
  bbolt compact [options] -o <dst-bbolt-file> <src-bbolt-file> [flags]

Flags:
  -h, --help              help for compact
      --no-sync
  -o, --output string
      --tx-max-size int   (default 65536)
```

Example:

  ```bash
  $bbolt compact -o ~/db.compact ~/default.etcd/member/snap/db
  16805888 -> 32768 bytes (gain=512.88x)
  ```

  - It will create a compacted database file: `db.compact` at given path.

### bench

run synthetic benchmark against bbolt database.

Usage:
```
$ bbolt bench -h
run synthetic benchmark against bbolt

Usage:
  bbolt bench [flags]

Flags:
      --batch-size int          (default 0)
      --blockprofile string
      --count int               (default 1000)
      --cpuprofile string
      --fill-percent float      (default 0.5)
      --gobench-output
  -h, --help                    help for bench
      --initial-mmap-size int   Set initial mmap size in bytes for database file.
      --key-size int            (default 8)
      --memprofile string
      --no-sync
      --page-size int           Set page size in bytes. (default 16384)
      --path string
      --profile-mode string     (default "rw")
      --read-mode string        (default "seq")
      --value-size int          (default 32)
      --work
      --write-mode string       (default "seq")
```

Example:

    ```bash
    $bbolt bench ~/default.etcd/member/snap/db -batch-size 400 -key-size 16
    # Write	68.523572ms	(68.523µs/op)	(14593 op/sec)
    # Read	1.000015152s	(11ns/op)	(90909090 op/sec)
    ```

  - It runs a benchmark with batch size of `400` and with key size of `16` while for others parameters default value is taken.

### surgery

`surgery` perform surgery on bbolt database for repair and recovery operations.

Usage:
```
$ bbolt surgery -h
surgery related commands

Usage:
  bbolt surgery <subcommand> [flags]

Available Commands:
  clear-page          Clears all elements from the given page, which can be a branch or leaf page
  clear-page-elements Clears elements from the given page, which can be a branch or leaf page
  copy-page           Copy page from the source page Id to the destination page Id
  freelist            freelist related surgery commands
  meta                meta page related surgery commands
  revert-meta-page    Revert the meta page to revert the changes performed by the latest transaction

Flags:
  -h, --help   help for surgery

Use "bbolt surgery [command] --help" for more information about a command.
```

#### surgery revert-meta-page

`surgery revert-meta-page` reverts to the previous transaction state by replacing the active meta page with the inactive one.

Usage:
```
$ bbolt surgery revert-meta-page -h
Revert the meta page to revert the changes performed by the latest transaction

Usage:
  bbolt surgery revert-meta-page <bbolt-file> [flags]

Flags:
  -h, --help            help for revert-meta-page
      --output string   path to the filePath db file
```

Example:

  ```bash
  $bbolt surgery revert-meta-page ~/default.etcd/member/snap/db --output reverted.db
  The meta page is reverted.
  ```

  - This is particularly useful when the most recent transaction has corrupted the database and you need to roll back to the previous consistent state.

#### surgery copy-page

`surgery copy-page` copies content from one page to another.

Usage:
```
$ bbolt surgery copy-page -h
Copy page from the source page Id to the destination page Id

Usage:
  bbolt surgery copy-page <bbolt-file> [flags]

Flags:
      --from-page uint    source page Id
  -h, --help              help for copy-page
      --output string     path to the filePath db file
      --to-page uint      destination page Id
```

Example:

  ```bash
  $bbolt surgery copy-page ~/default.etcd/member/snap/db --output copied.db --from-page 3 --to-page 2
  The page 3 was successfully copied to page 2
  WARNING: the free list might have changed.
  Please consider executing `./bbolt surgery freelist abandon ...`
  ```

  - This command is useful for recovering data from damaged pages or creating page backups.

#### surgery clear-page

`surgery clear-page` removes all elements from a branch or leaf page.

Usage:
```
$ bbolt surgery clear-page -h
Clears all elements from the given page, which can be a branch or leaf page

Usage:
  bbolt surgery clear-page <bbolt-file> [flags]

Flags:
  -h, --help            help for clear-page
      --output string   path to the filePath db file
      --pageId uint     page Id
```

Example:

  ```bash
  $bbolt surgery clear-page ~/default.etcd/member/snap/db --output cleared.db --pageId 3
  The page (3) was cleared
  WARNING: The clearing has abandoned some pages that are not yet referenced from free list.
  Please consider executing `./bbolt surgery freelist abandon ...`
  ```

  - The pageId must be at least 2 (meta pages 0 and 1 cannot be cleared).

#### surgery clear-page-elements

`surgery clear-page-elements` removes specific elements from a branch or leaf page by index range.

Usage:
```
$ bbolt surgery clear-page-elements -h
Clears elements from the given page, which can be a branch or leaf page

Usage:
  bbolt surgery clear-page-elements <bbolt-file> [flags]

Flags:
      --from-index int    start element index (included) to clear, starting from 0
  -h, --help              help for clear-page-elements
      --output string     path to the filePath db file
      --pageId uint       page id
      --to-index int      end element index (excluded) to clear, starting from 0, -1 means to the end of page
```

Example:

  ```bash
  $bbolt surgery clear-page-elements ~/default.etcd/member/snap/db --output partial.db --pageId 3 --from-index 1 --to-index 4
  All elements in [1, 4) in page 3 were cleared
  WARNING: The clearing has abandoned some pages that are not yet referenced from free list.
  Please consider executing `./bbolt surgery freelist abandon ...`
  ```

  - Use `--to-index -1` to clear elements to the end of the page.

#### surgery freelist

`surgery freelist` provides commands for managing the database's free page list.

Usage:
```
$ bbolt surgery freelist -h
freelist related surgery commands

Usage:
  bbolt surgery freelist [command]

Available Commands:
  abandon     abandon freelist
  rebuild     rebuild freelist

Flags:
  -h, --help   help for freelist

Use "bbolt surgery freelist [command] --help" for more information about a command.
```

##### surgery freelist abandon

`surgery freelist abandon` removes freelist references from both meta pages, forcing Bbolt to reconstruct the freelist when the database is next opened.

Usage:
```
$ bbolt surgery freelist abandon -h
Abandon the freelist from both meta pages

Usage:
  bbolt surgery freelist abandon <bbolt-file> [flags]

Flags:
  -h, --help            help for abandon
      --output string   path to the filePath db file
```

Example:

  ```bash
  $bbolt surgery freelist abandon ~/default.etcd/member/snap/db --output abandoned.db
  The freelist was abandoned in both meta pages.
  It may cause some delay on next startup because bbolt needs to scan the whole db to reconstruct the free list.
  ```

  - This sets the freelist page ID to a special value indicating the freelist is not present.

##### surgery freelist rebuild

`surgery freelist rebuild` rebuilds the freelist in a database where the freelist has been abandoned.

Usage:
```
$ bbolt surgery freelist rebuild -h
Rebuild the freelist

Usage:
  bbolt surgery freelist rebuild <bbolt-file> [flags]

Flags:
  -h, --help            help for rebuild
      --output string   path to the filePath db file
```

Example:

  ```bash
  $bbolt surgery freelist rebuild ~/default.etcd/member/snap/db --output rebuilt.db
  The freelist was successfully rebuilt.
  ```

  - This command opens the database and lets Bbolt automatically reconstruct and sync the freelist.

#### surgery meta

`surgery meta` provides commands for working with database metadata pages.

Usage:
```
$ bbolt surgery meta -h
meta page related surgery commands

Usage:
  bbolt surgery meta [command]

Available Commands:
  update      update meta page
  validate    validate meta page

Flags:
  -h, --help   help for meta

Use "bbolt surgery meta [command] --help" for more information about a command.
```

##### surgery meta validate

`surgery meta validate` validates the integrity of both meta pages in the database.

Usage:
```
$ bbolt surgery meta validate -h
Validate both meta pages

Usage:
  bbolt surgery meta validate <bbolt-file> [flags]

Flags:
  -h, --help   help for validate
```

Example:

  ```bash
  $bbolt surgery meta validate ~/default.etcd/member/snap/db
  The meta page 0 is valid!
  The meta page 1 is valid!
  ```

  - It checks magic number values, version compatibility, checksum integrity, and general metadata validity for both meta pages (0 and 1).

##### surgery meta update

`surgery meta update` updates specific fields in a meta page for manual repair of corrupted metadata.

Usage:
```
$ bbolt surgery meta update -h
Update fields in meta pages

Usage:
  bbolt surgery meta update <bbolt-file> [flags]

Flags:
      --fields strings     comma separated list of fields (supported fields: pageSize, root, freelist and pgid) to be updated, and each item is a colon-separated key-value pair
  -h, --help               help for update
      --meta-page uint32   the meta page ID to operate on, valid values are 0 and 1
      --output string      path to the filePath db file
```

Supported fields:
- `pageSize` - Size of database pages
- `root` - Root bucket page ID
- `freelist` - Freelist page ID
- `pgid` - Next page ID to allocate

Example:

  ```bash
  $bbolt surgery meta update ~/default.etcd/member/snap/db --output fixed.db --meta-page 0 --fields root:16,freelist:8
  The meta page 0 has been updated!
  ```

  - It updates the specified meta page and automatically updates magic number, version, flags, and checksum to ensure consistency.
