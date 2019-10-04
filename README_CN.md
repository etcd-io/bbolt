

# Getting Started

## 安装

```
go get go.etcd.io/bbolt/...
```

会 get 两项

1.  go package -> `$GOPATH`
2.  `bolt` command line -> `$GOBIN`

## Open Database

使用 kv 数据库都很简单，只需要一个文件路径即可搭建完成环境。

```
package main

import (
    "log"

    bolt "go.etcd.io/bbolt"
)

func main() {
    // Open the my.db data file in your current directory.
    // It will be created if it doesn't exist.
    db, err := bolt.Open("my.db", 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ...
}
```

这里到 db 不支持多链接。这是因为对于 database file 一个链接保持了一个文件锁 `file lock`。

如果并发，后续链接会阻塞。

可以为单个链接添加 超时控制

```
db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
```

## Transaction

### 本文无关

与 google 的 levelDB 不同，bbolt 支持事务。 [detail](https://link.zhihu.com/?target=https%3A//www.progville.com/go/bolt-embedded-db-golang/) bolt 优缺点：[detail](https://link.zhihu.com/?target=https%3A//npf.io/2014/07/intro-to-boltdb-painless-performant-persistence/) 同时 bbolt 出自 bolt ，没太多不同，只是 bbolt 目前还在维护。

## 事务

### 并发读写

同时只能有

-   一个读写事务
-   多个只读事务

actions⚠️：在事务开始时，会保持一个数据视图 `这意味着事务处理过程中不会由于别处更改而改变`

### 线程安全

单个事务和它所创建的所有对象（桶，键）都不是线程安全的。

建议加锁 或者 每一个 goroutine 并发都开启 一个 事务

当然，从 `db` 这个 bbolt 的顶级结构创建 事务 是 线程安全 的

### 死锁

前面提到的 读写事务 和 只读事务 拒绝相互依赖。当然也不能在同一个 goroutine 里。

死锁原因是 读写事务 需要周期性重新映射 data 文件（即`database`）。这在开启只读事务时是不可行的。

### 读写事务

使用 `db.Update`开启一个读写事务

```
err := db.Update(func(tx *bolt.Tx) error{
    ···
    return nil
})
```

上文提过，在一个事务中 ，数据视图是一样的。 （详细解释就是，在这个函数作用域中，数据对你呈现最终一致性）

### 返回值

bboltdb 根据你的返回值判断事务状态，你可以添加任意逻辑并认为出错时返回 `return err` bboltdb 会回滚，如果 `return nil` 则提交你的事务。

建议永远检查 `Update` 的返回值，因为他会返回如 硬盘压力 等造成事务失败的信息（这是在你的逻辑之外的情况）

⚠️：你自定义返回 error 的 error 信息同样会被传递出来。

### 只读事务

使用 `db.View` 来新建一个 只读事务

```
err := db.View(func(tx *bolt.Tx) error {
    ···
    return nil
})
```

同上，你会获得一个一致性的数据视图。

当然，只读事务 只能检索信息，不会有任何更改。（btw,但是你可以 copy 一个 database 的副本，毕竟这只需要读数据）

### 批量读写事务

读写事务 `db.Update` 最后需要对 `database`提交更改，这会等待硬盘就绪。

每一次文件读写都是和磁盘交互。这不是一个小开销。

你可以使用 `db.Batch` 开启一个 批处理事务。他会在最后批量提交（其实是多个 goroutines 开启 `db.Batch` 事务时有机会合并之后一起提交）从而减小了开销。 ⚠️：`db.Batch` 只对 goroutine 起效

使用 批处理事务 需要做取舍，用 幂等函数 换取 速度 ⚠️： `db.Batch` 在一部分事务失败的时候会尝试多次调用那些事务函数，如果不是幂等会造成不可预知的非最终一致性。

例：使用事务外的变量来使你的日志不那么奇怪

```
var id uint64
err := db.Batch(func(tx *bolt.Tx) error {
    // Find last key in bucket, decode as bigendian uint64, increment
    // by one, encode back to []byte, and add new key.
    ...
    id = newValue
    return nil
})
if err != nil {
    return ...
}
fmt.Println("Allocated ID %d", id)
```

### 手动事务

可以手动进行事务的 开启 ，回滚，新建对象，提交等操作。因为本身 `db.Update` 和 `db.View` 就是他们的包装 ⚠️：手动事务记得 关闭 （Close）

开启事务使用 `db.Begin(bool)` 同时参数代表着是否可以写操作。如下：

-   true - 读写事务
-   false - 只读事务

```
// Start a writable transaction.
tx, err := db.Begin(true)
if err != nil {
    return err
}
defer tx.Rollback()

// Use the transaction...
_, err := tx.CreateBucket([]byte("MyBucket"))
if err != nil {
    return err
}

// Commit the transaction and check for error.
if err := tx.Commit(); err != nil {
    return err
}
```

## Using Store ?

### Using Buckets

桶是键值对的集合。在一个桶中，键值唯一。

### 创建

使用 `Tx.CreateBucket()` 和 `Tx.CreateBucketIfNotExists()` 建立一个新桶（推荐使用第二个） 接受参数是 桶的名字

### 删除

使用 `Tx.DeleteBucket()` 根据桶的名字来删除

### 例子

```
func main() {
    db, err := bbolt.Open("./data", 0666, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    db.Update(func(tx *bbolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
        if err != nil {
            return fmt.Errorf("create bucket: %v", err)
        }

        if err = tx.DeleteBucket([]byte("MyBucket")); err != nil {
            return err
        }

        return nil
    })

}
```

### Using key/value pairs ?

最重要的部分，就是 kv 存储怎么使用了，首先需要一个 桶 来存储键值对。

### Put

使用`Bucket.Put()`来存储键值对，接收两个 `[]byte` 类型的参数

```
db.Update(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("MyBucket"))
    err := b.Put([]byte("answer"), []byte("42"))
    return err
})
```

很明显，上面的例子设置了 Pair: key：answer value：42

### Get

使用 `Bucket.Get()` 来查询键值。参数是一个 `[]byte`（别忘了这次我们只是查询，可以使用 只读事务）

```
db.View(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("MyBucket"))
    v := b.Get([]byte("answer"))
    fmt.Printf("The answer is: %s\n", v)
    return nil
})
```

细心会注意到，`Get`是不会返回 `error` 的，这是因为 `Get()` 一定能正常工作（除非系统错误），相应的，当返回 `nil` 时，查询的键值对不存在。 ⚠️：注意 0 长度的值 和 不存在键值对 的行为是不一样的。（一个返回是 nil， 一个不是）

```
func main() {
    db, err := bolt.Open("./data.db", 0666, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    err = db.Update(func(tx *bolt.Tx) error {
        b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
        if err != nil {
            return fmt.Errorf("create bucket: %v", err)
        }

        if err = b.Put([]byte("answer"), []byte("42")); err != nil {
            return err
        }

        if err = b.Put([]byte("zero"), []byte("")); err != nil {
            return err
        }

        return nil
    })

    db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte("MyBucket"))
        v := b.Get([]byte("noexists"))
        fmt.Println(reflect.DeepEqual(v, nil)) // false
        fmt.Println(v == nil)                  // true

        v = b.Get([]byte("zero"))
        fmt.Println(reflect.DeepEqual(v, nil)) // false
        fmt.Println(v == nil)                  // true
        return nil
    })
}
```

### Delete

使用 `Bucket.Delete()` 删除键值对

```
db.View(func(tx *bolt.Tx) error {
    b := tx.Bucket([]byte("MyBucket"))
    fmt.Println(b.Get([]byte("answer")))

    err := b.Delete([]byte("answer"))
    if err != nil {
        return err
    }
    return nil
})
```

⚠️： `Get()` 获取到的字节切片值只在当前事务（当前函数作用域）有效，如果要在其他事务中使用需要使用 `copy()` 将其拷贝到其他的字节切片

## Tricks

### 桶的自增键

使用 `NextSequence()`来创建自增键，见下例

```
// CreateUser saves u to the store. The new user ID is set on u once the data is persisted.
func (s *Store) CreateUser(u *User) error {
    return s.db.Update(func(tx *bolt.Tx) error {
        // Retrieve the users bucket.
        // This should be created when the DB is first opened.
        b := tx.Bucket([]byte("users"))

        // Generate ID for the user.
        // This returns an error only if the Tx is closed or not writeable.
        // That can't happen in an Update() call so I ignore the error check.
        id, _ := b.NextSequence()
        u.ID = int(id)

        // Marshal user data into bytes.
        buf, err := json.Marshal(u)
        if err != nil {
            return err
        }

        // Persist bytes to users bucket.
        return b.Put(itob(u.ID), buf)
    })
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
    b := make([]byte, 8)
    binary.BigEndian.PutUint64(b, uint64(v))
    return b
}

type User struct {
    ID int
    ...
}
```

### 嵌套桶

很简单的，桶可以实现嵌套存储

```
func (*Bucket) CreateBucket(key []byte) (*Bucket, error)
func (*Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error)
func (*Bucket) DeleteBucket(key []byte) error
```

### 例子

假设您有一个多租户应用程序，其中根级别存储桶是帐户存储桶。该存储桶内部有一系列帐户的序列，这些帐户本身就是存储桶。在序列存储桶（子桶）中，可能有许多相关的存储桶（Users，Note等）。

![](https://cdn.learnku.com/uploads/images/201910/02/41293/PoEk7aj8Fm.jpeg!/fw/1240)

\

```
// createUser creates a new user in the given account.
func createUser(accountID int, u *User) error {
    // Start the transaction.
    tx, err := db.Begin(true)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // Retrieve the root bucket for the account.
    // Assume this has already been created when the account was set up.
    root := tx.Bucket([]byte(strconv.FormatUint(accountID, 10)))

    // Setup the users bucket.
    bkt, err := root.CreateBucketIfNotExists([]byte("USERS"))
    if err != nil {
        return err
    }

    // Generate an ID for the new user.
    userID, err := bkt.NextSequence()
    if err != nil {
        return err
    }
    u.ID = userID

    // Marshal and save the encoded user.
    if buf, err := json.Marshal(u); err != nil {
        return err
    } else if err := bkt.Put([]byte(strconv.FormatUint(u.ID, 10)), buf); err != nil {
        return err
    }

    // Commit the transaction.
    if err := tx.Commit(); err != nil {
        return err
    }

    return nil
}
```

### 遍历键值

在桶中，键值对根据 键 的 值是有字节序的。 使用 `Bucket.Cursor()`对其进行迭代

```
db.View(func(tx *bolt.Tx) error {
    // Assume bucket exists and has keys
    b := tx.Bucket([]byte("MyBucket"))

    c := b.Cursor()

    for k, v := c.First(); k != nil; k, v = c.Next() {
        fmt.Printf("key=%s, value=%s\n", k, v)
    }

    return nil
})
```

Cursor 有 5 种方法进行迭代

1.  `First()` Move to the first key.
2.  `Last()` Move to the last key.
3.  `Seek()` Move to a specific key.\

4.  `Next()` Move to the next key.\

5.  `Prev()` Move to the previous key.

每一个方法都返回 `(key []byte, value []byte)` 两个值 当方法所指值不存在时返回 两个 `nil` 值，发生在以下情况：

1.  迭代到最后一个键值对时，再一次调用 `Cursor.Next()`
2.  当前所指为第一个键值对时，调用 `Cursor.Prev()`
3.  当使用 4.`Next()` 和 5. `Prev()`方法而未使用 1.`First()` 2.`Last()` 3. `Seek()`指定初始位置时

⚠️特殊情况：当 `key` 为 非 `nil` 但 `value` 是 `nil` 是，说明这是嵌套桶，`value` 值是子桶，使用 `Bucket.Bucket()` 方法访问 子桶，参数是 `key` 值

```
db.View(func(tx *bolt.Tx) error {
    c := b.Cursor()
    fmt.Println(c.First())
    k, v := c.Prev()
    fmt.Println(k == nil, v == nil) // true,true

    if k != nil && v == nil {
        subBucket := b.Bucket()
        // doanything
    }
    return nil
})
```

### 前缀遍历

通过使用 `Cursor`我们能够做到一些特殊的遍历，如：遍历拥有特定前缀的 键值对

```
db.View(func(tx *bolt.Tx) error {
    // Assume bucket exists and has keys
    c := tx.Bucket([]byte("MyBucket")).Cursor()

    prefix := []byte("1234")
    for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
        fmt.Printf("key=%s, value=%s\n", k, v)
    }

    return nil
})
```

### 范围遍历

在一个范围里遍历，如：使用可排序的时间编码（RFC3339）可以遍历特定日期范围的数据

```
db.View(func(tx *bolt.Tx) error {
    // Assume our events bucket exists and has RFC3339 encoded time keys.
    c := tx.Bucket([]byte("Events")).Cursor()

    // Our time range spans the 90's decade.
    min := []byte("1990-01-01T00:00:00Z")
    max := []byte("2000-01-01T00:00:00Z")

    // Iterate over the 90's.
    for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
        fmt.Printf("%s: %s\n", k, v)
    }

    return nil
})
```

⚠️：Golang 实现的 RFC3339Nano 是不可排序的

### ForEach

在桶中有值的情况下，可以使用 `ForEach()`遍历

```
db.View(func(tx *bolt.Tx) error {
    // Assume bucket exists and has keys
    b := tx.Bucket([]byte("MyBucket"))

    b.ForEach(func(k, v []byte) error {
        fmt.Printf("key=%s, value=%s\n", k, v)
        return nil
    })
    return nil
})
```

⚠️：在 `ForEach()`中遍历的键值对需要`copy()`到事务外才能在事务外使用

## Advance

### Backup

boltdb 是一个单一的文件，所以很容易备份。你可以使用`Tx.writeto()`函数写一致的数据库。如果从只读事务调用这个函数，它将执行热备份，而不会阻塞其他数据库的读写操作。

默认情况下，它将使用一个常规文件句柄，该句柄将利用操作系统的页面缓存。

有关优化大于RAM数据集的信息，请参见`[Tx](https://link.zhihu.com/?target=https%3A//godoc.org/go.etcd.io/bbolt%23Tx)`文档。

一个常见的用例是在HTTP上进行备份，这样您就可以使用像`cURL`这样的工具来进行数据库备份：

```
func BackupHandleFunc(w http.ResponseWriter, req *http.Request) {
    err := db.View(func(tx *bolt.Tx) error {
        w.Header().Set("Content-Type", "application/octet-stream")
        w.Header().Set("Content-Disposition", `attachment; filename="my.db"`)
        w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
        _, err := tx.WriteTo(w)
        return err
    })
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}
```

然后您可以使用此命令进行备份：

`$ curl http://localhost/backup > my.db`

或者你可以打开你的浏览器以[http://localhost/backup](https://link.zhihu.com/?target=http%3A//localhost/backup)，它会自动下载。

如果你想备份到另一个文件，你可以使用`TX.copyfile()`辅助功能。

### Statistics

数据库对运行的许多内部操作保持一个运行计数，这样您就可以更好地了解发生了什么。通过捕捉两个时间点数据的快照，我们可以看到在这个时间范围内执行了哪些操作。

例如，我们可以用一个 goroutine 里记录统计每一个 10 秒：

```
go func() {
    // Grab the initial stats.
    prev := db.Stats()
    for {
        // Wait for 10s.
        time.Sleep(10 * time.Second)
        // Grab the current stats and diff them.
        stats := db.Stats()
        diff := stats.Sub(&prev)
        // Encode stats to JSON and print to STDERR.
        json.NewEncoder(os.Stderr).Encode(diff)
        // Save stats for the next loop.
        prev = stats
    }
}()
```

将这些信息通过管道输出到监控也很有用。

### Read-Only Mode

可以开启只读模式防止错误更改

```
db, err := bolt.Open("my.db", 0666, &bolt.Options{ReadOnly: true})
if err != nil {
    log.Fatal(err)
}
```

现在使用 `db.Update()` 等开启读写事务 将会阻塞

### Mobile Use

移动端支持由 [gomobile](https://link.zhihu.com/?target=https%3A//github.com/golang/mobile) 工具提供

Create a struct that will contain your database logic and a reference to a `*bolt.DB` with a initializing constructor that takes in a filepath where the database file will be stored. Neither Android nor iOS require extra permissions or cleanup from using this method.

```
func NewBoltDB(filepath string) *BoltDB {
 db, err := bolt.Open(filepath+"/demo.db", 0600, nil)
 if err != nil {
    log.Fatal(err)
 }
 return &BoltDB{db}
}
type BoltDB struct {
 db *bolt.DB
 ...
}
func (b *BoltDB) Path() string {
 return b.db.Path()
}
func (b *BoltDB) Close() {
 b.db.Close()
}
```

Database logic should be defined as methods on this wrapper struct. To initialize this struct from the native language (both platforms now sync their local storage to the cloud. These snippets disable that functionality for the database file):

### Android

```
String path;
if (android.os.Build.VERSION.SDK_INT >=android.os.Build.VERSION_CODES.LOLLIPOP){
    path = getNoBackupFilesDir().getAbsolutePath();
} else{
    path = getFilesDir().getAbsolutePath();
}
Boltmobiledemo.BoltDB boltDB = Boltmobiledemo.NewBoltDB(path)
```

### iOS

```
- (void)demo {
    NSString* path = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory,
                                                          NSUserDomainMask,
                                                          YES) objectAtIndex:0];
 GoBoltmobiledemoBoltDB * demo = GoBoltmobiledemoNewBoltDB(path);
 [self addSkipBackupAttributeToItemAtPath:demo.path];
 //Some DB Logic would go here
 [demo close];
}
- (BOOL)addSkipBackupAttributeToItemAtPath:(NSString *) filePathString
{
    NSURL* URL= [NSURL fileURLWithPath: filePathString];
    assert([[NSFileManager defaultManager] fileExistsAtPath: [URL path]]);
    NSError *error = nil;
    BOOL success = [URL setResourceValue: [NSNumber numberWithBool: YES]
                                  forKey: NSURLIsExcludedFromBackupKey error: &error];
    if(!success){
        NSLog(@"Error excluding %@ from backup %@", [URL lastPathComponent], error);
    }
    return success;
}
```

## 扩展阅读

## 更多指导

For more information on getting started with Bolt, check out the following articles:

-   [Intro to BoltDB: Painless Performant Persistence](https://link.zhihu.com/?target=http%3A//npf.io/2014/07/intro-to-boltdb-painless-performant-persistence/) by [Nate Finch](https://link.zhihu.com/?target=https%3A//github.com/natefinch).
-   [Bolt -- an embedded key/value database for Go](https://link.zhihu.com/?target=https%3A//www.progville.com/go/bolt-embedded-db-golang/) by Progville

## 与其他数据库的比较

### Postgres，MySQL和其他关系数据库

关系数据库将数据组织成行，并且只能通过使用SQL进行访问。这种方法在存储和查询数据方面提供了灵活性，但是在解析和计划SQL语句时也会产生开销。Bolt通过字节切片键访问所有数据。这使得Bolt可以快速地通过键读取和写入数据，但是不提供将值连接在一起的内置支持。 大多数关系数据库（SQLite除外）都是独立于应用程序运行的独立服务器。这使您的系统具有将多个应用程序服务器连接到单个数据库服务器的灵活性，但同时也增加了在网络上序列化和传输数据的开销。Bolt作为应用程序中包含的库运行，因此所有数据访问都必须经过应用程序的过程。这使数据更接近您的应用程序，但限制了对数据的多进程访问。

### LevelDB，RocksDB

LevelDB及其派生类（RocksDB，HyperLevelDB）与Bolt类似，因为它们是捆绑到应用程序中的库，但是它们的底层结构是日志结构的合并树（LSM树）。LSM树通过使用预写日志和称为SSTables的多层排序文件来优化随机写入。Bolt在内部使用B +树，并且仅使用一个文件。两种方法都需要权衡。 如果您需要较高的随机写入吞吐量（> 10,000 w / sec），或者需要使用旋转磁盘，那么LevelDB可能是一个不错的选择。如果您的应用程序是大量读取或进行大量范围扫描，那么Bolt可能是一个不错的选择。 另一个重要的考虑因素是LevelDB没有事务。它支持键/值对的批量写入，并且支持读取快照，但不能使您安全地执行比较和交换操作。Bolt支持完全可序列化的ACID事务。

### LMDB

Bolt最初是LMDB的端口，因此在架构上相似。两者都使用B +树，具有ACID语义和完全可序列化的事务，并支持使用单个写入器和多个读取器的无锁MVCC。 这两个项目有些分歧。LMDB专注于原始性能，而Bolt专注于简单性和易用性。例如，出于性能考虑，LMDB允许执行几种不安全的操作，例如直接写入。Bolt选择禁止可能使数据库处于损坏状态的操作。Bolt唯一的例外是`DB.NoSync`。 API也有一些区别。打开LMDB时需要最大的mmap大小，`mdb_env`而Bolt会自动处理增量mmap的大小。LMDB使用多个标志来重载getter和setter函数，而Bolt将这些特殊情况拆分为自己的函数。

## 注意事项和局限性

选择合适的工具来完成这项工作很重要，而Bolt也不例外。在评估和使用Bolt时，需要注意以下几点：

-   Bolt非常适合读取密集型工作负载。顺序写入性能也很快，但是随机写入可能会很慢。您可以使用`DB.Batch()`或添加预写日志来帮助缓解此问题。

-   Bolt在内部使用B + tree，因此可以有很多随机页面访问。与旋转磁盘相比，SSD可以显着提高性能。

-   尝试避免长时间运行的读取事务。Bolt使用写时复制功能，因此在旧事务使用旧页时无法回收这些旧页。

-   从Bolt返回的字节片仅在事务期间有效。一旦事务被提交或回滚，它们所指向的内存就可以被新页面重用，或者可以从虚拟内存中取消映射，`unexpected fault address`访问时会出现恐慌。

-   Bolt在数据库文件上使用排他写锁定，因此不能被多个进程共享。

-   使用时要小心`Bucket.FillPercent`。为具有随机插入的存储桶设置较高的填充百分比将导致数据库的页面利用率非常差。

-   通常使用较大的水桶。较小的存储桶一旦超过页面大小（通常为4KB），就会导致页面利用率下降。

-   批量加载大量随机写入新的存储桶可能很慢，因为在提交事务之前页面不会拆分。建议不要在单个事务中将100,000个以上的键/值对随机插入到一个新的存储桶中。

-   Bolt使用内存映射文件，因此底层操作系统可以处理数据的缓存。通常，操作系统将在内存中缓存尽可能多的文件，并根据需要将内存释放给其他进程。这意味着在使用大型数据库时，Bolt可能会显示很高的内存使用率。但是，这是预料之中的，操作系统将根据需要释放内存。只要Bolt的内存映射适合进程虚拟地址空间，它就可以处理比可用物理RAM大得多的数据库。在32位系统上可能会出现问题。

-   Bolt数据库中的数据结构是内存映射的，因此数据文件将是特定于字节序的。这意味着您无法将Bolt文件从小字节序计算机复制到大字节序计算机并使其正常工作。对于大多数用户而言，这不是问题，因为大多数现代CPU的字节序都很少。

-   由于页面在磁盘上的布局方式，Bolt无法截断数据文件并将可用页面返回到磁盘。取而代之的是，Bolt会在其数据文件中维护未使用页面的空闲列表。这些空闲页面可以被以后的事务重用。由于数据库通常会增长，因此这在许多用例中效果很好。但是，请务必注意，删除大块数据将不允许您回收磁盘上的该空间。 有关页面分配的更多信息，[请参见此注释](https://link.zhihu.com/?target=https%3A//github.com/boltdb/bolt/issues/308%23issuecomment-74811638)。

## 阅读资料

对于嵌入式，可序列化的事务性键/值数据库，Bolt是一个相对较小的代码库（<5KLOC），因此对于那些对数据库的工作方式感兴趣的人来说，Bolt可能是一个很好的起点。

最佳起点是Bolt的主要切入点：

-   `Open()`-初始化对数据库的引用。它负责创建数据库（如果不存在），获得文件的排他锁，读取元页面以及对文件进行内存映射。

-   `DB.Begin()`-根据`writable`参数的值启动只读或读写事务。这需要短暂获得“元”锁以跟踪未结交易。一次只能存在一个读写事务，因此在读写事务期间将获得“ rwlock”。

-   `Bucket.Put()`-将键/值对写入存储桶。验证参数之后，使用光标将B +树遍历到将键和值写入的页面和位置。找到位置后，存储桶会将基础页面和页面的父页面具体化为“节点”到内存中。这些节点是在读写事务期间发生突变的地方。提交期间，这些更改将刷新到磁盘。

-   `Bucket.Get()`-从存储桶中检索键/值对。这使用光标移动到键/值对的页面和位置。在只读事务期间，键和值数据将作为对基础mmap文件的直接引用返回，因此没有分配开销。对于读写事务，此数据可以引用mmap文件或内存节点值之一。

-   `Cursor`-该对象仅用于遍历磁盘页或内存节点的B +树。它可以查找特定的键，移至第一个或最后一个值，也可以向前或向后移动。光标对最终用户透明地处理B +树的上下移动。

-   `Tx.Commit()`-将内存中的脏节点和可用页面列表转换为要写入磁盘的页面。然后写入磁盘分为两个阶段。首先，脏页被写入磁盘并`fsync()`发生。其次，写入具有递增的事务ID的新元页面，然后`fsync()`发生另一个页面 。这两个阶段的写入操作确保崩溃时会忽略部分写入的数据页，因为指向它们的元页不会被写入。部分写入的元页面是无效的，因为它们是用校验和写入的。

如果您还有其他可能对他人有用的注释，请通过请求请求将其提交。

## 其他使用螺栓的项目

以下是使用Bolt的公共开源项目的列表：

-   [Algernon](https://link.zhihu.com/?target=https%3A//github.com/xyproto/algernon) - A HTTP/2 web server with built-in support for Lua. Uses BoltDB as the default database backend.
-   [Bazil](https://link.zhihu.com/?target=https%3A//bazil.org/) - A file system that lets your data reside where it is most convenient for it to reside.
-   [bolter](https://link.zhihu.com/?target=https%3A//github.com/hasit/bolter) - Command-line app for viewing BoltDB file in your terminal.
-   [boltcli](https://link.zhihu.com/?target=https%3A//github.com/spacewander/boltcli) - the redis-cli for boltdb with Lua script support.
-   [BoltHold](https://link.zhihu.com/?target=https%3A//github.com/timshannon/bolthold) - An embeddable NoSQL store for Go types built on BoltDB
-   [BoltStore](https://link.zhihu.com/?target=https%3A//github.com/yosssi/boltstore) - Session store using Bolt.
-   [Boltdb Boilerplate](https://link.zhihu.com/?target=https%3A//github.com/bobintornado/boltdb-boilerplate) - Boilerplate wrapper around bolt aiming to make simple calls one-liners.
-   [BoltDbWeb](https://link.zhihu.com/?target=https%3A//github.com/evnix/boltdbweb) - A web based GUI for BoltDB files.
-   [bleve](https://link.zhihu.com/?target=http%3A//www.blevesearch.com/) - A pure Go search engine similar to ElasticSearch that uses Bolt as the default storage backend.
-   [btcwallet](https://link.zhihu.com/?target=https%3A//github.com/btcsuite/btcwallet) - A bitcoin wallet.
-   [buckets](https://link.zhihu.com/?target=https%3A//github.com/joyrexus/buckets) - a bolt wrapper streamlining simple tx and key scans.
-   [cayley](https://link.zhihu.com/?target=https%3A//github.com/google/cayley) - Cayley is an open-source graph database using Bolt as optional backend.
-   [ChainStore](https://link.zhihu.com/?target=https%3A//github.com/pressly/chainstore) - Simple key-value interface to a variety of storage engines organized as a chain of operations.
-   [Consul](https://link.zhihu.com/?target=https%3A//github.com/hashicorp/consul) - Consul is service discovery and configuration made easy. Distributed, highly available, and datacenter-aware.
-   [DVID](https://link.zhihu.com/?target=https%3A//github.com/janelia-flyem/dvid) - Added Bolt as optional storage engine and testing it against Basho-tuned leveldb.
-   [dcrwallet](https://link.zhihu.com/?target=https%3A//github.com/decred/dcrwallet) - A wallet for the Decred cryptocurrency.
-   [drive](https://link.zhihu.com/?target=https%3A//github.com/odeke-em/drive) - drive is an unofficial Google Drive command line client for *NIX operating systems.
-   [event-shuttle](https://link.zhihu.com/?target=https%3A//github.com/sclasen/event-shuttle) - A Unix system service to collect and reliably deliver messages to Kafka.
-   [Freehold](https://link.zhihu.com/?target=http%3A//tshannon.bitbucket.org/freehold/) - An open, secure, and lightweight platform for your files and data.
-   [Go Report Card](https://link.zhihu.com/?target=https%3A//goreportcard.com/) - Go code quality report cards as a (free and open source) service.
-   [GoWebApp](https://link.zhihu.com/?target=https%3A//github.com/josephspurrier/gowebapp) - A basic MVC web application in Go using BoltDB.
-   [GoShort](https://link.zhihu.com/?target=https%3A//github.com/pankajkhairnar/goShort) - GoShort is a URL shortener written in Golang and BoltDB for persistent key/value storage and for routing it's using high performent HTTPRouter.
-   [gopherpit](https://link.zhihu.com/?target=https%3A//github.com/gopherpit/gopherpit) - A web service to manage Go remote import paths with custom domains
-   [Gitchain](https://link.zhihu.com/?target=https%3A//github.com/gitchain/gitchain) - Decentralized, peer-to-peer Git repositories aka "Git meets Bitcoin".
-   [InfluxDB](https://link.zhihu.com/?target=https%3A//influxdata.com/) - Scalable datastore for metrics, events, and real-time analytics.
-   [ipLocator](https://link.zhihu.com/?target=https%3A//github.com/AndreasBriese/ipLocator) - A fast ip-geo-location-server using bolt with bloom filters.
-   [ipxed](https://link.zhihu.com/?target=https%3A//github.com/kelseyhightower/ipxed) - Web interface and api for ipxed.
-   [Ironsmith](https://link.zhihu.com/?target=https%3A//github.com/timshannon/ironsmith) - A simple, script-driven continuous integration (build - > test -> release) tool, with no external dependencies
-   [Kala](https://link.zhihu.com/?target=https%3A//github.com/ajvb/kala) - Kala is a modern job scheduler optimized to run on a single node. It is persistent, JSON over HTTP API, ISO 8601 duration notation, and dependent jobs.
-   [Key Value Access Langusge (KVAL)](https://link.zhihu.com/?target=https%3A//github.com/kval-access-language) - A proposed grammar for key-value datastores offering a bbolt binding.
-   [LedisDB](https://link.zhihu.com/?target=https%3A//github.com/siddontang/ledisdb) - A high performance NoSQL, using Bolt as optional storage.
-   [lru](https://link.zhihu.com/?target=https%3A//github.com/crowdriff/lru) - Easy to use Bolt-backed Least-Recently-Used (LRU) read-through cache with chainable remote stores.
-   [mbuckets](https://link.zhihu.com/?target=https%3A//github.com/abhigupta912/mbuckets) - A Bolt wrapper that allows easy operations on multi level (nested) buckets.
-   [MetricBase](https://link.zhihu.com/?target=https%3A//github.com/msiebuhr/MetricBase) - Single-binary version of Graphite.
-   [MuLiFS](https://link.zhihu.com/?target=https%3A//github.com/dankomiocevic/mulifs) - Music Library Filesystem creates a filesystem to organise your music files.
-   [NATS](https://link.zhihu.com/?target=https%3A//github.com/nats-io/nats-streaming-server) - NATS Streaming uses bbolt for message and metadata storage.
-   [Operation Go: A Routine Mission](https://link.zhihu.com/?target=http%3A//gocode.io/) - An online programming game for Golang using Bolt for user accounts and a leaderboard.
-   [photosite/session](https://link.zhihu.com/?target=https%3A//godoc.org/bitbucket.org/kardianos/photosite/session) - Sessions for a photo viewing site.
-   [Prometheus Annotation Server](https://link.zhihu.com/?target=https%3A//github.com/oliver006/prom_annotation_server) - Annotation server for PromDash & Prometheus service monitoring system.
-   [reef-pi](https://link.zhihu.com/?target=https%3A//github.com/reef-pi/reef-pi) - reef-pi is an award winning, modular, DIY reef tank controller using easy to learn electronics based on a Raspberry Pi.
-   [Request Baskets](https://link.zhihu.com/?target=https%3A//github.com/darklynx/request-baskets) - A web service to collect arbitrary HTTP requests and inspect them via REST API or simple web UI, similar to [RequestBin](https://link.zhihu.com/?target=http%3A//requestb.in/) service
-   [Seaweed File System](https://link.zhihu.com/?target=https%3A//github.com/chrislusf/seaweedfs) - Highly scalable distributed key~file system with O(1) disk read.
-   [stow](https://link.zhihu.com/?target=https%3A//github.com/djherbis/stow) - a persistence manager for objects backed by boltdb.
-   [Storm](https://link.zhihu.com/?target=https%3A//github.com/asdine/storm) - Simple and powerful ORM for BoltDB.
-   [SimpleBolt](https://link.zhihu.com/?target=https%3A//github.com/xyproto/simplebolt) - A simple way to use BoltDB. Deals mainly with strings.
-   [Skybox Analytics](https://link.zhihu.com/?target=https%3A//github.com/skybox/skybox) - A standalone funnel analysis tool for web analytics.
-   [Scuttlebutt](https://link.zhihu.com/?target=https%3A//github.com/benbjohnson/scuttlebutt) - Uses Bolt to store and process all Twitter mentions of GitHub projects.
-   [tentacool](https://link.zhihu.com/?target=https%3A//github.com/optiflows/tentacool) - REST api server to manage system stuff (IP, DNS, Gateway...) on a linux server.
-   [torrent](https://link.zhihu.com/?target=https%3A//github.com/anacrolix/torrent) - Full-featured BitTorrent client package and utilities in Go. BoltDB is a storage backend in development.
-   [Wiki](https://link.zhihu.com/?target=https%3A//github.com/peterhellberg/wiki) - A tiny wiki using Goji, BoltDB and Blackfriday.

If you are using Bolt in a project please send a pull request to add it to the list.
