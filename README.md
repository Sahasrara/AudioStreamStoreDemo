# AudioStreamStoreDemo

Benchmark Results

*50 - 118kb Utterances Streamed and Read Simultaneously from One Node in 4096 byte chunks*

Redis

*Notes:*
I used a 50ms polling interval to check for updates to the cache.

*Results:*
Local:
Total Time to stream and read 50 audio files: 226ms.
average complete read and write: 125ms
average time to first byte read: 26ms
average read time per runner: 103ms
average write time per runner: 71ms
p99 time to first byte: 59ms
p90 time to first byte: 57ms
p50 time to first byte: 6ms

Remote (Over SSH | Seattle → IAD):
Total Time to stream and read 50 audio files: 1405ms.
average complete read and write: 1298ms
average time to first byte read: 81ms
average read time per runner: 1277ms
average write time per runner: 1238ms
p99 time to first byte: 148ms
p90 time to first byte: 126ms
p50 time to first byte: 84ms

Remote (Through VIP | Seattle → IAD):
Total Time to stream and read 50 audio files: 2035ms.
average complete read and write: 1245ms
average time to first byte read: 67ms
average read time per runner: 1226ms
average write time per runner: 1034ms
p99 time to first byte: 161ms
p90 time to first byte: 87ms
p50 time to first byte: 74ms


Ignite

*Notes:*
I have a feeling these numbers are artificially inflated.  I think the client is not well built for extreme parallelism.  I believe it's doing quite a bit of locking.  I think if you were to have many nodes doing the same amount of work, the numbers might be better.  This would require more in depth benchmarking.

*Results:*
Local:
Total Time to stream and read 50 audio files: 327ms.
average complete read and write: 321ms
average time to first byte read: 184ms
average read time per runner: 225ms
average write time per runner: 35ms
p99 time to first byte: 212ms
p90 time to first byte: 197ms
p50 time to first byte: 191ms

Remote (Over SSH | Seattle → IAD):
Total Time to stream and read 50 audio files: 5148ms.
average complete read and write: 4483ms
average time to first byte read: 947ms
average read time per runner: 3224ms
average write time per runner: 2779ms
p99 time to first byte: 4936ms
p90 time to first byte: 926ms
p50 time to first byte: 577ms

Remote (Through VIP | Seattle → IAD):
Total Time to stream and read 50 audio files: 4840ms.
average complete read and write: 4287ms
average time to first byte read: 780ms
average read time per runner: 3035ms
average write time per runner: 2562ms
p99 time to first byte: 4458ms
p90 time to first byte: 857ms
p50 time to first byte: 566ms


*1 - 118kb Utterances Streamed and Read Simultaneously from One Node in 4096 byte chunks*

Redis

*Notes:*
I used a 50ms polling interval to check for updates to the cache.

*Results:*
Local:
Total Time to stream and read 1 audio files: 62ms.
average complete read and write: 62ms
average time to first byte read: 55ms
average read time per runner: 61ms
average write time per runner: 3ms
p99 time to first byte: 55ms
p90 time to first byte: 55ms
p50 time to first byte: 55ms

Remote (Over SSH | Seattle → IAD):
Total Time to stream and read 1 audio files: 394ms.
average complete read and write: 394ms
average time to first byte read: 57ms
average read time per runner: 394ms
average write time per runner: 342ms
p99 time to first byte: 57ms
p90 time to first byte: 57ms
p50 time to first byte: 57ms

Remote (Through VIP | Seattle → IAD):
Total Time to stream and read 1 audio files: 388ms.
average complete read and write: 388ms
average time to first byte read: 61ms
average read time per runner: 388ms
average write time per runner: 343ms
p99 time to first byte: 61ms
p90 time to first byte: 61ms
p50 time to first byte: 61ms


Ignite

*Notes:*


*Results:*
Local:
Total Time to stream and read 1 audio files: 32ms.
average complete read and write: 32ms
average time to first byte read: 2ms
average read time per runner: 23ms
average write time per runner: 11ms
p99 time to first byte: 2ms
p90 time to first byte: 2ms
p50 time to first byte: 2ms

Remote (Over SSH | Seattle → IAD):
Total Time to stream and read 1 audio files: 259ms.
average complete read and write: 258ms
average time to first byte read: 19ms
average read time per runner: 232ms
average write time per runner: 169ms
p99 time to first byte: 19ms
p90 time to first byte: 19ms
p50 time to first byte: 19ms

Remote (Through VIP | Seattle → IAD):
Total Time to stream and read 1 audio files: 203ms.
average complete read and write: 203ms
average time to first byte read: 20ms
average read time per runner: 174ms
average write time per runner: 93ms
p99 time to first byte: 20ms
p90 time to first byte: 20ms
p50 time to first byte: 20ms
