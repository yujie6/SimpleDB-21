# Lab 1 Document
## Design Decisions
* I use `ArrayList.iterator()` to generate `HeapPage`'s iterator.
* Use `HashMap` to store buffered pages.
* Use `InputFileStream` to access file.

## API Changes
Nope

## Missing or Incomplete Elements
Nope

## How long I spent
I spent about 2 days, and I used half of the time debugging the last test.
One confusing part is the usage of `readNBytes` in java, the offset is not about the file's offset
but the target's offset. And we should use `skip()` to adjust file's offset, it took me quite some time
to find out. Other parts are quite easy.

