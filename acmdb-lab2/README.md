# Lab 2 Document
## Design Decisions
* Page replacement policy: choose the page which is queried the 
least frequently. To achieve this, I maintained a table 
`pageUseTime` which record how many times each page has been
accessed.
* Insertion: just take care of the update of pointers, especially
sibling and parent. And remember to update entry of parent. 
* Deletion: the merge part requires the deletion of 
merged page. And use the method `deleteParentEntry`
to recursively merge parent pages if necessary.

## API Changes
Nope

## Missing or Incomplete Elements
Nope

## How long I spent
I spent about 3 days (2 hours per day), and there aren't anything confusing, I found it quite clear after reading the docs

