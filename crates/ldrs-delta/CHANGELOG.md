# Changelog

All notable changes to this project will be documented in this file.

## [0.1.5] - 2026-11-13

### Bug Fixes

- *(delta)* Protocol upgrade, txn guard, inline DVs, INT96 optimize

Fix for legacy protocol upgrades, or skipping them.

txn version failure fix

Inline DVs correctly pad to multiples of 4 (Spark compat)

Optimize handles INT96 and nanosecond columns

- *(parquet)* INT96 as micros, max_bytes, abort upload

Always upgrade INT96 to micros.

max_bytes counts in memory row groups now.

Better abort on failures to not leave uploads hanging around.

- *(postgres)* Out-of-range dates error, deprecate role

Properly errors out bad dates instead of NULL.

role config option is deprecated. Use the options=-c role
instead

- *(storage)* Reserved characters in paths

Properly handle #,?, and % in a path.


### Refactor

- *(parquet)* Drop unused write_parquet functions

Everything writes through the sink and this was code that just was used
in tests.


## [0.1.4] - 2026-10-13

### Bug Fixes

- *(delta)* Bump kernel to 0.29.0

Flipped physical name to kernel functions.
Vacuum scan now only pulls data it uses and fixed a small url bug.


## [0.1.3] - 2026-10-08

### Bug Fixes

- *(delta)* Columns lookup by name

Previously ldrs looked up by index which could be wrong if there was a
nested column.

Readded stats are now correct when a DV is added.

Merge keys and the watermark columns are checked before anything is
written or created.


## [0.1.2] - 2026-09-23

### Dependencies

- *(deps)* Bump delta-kernel to 0.28

delta_kernel and delta_kernel_default_engine move to 0.28.0.


## [0.1.1] - 2026-09-17

### Bug Fixes

- *(storage)* Resolve azure url

Standardize the way the different azure urls resolve into a base that
can be compared.


### Refactor

- *(delta)* Name the caller and this library in engineInfo

commitInfo.engineInfo is now written that correctly reflects the version
of ldrs and ldrs-delta.
