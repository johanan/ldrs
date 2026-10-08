# Changelog

All notable changes to this project will be documented in this file.

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
