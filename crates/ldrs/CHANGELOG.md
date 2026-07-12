# Changelog

All notable changes to this project will be documented in this file.

## [0.21.1] - 2026-07-12

### Bug Fixes

- *(parquet)* Type resolution improvements

Bug fix for defaults leaking through. Now ldrs uses the correct decimal
datatype in arrow. And it does not default everything it does not know
to text, but will fallback to what arrow maps the column as.

- *(sf)* Better ldrs-sf handling

ldrs will now correctly bind params in the order defined in the config.

ldrs can handle multiple ldrs-sf connections in the env and execute each
ldrs-sf that owns its own env.


## [0.21.0] - 2026-07-09

### Features

- *(finalize)* Added post load Lua phase finalize

Added a phase after destinations have loaded that takes a Lua script and
can run arbitrary commands after. This is only tied to Snowflake now,
but is perfect for COPY, CREATE, etc.


## [0.20.1] - 2026-07-01

### Bug Fixes

- *(config)* Target and url lookups

URL will correct check for shouty snake_case for env vars for
destination url.

target was added so that the landing place can be different from the
source name.

Types and structs added for finalize which will allow Lua to run over
the results of what was completed.


## [0.20.0] - 2026-06-27

### Bug Fixes

- *(ldrs-storage)* Regression on file://

This started resolving to root instead. This is the fix and also
pointing towards the evenutal usage which is to only use file:// for
absolute paths and relative paths will be just strings. like rel/path or
even just .


### Features

- *(sinks)* Multi-destination pipelines

ldrs can now write the same source to multiple destinations in the same
load. This comes with a new v2 config that is roughly the same.

v1 configs will still parse and work, for now.


## [0.19.0] - 2026-06-24

### Features

- *(sinks)* Turn all destinations into sinks

## [0.18.1] - 2026-05-28

### Bug Fixes

- *(cli)* Usage cleanup and tracing filters

## [0.18.0] - 2026-05-20

### Features

- Ldrs run, arrow stdout, and ldrs schema
- Config validation

### Refactor

- *(config)* Pulling out task executor

## [0.17.4](https://github.com/johanan/ldrs/compare/v0.17.3...v0.17.4) - 2026-05-08

### Added

- engine crate split and docs start

### Other

- release v0.17.3

## [0.17.3](https://github.com/johanan/ldrs/releases/tag/v0.17.3) - 2026-04-24

### Other

- *(release-plz)* adding release-plz
- chore(move to workspace):
