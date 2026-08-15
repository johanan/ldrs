# Changelog

All notable changes to this project will be documented in this file.

## [0.22.3] - 2026-08-15

### Bug Fixes

- *(duckdb)* Fixed s3 credentials

Used object_store and mapped these over to the correct duckdb
credentials.


### Dependencies

- *(deps)* Bump delta-kernel to 0.26 and parquet to 59

Upgrading core dependencies to stay on the leading edge.


### Refactor

- *(parquet)* Replace deprecated reader and writer

Parquet 59 introduced a new type SpawnedReader. This makes the handle
usage more clear.

The writer we used also is deprecated and it was replaced with BufWriter


## [0.22.2] - 2026-08-13

### Bug Fixes

- *(delta)* Added vacuum

ldrs will now run vacuum on delta tables. You can target all the tables
in a YAML load or give an object_store URL.


## [0.22.1] - 2026-08-05

### Refactor

- *(delta)* Cloud io runtime to delta

Delta kernel engine was using its own runtime. ldrs now explicitly
passes down the runtime used for object_store io.

- *(delta)* Checkpoint writes

Delta writing will now add a checkpoint every 10 versions. This is not
on the tenth version, but rather if there are ever more than 10 or more
versions since the last checkpoint it will write one.


## [0.22.0] - 2026-08-03

### Bug Fixes

- *(spawned)* Stdin, stdout, and stderr updates

Spawned tools now have better handling for all the pipes in and out of
the process.

Better error handling with each pipe.

- *(env)* Simplified env resolution for spawned tools

Environemnts now have a clean split. ambient is all env vars that do not
start with LDRS_*.

And managed is then the subset of those that each tool wants to put back
into the spawned process.

Cleanly injected into the function to make them testable and not have
each function reliant on the environment.

- *(pg)* Read role from url

Fixes earlier url processing where the role was a non-standard param in
the PG url.

Now it uses -c role=role which is the valid way that PG will bind this.
Also removed URL parse so that PG can be keyword or have non-standard
URLs and still parse correctly.


### Chore

- *(duckdb)* Remaining code to make this work

Required code.


### Features

- *(duckdb)* DuckDB source

ldrs can spawn a duckdb binary and stream the Arrow IPC output.

Many config and credential helpers make it easy to just add SQL and let
duckdb read all the data into Arrow.


## [0.21.3] - 2026-07-24

### Bug Fixes

- *(delta)* Int/decimal fix

ldrs did not have a section to change integer columns back to decimal
when the logical type was decimal. This created issues when comparing
the columns.

- *(delta)* Fix file deletion vectors

A missing trailing slash created a bad join on the root to the deletion
vector file. Switched to the snapshot table root and it will always be
correct.

- *(finalize)* Added columns, delta output, full url, and lua helpers

Not much more to add, a lot more data flows into the finalize process.


### Performance

- *(delta,parquet)* Remove object-store requests

ldrs writes the parquets so we capture the file size after closing. Then
we use that with delta to avoid making an extra call to object-store to
get size and modified timestamp.


### Refactor

- *(config)* Stdout guard moved to cli binary

The terminal check moved to ldrs instead of core as that is the correct
abstraction.


## [0.21.2] - 2026-07-21

### Bug Fixes

- *(delta)* New delta version and dv fix

Upgraded to delta-kernel-rs to 0.25.0.

Fixed a known issue where a checkpoint would create issues with DV
descriptors. This is fixed by using the engines methods to find the
correct dv info.

- *(postgres)* Pg pools now immutable

The shared pg pool hashmap did not need to be mutable. ldrs will
construct all the connections up front and use that through all tasks.

- *(object_store)* AWS and GCP

Adding the other 2 big cloud providers. Fixed infer_env_type to properly
handle the new options.


### Refactor

- *(core)* Execution and config are split

ldrs-core holds resolved execution primitives that higher level crates
can build. The current ldrs crate now just parses YAML config and calls
ldrs-core.


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
