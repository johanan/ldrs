---
title: ldrs-sf for Snowflake
description: Why ldrs-sf exists
---

To put it succinctly , Rust has no Snowflake SDK and the Go Snowflake SDK is very mature and can directly output Arrow RecordBatches.

Longer answer, I am not a C++ developer that I could adapt the C++ Arrow SDK to create a C FFI Arrow interface into ldrs. So I spin up another process that writes Arrow RecordBatches to stdout and reads through stdin.
