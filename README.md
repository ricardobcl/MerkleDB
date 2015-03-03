# BasicDB


## Benchmarks

TODO

## Building

#### Note for Erlang 17+
For erlang 17+, you have to remove `warnings_as_errors` from `rebar.config` in
the following projects in the `deps` folder:

* poolboy
* meck
* riak_core

```shell
#!/usr/bin/env sh

sed -i 's/, warnings_as_errors//' deps/poolboy/rebar.config
sed -i 's/warnings_as_errors, //' deps/meck/rebar.config
sed -i 's/warnings_as_errors, //' deps/riak_core/rebar.config
```

#### Normal release

```shell
> rake rel
```

#### 4 node dev cluster

```shell
> rake dev
```
