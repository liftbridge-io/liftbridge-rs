# liftbridge-rs
[![crates.io Badge]][crates.io package]

Liftbridge client for Rust language. 

## Protobuf definitions
Currently protobuf definitions are copied from the [liftbridge-api](https://github.com/liftbridge-io/liftbridge-api/blob/master/api.proto)  
repo and their rust representation is generated on build and auto-included.

## The current state of the client and roadmap
The client currently supports most of the operations, but it does not support some advanced options  
like a custom partitioner and different ack policies. Therefore the api is subject to change as this  
functionality gets implemented.   

There are a lot of optimizations that could be done in terms of extra allocations, this is coming in future  
versions.

Also, the documentation is pretty much non-existent at the moment and so are the tests. The
client api pretty much follows that of [go client](https://github.com/liftbridge-io/go-liftbridge/),  
which would be a good place to consult should you need any guidance in terms of usage for the time being.

Note that the underlying `ApiClient` is being cloned - this has been done on purpose as it's cheap,  
because the underlying connection is being reused for all the cloned versions of the client.  
The related tonic issue that explains the reasoning behind this can be found [here](https://github.com/hyperium/tonic/issues/33). 

## Contributions
Contributions are most welcome via issues/PRs. It's best to open an issue and discuss it first before committing to implementation
of any features.

## Credits
The initial work on this client was generously sponsored by [Tribe Health Solutions Inc.](http://tribehealthsolutions.com).

[crates.io badge]: https://img.shields.io/crates/v/liftbridge.svg
[crates.io package]: https://crates.io/crates/liftbridge/
