# Changelog



## Version 2019.05 (2019-05-28)

### New Features
- [manager] config is a standalone state
- [manager] add base CometError
- [manager] use file name if caller is not a module
- [manager] add register_start()
- [manager] add public functions
- [broker] add opt-out for dump
- [broker] add output: which dump files are read
- [dumper] dump in locked timestamped files


### Bug Fixes
- [script] use python 3.7
- [__init__] only import Dumper for python>2
- [manager] remove all functionality but register config for now
- [broker] use sanic instead of flask
- [broker] don't reply before dump is finished
- [broker] use DEFAULT_PORT were needed
- [broker] use UTC for all timestamps
- [broker]: only dump hash of re-registered states
- [broker]: sort files by date on recover
- [dumper] remove debug print
- allow state and type to be None for re-register
