### no
well.. lets see

#### Running 
```
#> node rw.js --writer 8000 --searcher 8001 &

# must have only one writer per box, but can have multiple searchers

#> node rw.js --writer 8000 &
#> node rw.js --searcher 8001 &
#> node rw.js --searcher 8002 &
#> node rw.js --searcher 8003 &

```

#### Reading
```
$ curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
$ curl -XGET -d '{"tag":"any"}' http://localhost:8001/' # query a

```
PS: Reading is buffered, so you'll have to push a bunch of messages to see them arrive

#### Writing
```

$ node json_to_protobuf.js '{"header":{"node_id": 0, "time_id": 0, "offset": 0, "tags":["a","b"]},"frames":[{"data": "AAAAAAAAAAA"},{"data": "BBBBBBBBBB","id":"b"}]}' > example.pb
$ curl -silent -XGET --data-binary @example.pb 'http://localhost:8000/' # send messages with tags a and b
$ cat example.pb >/dev/udp/localhost/8003

```
