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
$> curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
$> curl -XGET -d '{"tag":"a"}' http://localhost:8001/' # query a
$> curl -XGET -d '{"tag":"a", "from": 144658121, "to": 144658324}' 'http://localhost:8001/'' # query a from time id 144658121 to time id 144658324

```
PS: Reading is buffered, so you'll have to push a bunch of messages to see them arrive

#### Writing
```
$> curl -XGET -d '{blablabla}' 'http://localhost:8000/?tags=a&tags=b' # send messages with tags a and b
$> echo -n "hello" >/dev/udp/localhost/8003
```
