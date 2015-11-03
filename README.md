### no
well.. lets see

#### Running 
```#> node rw.js &```

#### Reading
```
$> curl -XGET -d '{"and":[{"or":[{"tag":"a"}]},{"or":[{"tag":"b"},{"tag":"a"}]}]}' http://localhost:8001/' # query AND(OR(a),OR(b,a))
$> curl -XGET -d '{"tag":"a"}' http://localhost:8001/' # query a
```
PS: Reading is buffered, so you'll have to push a bunch of messages to see them arrive

#### Writing
```
$> curl -XGET -d '{blablabla}' 'http://localhost:8000/?tags=a&tags=b' # send messages with tags a and b
$> echo -n "hello" >/dev/udp/localhost/8003
```
