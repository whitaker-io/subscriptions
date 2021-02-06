`Machine` is a library for creating data workflows. These workflows can be either very concise or quite complex, even allowing for cycles for flows that need retry or self healing mechanisms.

### **Installation**

Add the primary library to your project
```bash
  go get -u github.com/whitaker-io/machine
```
------
Add the different Subscription Implementations
```bash
  go get -u github.com/whitaker-io/subscriptions/kafka
```
```bash
  go get -u github.com/whitaker-io/subscriptions/redis
```
```bash
  go get -u github.com/whitaker-io/subscriptions/pubsub
```
```bash
  go get -u github.com/whitaker-io/subscriptions/sqs
```

***
## [Example](#example)


Redis Subscription with basic `receive` -> `process` -> `send` Stream

```golang

  // logger allows for logs to be transmitted to your log provider
  var logger machine.Logger

  // logStore allows for running a cluster and handles communication
  var logStore machine.LogStore

  // pool is a redigo Pool for a redis cluster to read the stream from
  // see also the Google Pub/Sub, Kafka, and SQS implementations
  var pool *redigo.Pool

  redisStream := redis.New(pool, logger)
  
  // NewPipe creates a pipe in which you can run multiple streams
  // the id is the instance identifier for the cluster
  p := NewPipe(uuid.New().String(), logger, logStore, fiber.Config{
    ReadTimeout: time.Second,
    WriteTimeout: time.Second,
    BodyLimit: 4 * 1024 * 1024,
    DisableKeepalive: true,
  })

  // StreamSubscription takes an instance of machine.Subscription
  // and a time interval in which to read
  // the id here needs to be the same for all the nodes for the clustering to work
  builder := p.StreamSubscription("unique_stream_id", redisStream, 5*time.Millisecond,
    &Option{FIFO: boolP(false)},
    &Option{Injectable: boolP(true)},
    &Option{Metrics: boolP(true)},
    &Option{Span: boolP(false)},
    &Option{BufferSize: intP(0)},
  ).Builder()

  builder.Map("unique_id2", 
      func(m Data) error {
        var err error

        // ...do some processing

        return err
      },
    ).
    Transmit("unique_id3", 
      func(d []Data) error {
        // send a copy of the data somewhere

        return nil
      },
    )

  // Run requires a context, the port to run the fiber.App,
  // and the timeout for graceful shutdown
  if err := p.Run(context.Background(), ":5000", 10 * time.Second); err != nil {
    // Run will return an error in the case that 
    // one of the paths is not terminated (i.e. missing a Transmit)
    panic(err)
  }
```

***
## 🤝 Contributing

Contributions, issues and feature requests are welcome.<br />
Feel free to check [issues page](https://github.com/whitaker-io/machine/issues) if you want to contribute.<br />
[Check the contributing guide](./CONTRIBUTING.md).<br />

## Author

👤 **Jonathan Whitaker**

- Twitter: [@io_whitaker](https://twitter.com/io_whitaker)
- Github: [@jonathan-whitaker](https://github.com/jonathan-whitaker)
- Subreddit: [golang_machine](https://www.reddit.com/r/golang_machine/)

## Show your support

Please ⭐️ this repository if this project helped you!

***
## [License](#license)

Machine is provided under the [MIT License](https://github.com/whitaker-io/machine/blob/master/LICENSE).

```text
The MIT License (MIT)

Copyright (c) 2020 Jonathan Whitaker
```
