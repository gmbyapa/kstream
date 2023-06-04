# KStream - Kafka Streams for Golang

[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/gmbyapa/kstream)
[![Releases](https://img.shields.io/github/release/gmbyapa/kstream/all.svg?style=flat-square)](https://github.com/gmbyapa/kstream/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/gmbyapa/kstream)](https://goreportcard.com/report/github.com/gmbyapa/kstream)
[![LICENSE](https://img.shields.io/github/license/gmbyapa/kstream.svg?style=flat-square)](https://github.com/gmbyapa/kstream/blob/master/LICENSE)

KStream is a [kafka streams](https://kafka.apache.org/documentation/streams/) implementation written in Golang. It is
heavily influenced by Kafka-Streams(Java) library and includes features such as Streams, Tables, State Stores,
[EOS](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)
support, and so on.

## Features

- **Streams**: Leverage the streaming capabilities of KStream to process and transform data in real-time.
- **Tables**: Utilize tables to perform aggregations, joins, and other advanced operations on the data streams.
- **State Stores**: Benefit from built-in state stores for efficient storage and retrieval of stream data.
- **EOS Support**: Enjoy the reliability of exactly-once semantics with KStream's support
  for [EOS](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics).

| DSL                   |                                                                |
|:----------------------|----------------------------------------------------------------|
| Branch                | Supported([Split Example](examples/split/README.md)    )       |
| Filter                | Supported                                                      |
| Inverse Filter        | Not Supported                                                  |
| FlatMap               | Supported                                                      |
| FlatMap (values only) | Supported([Word Count Example](examples/word-count/README.md)) |
| Peek                  | Not Supported(Use **Each** instead)                            |
| Foreach               | Supported([Word Count Example](examples/word-count/README.md)) |
| Map                   | Supported                                                      |
| Map (values only)     | Supported                                                      |
| Merge                 | Supported                                                      |
| SelectKey             | Supported([Word Count Example](examples/word-count/README.md)) |
| Repartition           | Supported                                                      |
| Table to Stream       | Supported([Word Count Example](examples/word-count/README.md)) |
| Stream to Table       | Supported([Word Count Example](examples/word-count/README.md)) |
| Aggregate             | Supported([Word Count Example](examples/word-count/README.md)) |
| GroupBy               | Not Supported                                                  |
| GroupByKey            | Not Supported                                                  |
| Aggregate (windowed)  | Not Supported                                                  |
| Cogroup               | Not Supported                                                  |

### Join Support

Please refer https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#joining

| Join                              |                                   |
|:----------------------------------|-----------------------------------|
| KTable-to-KTable                  | Supported                         |
| KStream-to-KTable                 | Supported                         |
| KStream-to-GlobalKTable           | Supported                         |
| KTable-to-KTable Foreign-Key Join | Supported(Using lookup functions) |
| KTable-to-GlobalKTable            | Not Supported                     |
| KStream-to-KStream                | Not Supported                     |

## Getting Started

To get started with KStream, follow these steps:

1. Install librdkafka(https://github.com/confluentinc/librdkafka#installation).
