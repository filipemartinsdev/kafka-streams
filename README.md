<br>

<div align="center">

<img src="images/banner.png">

<h1>Kafka Streams</h1>

</div>

## Stream Types

There's some stream types to use with **Kafka Streams**, each with its approach. All streams are based on Key-Value data.

### KStream

The basic one, also known as Event Stream, might represent unreleated events with the same key. Here, all events are stored in-memory.

![Event Stream](images/event-stream.png)


Example:
```java
var builder = new StreamsBuilder();
KStream<String, String> stream = builder.stream(
	"entry-topic",
	Consumed.with(Serdes.String(), Serdes.String())
);

stream.peek((key, value) -> System.out.println(key + " - " + value));

stream.to(
	"out-topic", 
	Produced.with(Serdes.String(), Serdes.String())
);
```


### KTable

Also known as Update Stream, KTable has a different semantic. Here, multiple record with the same Key are considered an update for the same event. On KTable, the events aren't entirely stored in-memory, the last state of an event is stored in-disk using **RockDB** instead.

![Update Stream](images/update-stream.png)


The events are not auto-forward, they are cached instead. For default, the events are only commited every 30 seconds, or when it reaches 10 MB on cache.

Example:
```java
var builder = new StreamsBuilder();
KTable<String, String> table = builder.table(
	"entry-topic",
	Consumed.with(Serdes.String(), Serdes.String())
);

table.peek((key, value) -> System.out.println(key + " - " + value));

table.toStream();

table.to(
	"out-topic", 
	Produced.with(Serdes.String(), Serdes.String())
);
```


### GlobalKTable

This stream is a special kind of Table than can consumes all partitions at the same time. It's a good choice for static data or small and predictable data flow.

Example:
```java
var builder = new StreamsBuilder();
KTable<String, String> table = builder.globalTable(
	"entry-topic",
	Consumed.with(Serdes.String(), Serdes.String())
);

table.peek((key, value) -> System.out.println(key + " - " + value));

table.toStream();

table.to(
	"out-topic", 
	Produced.with(Serdes.String(), Serdes.String())
);
```


---
## Basic Stream Operations

- `mapValue`: Transforms only the value from Key/Value pair. It's the recommended for ordinary flow.
- `map`: Transforms both Key and Value from the record.
- `filter`: Apply a conditional filter.
- `aggregate`:
- `toStream`: Transform from KTable to KStream.
- `to`: Forward the event to some topic.

---
## Joins

A join is a Stream operation with immutable Keys that merges two streams, using related keys, on a single output. There's three type of Joins: Stream-Stream, Stream-Table and Table-Table. Each type of Join can support some of these operations:

- **Inner Join**: Only if both sides are available.
- **Outer Join**: Both side always produces a new merge, joining the nullable side.
- **Left Outer Join**: The left side always produces a new merge, joining the nullable right side.

Let's go ahead and explore each of them.

### Stream-Stream Join

It merges two KStream on a single KStream, supporting **Inner Join**, **Outer Join** and **Left Outer Join**. Both Key and Time Window needs to be related.

Join Examples:
```java
var builder = new StreamsBuilder();

KStream<String, String> leftStream = builder.stream(
	"left-stream",
	Consumed.with(Serdes.String(), Serdes.String())
);
KStream<String, String> leftStream = builder.stream(
	"right-stream",
	Consumed.with(Serdes.String(), Serdes.String())
);

ValueJoiner<String, String, String> joiner = (leftValue, rightValue) -> leftValue + "-" + rightValue;

KStream<String, String> innerStream = leftStream.join(
	rightStream,
	joiner,
	JoinWindows.of(Duration.ofSeconds(10)),
	StreamJoined.with(Serdes.String(), Serder.String(), Serdes.String())
);

KStream<String, String> leftOuterStream = leftStream.outerJoin(
	rightStream,
	joiner,
	JoinWindows.of(Duration.ofSeconds(10)),
	StreamJoined.with(Serdes.String(), Serder.String(), Serdes.String())
);

KStream<String, String> leftOuterStream = leftStream.leftJoin(
	rightStream,
	joiner,
	JoinWindows.of(Duration.ofSeconds(10)),
	StreamJoined.with(Serdes.String(), Serder.String(), Serdes.String())
);
```

### Stream-Table Join

It merges one KStream and one KTable (without time window) on a single KStream, supporting **Inner Join** and **Left Outer Join**.

Join examples:
```java
var builder = new StreamsBuilder();

KStream<String, String> leftStream = builder.stream(
	"left-stream",
	Consumed.with(Serdes.String(), Serdes.String())
);
KTable<String, String> leftStream = builder.table(
	"right-stream",
	Materialized.with(Serdes.String(), Serdes.String())
);

ValueJoiner<String, String, String> joiner = (leftValue, rightValue) -> leftValue + "-" + rightValue;

KStream<String, String> innerStream = leftStream.join(
	rightStream,
	joiner,
	Joined.with(Serdes.String(), Serder.String(), Serdes.String())
);

KStream<String, String> leftOuterStream = leftStream.leftJoin(
	rightStream,
	joiner,
	Joined.with(Serdes.String(), Serder.String(), Serdes.String())
);
```


### Table-Table Join

It merges two KTable into one KTable (without Time Window), supporting **Inner Join**, **Outer Join** and **Left Outer Join**.

```java
var builder = new StreamsBuilder();

KTable<String, String> leftTable = builder.table(
	"left-stream",
	Materialized.with(Serdes.String(), Serdes.String())
);
KTable<String, String> rightTable = builder.table(
	"right-stream",
	Materialized.with(Serdes.String(), Serdes.String())
);

ValueJoiner<String, String, String> joiner = (leftValue, rightValue) -> leftValue + "-" + rightValue;

KTable<String, String> innerTable = leftTable.join(
	rightStream,
	joiner
);

KTable<String, String> leftOuterTable = leftTable.outerJoin(
	rightStream,
	joiner
);

KTable<String, String> leftOuterTable = leftTable.leftJoin(
	rightStream,
	joiner
);
```

---
<div align="center">

Made with ☕ by **Filipe Martins**

</div>
---