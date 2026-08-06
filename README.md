<br>

<div align="center">

<img src="images/banner.png">

<h1>Kafka Streams</h1>

</div>

## Index

1. [Stream Types](#stream-types) <br>
    1.1. [KStream](#kstream) <br>
    1.2. [KTable](#ktable) <br>
    1.3. [GlobalKTable](#globalktable) <br>
2. [Basic Stream operations](#basic-stream-operations) <br>
    2.1. [Stateless](#stateless) <br>
    2.2. [Stateful](#stateful) <br>
3. [Join](#join) <br>
    3.1. [Stream-Stream Join](#stream-stream-join) <br>
    3.2. [Stream-Table Join](#stream-table-join) <br>
    3.3. [Table-Table Join](#table-table-join) <br>

## Stream Types

There are some stream types to use with **Kafka Streams**, each with its use cases. All streams are based on Key-Value data.

### KStream

The basic one, also known as Event Stream, represents unrelated events that might have the same key. Here, all events are stored on memory.

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

Also known as Update Stream, KTable has a different semantic. Here, multiple records with the same Key are considered an update for the same event. In KTable, the events aren't entirely stored on memory, the last state of an event is stored on disk using **RocksDB** instead.

![Update Stream](images/update-stream.png)


The events are not automatically forwarded, they are cached instead. By default, the events are only commited every 30 seconds, or when it reaches 10 MB on cache.

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

This stream is a special kind of Table that consumes all partitions at the same time. It's a good choice for static data or small and predictable data flow.

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

### Stateless

- `mapValue`: Transforms only the value from Key/Value pair. It's recommended for ordinary flows.
- `map`: Transforms both Key and Value from the record.
- `filter`: Applies a conditional filter.
- `toStream`: Transforms from KTable to KStream.
- `to`: Forwards the event to some topic.

### Stateful

> Building...

- `count`:
- `reduce`:
- `aggregate`: 


---
## Join

![Join](images/join.png)

A join is a Stream operation with immutable Keys that merges two streams/tables, using related keys, into a single output. There are three types of Join: Stream-Stream, Stream-Table and Table-Table. Each type of Join can support some of these operations:

- **Inner Join**: Only if both sides are available.

| Left has value | Right has value | Joined |
|----------------|-----------------|--------|
| False          | False           | False  |
| False          | True            | False  |
| True           | False           | False  |
| True           | True            | True   |


- **Outer Join**: Both side always produce a new merge, joining the nullable side.

| Left has value | Right has value | Joined |
|----------------|-----------------|--------|
| False          | False           | False  |
| False          | True            | True   |
| True           | False           | True   |
| True           | True            | True   |

- **Left Outer Join**: The left side always produce a new merge, joining the nullable right side.

| Left has value | Right has value | Joined |
|----------------|-----------------|--------|
| False          | False           | False  |
| False          | True            | False  |
| True           | False           | True   |
| True           | True            | True   |

Let's go ahead and explore each of them.

### Stream-Stream Join

It merges two KStream into a single KStream, supporting **Inner Join**, **Outer Join** and **Left Outer Join**. Both Key and Time Window needs to be related.

Join Examples:
```java
var builder = new StreamsBuilder();

KStream<String, String> leftStream = builder.stream(
	"left-stream",
	Consumed.with(Serdes.String(), Serdes.String())
);
KStream<String, String> rightStream = builder.stream(
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

It merges one KStream and one KTable (without time window) into a single KStream, supporting **Inner Join** and **Left Outer Join**.

Join examples:
```java
var builder = new StreamsBuilder();

KStream<String, String> leftStream = builder.stream(
	"left-stream",
	Consumed.with(Serdes.String(), Serdes.String())
);
KTable<String, String> rightTable = builder.table(
	"right-stream",
	Materialized.with(Serdes.String(), Serdes.String())
);

ValueJoiner<String, String, String> joiner = (leftValue, rightValue) -> leftValue + "-" + rightValue;

KStream<String, String> innerStream = leftStream.join(
	rightTable,
	joiner,
	Joined.with(Serdes.String(), Serder.String(), Serdes.String())
);

KStream<String, String> leftOuterStream = leftStream.leftJoin(
	rightTable,
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
	rightTable,
	joiner
);

KTable<String, String> leftOuterTable = leftTable.outerJoin(
	rightTable,
	joiner
);

KTable<String, String> leftOuterTable = leftTable.leftJoin(
	rightTable,
	joiner
);
```

---
<div align="center">

Made with ☕ by **Filipe Martins**

</div>
