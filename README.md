<br>

<div align="center">

<img src="images/banner.png">

<h1>Kafka Streams</h1>

</div>

## Index
1. [Serialization](#serialization) <br>
2. [Stream Types](#stream-types) <br>
    2.1. [KStream](#kstream) <br>
    2.2. [KTable](#ktable) <br>
    2.3. [GlobalKTable](#globalktable) <br>
3. [Basic Stream operations](#basic-stream-operations) <br>
    3.1. [Stateless](#stateless) <br>
    3.2. [Stateful](#stateful) <br>
4. [Join](#join) <br>
    4.1. [Stream-Stream Join](#stream-stream-join) <br>
    4.2. [Stream-Table Join](#stream-table-join) <br>
    4.3. [Table-Table Join](#table-table-join) <br>
5. [Spring/Quarkus integration](#springquarkus-integration) <br>

## Serialization

The entire Kafka Streams serialization are based on Serde concept. Serde means Serializer/Desserializer, thus, they are wrapper classes to abstract both Serializer and Deserializer classes from the same type to bytes.

We can use differents built-in Serdes for basic types or create a custom Serde for specific classes. All built-in Serdes can be retrieved from `Serdes` class, e.g. `Serdes.String()`.

Examples:

```java
// Basic Serdes:
Serdes.String();
Serdes.Integer();
Serdes.Boolean();
Serdes.UUID();

// Custom Serde, if using Jackson:
Serde<YourClass> customSerde = new ObjectMapperSerde<>(YourClass.class, new ObjectMapper());
```

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

The stream operations are separated into two categories, Stateless and Stateful. The Stateless includes basic operations that doesn't need past record state or any stored data. On the other hand, Stateful operations need the past event state.

### Stateless

- `mapValue`: Transforms only the value from Key/Value pair. It's recommended for ordinary flows.
- `map`: Transforms both Key and Value from the record.
- `filter`: Applies a conditional filter.
- `windowed`: Applies sliding window to stream.
- `toStream`: Transforms from KTable to KStream.
- `to`: Forwards the event to some topic.

### Stateful

- `count`: Calculate how many messages have already been forwarded.
- `reduce`: Apply a reduce function to the entire stream.
- `aggregate`: It's like reduce, but can handle multiple types.

The stateful operations just make sense if the events are grouped, for instance, by Key. Thus, we need to use `groubByKey` or a custom `groupBy` method to group the events before applying any stateful operations.

Example:

```java
var builder = new StreamsBuilder();

KStream textStream = builder.stream(
        "text-topic",
        Consumed.with(Serdes.String(), Serdes.String())
);

stream.mapValue((value) -> value.toUpperCase()) // Transform text to uppercase
    .peek((key, value) -> System.out.println(value))

    .filter((key, value) -> value.length() > 10) // Get only text with 10+ characters

    .groupByKey()
    
    .aggregate(
            () -> 0L, // Default accumulator value
            (key, value, accumulator) -> accumulator + value.length(),
            Materialized.with(Serdes.String(), Serdes.Integer())
    )
    
    .toStream()
    
    .to("character-count-topic", Produced.with(Serdes.String(), Serdes.Integer()));


```

If your events have not been sent with a Key, you can set a custom `groupBy` like this:

```java
stream.groupBy(
        (key, value) -> value.youCustomId(),
        Grouped.with(Serdes.String(), Serdes.String())
);
```

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

## Spring/Quarkus integration

You can see the same example project implementing Kafka Streams using Spring Boot or Quarkus:

- [Spring Boot project](https://github.com/filipemartinsdev/spring-kafka-sandbox)
- [Quarkus project](https://github.com/filipemartinsdev/quarkus-kafka-sandbox)

---
<div align="center">

Made with ☕ by **Filipe Martins**

</div>