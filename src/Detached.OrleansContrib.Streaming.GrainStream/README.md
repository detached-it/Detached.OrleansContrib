<div align="center">
  
[![Build Status](https://github.com/detached-it/Detached.OrleansContrib/actions/workflows/publish.yml/badge.svg)](https://github.com/detached-it/Detached.OrleansContrib/actions/workflows/publish.yml)
![Coverage](https://img.shields.io/badge/Coverage-98.7%25-brightgreen)

</div>

# Detached.OrleansContrib.Streaming.GrainStream

A simple, lightweight **Persistent Stream Provider** for Microsoft Orleans that uses **Grains** as queue storage.

## Overview

`GrainStream` provides a way to use Orleans streams with persistence and delivery guarantees without requiring any external queuing infrastructure (like Azure Queues, Amazon SQS, or RabbitMQ). It leverages Orleans Grains and existing Grain Storage Providers to manage message queues.

This provider is ideal for:
- On-premise deployments where you want to minimize infrastructure dependencies.
- Local development and testing of stream-based applications.
- Scenarios where you already have a reliable Grain Storage provider (ADO.NET, Mongo, etc.) and want to use it for streaming as well.

## Features

- **Infrastructure-Free**: No need for external queue services. If your Silo is running, your streams are running.
- **Persistent**: Messages are stored using your configured Grain Storage provider.
- **Reliable Delivery**: Supports message acknowledgment and automatic retries for timed-out messages.
- **Scalable**: Uses sharded "Queue Grains" to distribute the workload.
- **Transactional**: Uses Orleans Transactions to ensure atomicity in enqueue/dequeue operations (requires Transactions to be enabled).

## Installation

Add the library to your Silo and Client projects:

```bash
dotnet add package Detached.OrleansContrib.Streaming.GrainStream
```

## Configuration

### 1. Enable Transactions
Because `GrainStream` uses grains to manage state across operations, it requires Orleans Transactions to be enabled on your Silo.

```csharp
siloBuilder.UseTransactions();
```

### 2. Configure Grain Storage
Define a named storage provider that the queue grains will use to persist messages.

```csharp
siloBuilder.AddMemoryGrainStorage("GrainStreamStore"); 
// Or use ADO.NET, Azure, etc.
```

### 3. Register the Stream Provider

**On the Silo:**
```csharp
using Detached.OrleansContrib.Streaming.GrainStream.Extensions;

siloBuilder.AddGrainStream("MyStreamProvider", options => {
    options.StorageProviderName = "GrainStreamStore";
    options.NumQueues = 8;
});
```

**On the Client:**
```csharp
using Detached.OrleansContrib.Streaming.GrainStream.Extensions;

clientBuilder.AddGrainStream("MyStreamProvider");
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `NumQueues` | `8` | Number of queue partitions (grain shards). |
| `MaxBatchSize` | `100` | Maximum messages per dequeue batch. |
| `InFlightTimeoutSeconds` | `60` | Time before an unacknowledged message is returned to the queue. |
| `ReminderIntervalSeconds` | `60` | Frequency of the background check for timed-out messages. |
| `StorageProviderName` | `"GrainStreamStore"` | Name of the storage provider for queue state. |
| `CacheSize` | `4096` | Size of the stream adapter cache. |

## Usage

Usage is identical to any other Orleans persistent stream provider.

### Sending Messages
```csharp
var streamProvider = GetStreamProvider("MyStreamProvider");
var stream = streamProvider.GetStream<string>(StreamId.Create("MyNamespace", Guid.NewGuid()));

await stream.OnNextAsync("Hello GrainStream!");
```

### Receiving Messages
```csharp
[ImplicitStreamSubscription("MyNamespace")]
public class MyReceiverGrain : Grain, IMyReceiverGrain
{
    public override async Task OnActivateAsync(CancellationToken ct)
    {
        var streamProvider = this.GetStreamProvider("MyStreamProvider");
        var stream = streamProvider.GetStream<string>(this.GetPrimaryKey(), "MyNamespace");
        
        await stream.SubscribeAsync(async (item, token) => {
            Console.WriteLine($"Received: {item}");
        });
    }
}
```

## How it Works

1. **Enqueue**: When a message is sent, it is hashed to one of the `NumQueues` grains and stored in its `Pending` list.
2. **Pulling**: The Orleans `PersistentStreamPullingAgent` calls the Queue Grains to dequeue batches.
3. **In-Flight**: Dequeued messages move from `Pending` to `InFlight`.
4. **Acknowledgment**: Once the consumer successfully processes the message, it is removed from the `InFlight` set.
5. **Dead Letter Check**: A background reminder checks for messages that have been `InFlight` longer than `InFlightTimeoutSeconds` and moves them back to `Pending` for re-delivery.

---
Developed by **Detached IT** | © 2026
