// -----------------------------------------------------------------------
// <copyright file="EndToEndStreamingTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Grains;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Detached.OrleansContrib.Streaming.GrainStream.Tests.TestGrains;
using Orleans.TestingHost;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Integration;

[Collection("ClusterCollection")]
public sealed class EndToEndStreamingTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster ?? throw new InvalidOperationException("Cluster not initialised");

    [Fact]
    public async Task ProduceAndConsume_SingleMessage()
    {
        var streamKey = Guid.NewGuid();
        var client = _cluster.Client;

        // Setup consumer
        var consumer = client.GetGrain<ITestConsumerGrain>(streamKey);
        await consumer.Subscribe(ClusterFixture.StreamProviderName, "TestNs", streamKey);

        // Produce a message
        var streamProvider = client.GetStreamProvider(ClusterFixture.StreamProviderName);
        var stream = streamProvider.GetStream<string>(StreamId.Create("TestNs", streamKey.ToString()));
        await stream.OnNextAsync("Hello, GrainStream!");

        // Wait for delivery
        await WaitForReceivedCount(consumer, 1, TimeSpan.FromSeconds(30));

        var received = await consumer.GetReceivedEvents();
        Assert.Single(received);
        Assert.Equal("Hello, GrainStream!", received[0]);
    }

    [Fact]
    public async Task ProduceAndConsume_MultipleBatch()
    {
        var streamKey = Guid.NewGuid();
        var client = _cluster.Client;

        var consumer = client.GetGrain<ITestConsumerGrain>(streamKey);
        await consumer.Subscribe(ClusterFixture.StreamProviderName, "TestNs", streamKey);

        var streamProvider = client.GetStreamProvider(ClusterFixture.StreamProviderName);
        var stream = streamProvider.GetStream<string>(StreamId.Create("TestNs", streamKey.ToString()));

        for (var i = 0; i < 10; i++)
        {
            await stream.OnNextAsync($"Message-{i}");
        }

        await WaitForReceivedCount(consumer, 10, TimeSpan.FromSeconds(30));

        var received = await consumer.GetReceivedEvents();
        Assert.Equal(10, received.Count);
        for (var i = 0; i < 10; i++)
        {
            Assert.Contains($"Message-{i}", received);
        }
    }

    [Fact]
    public async Task StreamQueueGrain_EnqueueAndDequeue_DirectlyWorks()
    {
        var client = _cluster.Client;
        var grain = client.GetGrain<IStreamQueueGrain>("test-queue-direct");

        var streamId = StreamId.Create("TestNs", "direct-test");
        var messages = new List<StreamMessage>
        {
            GrainStreamBatchContainer.ToMessage(streamId, "msg1"),
            GrainStreamBatchContainer.ToMessage(streamId, "msg2"),
            GrainStreamBatchContainer.ToMessage(streamId, "msg3")
        };

        await grain.EnqueueAsync(messages);
        var length = await grain.GetQueueLengthAsync();
        Assert.Equal(3, length);

        var dequeued = await grain.DequeueAsync(2);
        Assert.Equal(2, dequeued.Count);
        length = await grain.GetQueueLengthAsync();
        Assert.Equal(3, length); // 1 pending + 2 in-flight

        var idsToAck = dequeued.Select(m => m.MessageId).ToList();
        await grain.AcknowledgeAsync(idsToAck);
        length = await grain.GetQueueLengthAsync();
        Assert.Equal(1, length); // 1 pending remaining
    }

    [Fact]
    public async Task StreamQueueGrain_EmptyDequeue_ReturnsEmpty()
    {
        var client = _cluster.Client;
        var grain = client.GetGrain<IStreamQueueGrain>("test-queue-empty");

        var dequeued = await grain.DequeueAsync(10);
        Assert.Empty(dequeued);
    }

    private static async Task WaitForReceivedCount(ITestConsumerGrain consumer, int expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var count = await consumer.GetReceivedCount();
            if (count >= expected)
            {
                return;
            }

            await Task.Delay(250);
        }
        var finalCount = await consumer.GetReceivedCount();
        Assert.Equal(expected, finalCount);
    }
}
