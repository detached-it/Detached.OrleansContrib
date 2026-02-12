// -----------------------------------------------------------------------
// <copyright file="GrainStreamQueueAdapterTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Adapter;
using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Detached.OrleansContrib.Streaming.GrainStream.Grains;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

public sealed class GrainStreamQueueAdapterTests
{
    private readonly Mock<IGrainFactory> _grainFactory = new();
    private readonly Mock<IStreamQueueMapper> _mapper = new();
    private readonly Mock<ILoggerFactory> _loggerFactory = new();
    private readonly GrainStreamOptions _options = new();

    public GrainStreamQueueAdapterTests()
    {
        _loggerFactory
            .Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());
    }

    private GrainStreamQueueAdapter CreateAdapter() =>
        new("TestProvider", _grainFactory.Object, _mapper.Object, _options, _loggerFactory.Object);

    [Fact]
    public void Name_ReturnsProviderName()
    {
        var adapter = CreateAdapter();
        Assert.Equal("TestProvider", adapter.Name);
    }

    [Fact]
    public void IsRewindable_ReturnsFalse()
    {
        var adapter = CreateAdapter();
        Assert.False(adapter.IsRewindable);
    }

    [Fact]
    public void Direction_ReturnsReadWrite()
    {
        var adapter = CreateAdapter();
        Assert.Equal(StreamProviderDirection.ReadWrite, adapter.Direction);
    }

    [Fact]
    public void CreateReceiver_ReturnsGrainStreamQueueAdapterReceiver()
    {
        var queueId = QueueId.GetQueueId("TestProvider", 0, 0);
        var mockGrain = new Mock<IStreamQueueGrain>();
        _grainFactory
            .Setup(f => f.GetGrain<IStreamQueueGrain>(queueId.ToString(), null))
            .Returns(mockGrain.Object);

        var adapter = CreateAdapter();
        var receiver = adapter.CreateReceiver(queueId);

        Assert.NotNull(receiver);
        Assert.IsType<GrainStreamQueueAdapterReceiver>(receiver);
    }

    [Fact]
    public async Task QueueMessageBatchAsync_EnqueuesViaGrain()
    {
        var streamId = StreamId.Create("TestNs", "key1");
        var queueId = QueueId.GetQueueId("TestProvider", 0, 0);
        var mockGrain = new Mock<IStreamQueueGrain>();

        _mapper.Setup(m => m.GetQueueForStream(streamId)).Returns(queueId);
        _grainFactory
            .Setup(f => f.GetGrain<IStreamQueueGrain>(queueId.ToString(), null))
            .Returns(mockGrain.Object);

        var adapter = CreateAdapter();
        var events = new[] { "event1", "event2" };

        await adapter.QueueMessageBatchAsync(streamId, events, null, []);

        mockGrain.Verify(g => g.EnqueueAsync(It.Is<List<StreamMessage>>(
            msgs => msgs.Count == 2)), Times.Once);
    }

    [Fact]
    public void Constructor_ThrowsOnNullArguments()
    {
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapter(null!, _grainFactory.Object, _mapper.Object, _options, _loggerFactory.Object));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapter("name", null!, _mapper.Object, _options, _loggerFactory.Object));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapter("name", _grainFactory.Object, null!, _options, _loggerFactory.Object));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapter("name", _grainFactory.Object, _mapper.Object, null!, _loggerFactory.Object));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapter("name", _grainFactory.Object, _mapper.Object, _options, null!));
    }
}
