// -----------------------------------------------------------------------
// <copyright file="GrainStreamQueueAdapterReceiverTests.cs" company="Detached IT">
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

public sealed class GrainStreamQueueAdapterReceiverTests
{
    private readonly Mock<IStreamQueueGrain> _mockGrain = new();
    private readonly GrainStreamOptions _options = new() { MaxBatchSize = 50 };
    private readonly ILogger<GrainStreamQueueAdapterReceiver> _logger =
        Mock.Of<ILogger<GrainStreamQueueAdapterReceiver>>();

    private GrainStreamQueueAdapterReceiver CreateReceiver() =>
        new(_mockGrain.Object, _options, _logger);

    [Fact]
    public async Task Initialize_CompletesSuccessfully()
    {
        var receiver = CreateReceiver();
        await receiver.Initialize(TimeSpan.FromSeconds(5));
        // Should not throw
    }

    [Fact]
    public async Task GetQueueMessagesAsync_ReturnsNull_WhenNoMessages()
    {
        _mockGrain
            .Setup(g => g.DequeueAsync(It.IsAny<int>()))
            .ReturnsAsync([]);

        var receiver = CreateReceiver();
        var result = await receiver.GetQueueMessagesAsync(100);

        Assert.Null(result);
    }

    [Fact]
    public async Task GetQueueMessagesAsync_ReturnsBatchContainers_WhenMessagesExist()
    {
        var streamId = StreamId.Create("ns", "key");
        var messages = new List<StreamMessage>
        {
            GrainStreamBatchContainer.ToMessage(streamId, "event1"),
            GrainStreamBatchContainer.ToMessage(streamId, "event2")
        };
        messages[0].SequenceNumber = 1;
        messages[1].SequenceNumber = 2;

        _mockGrain
            .Setup(g => g.DequeueAsync(It.IsAny<int>()))
            .ReturnsAsync(messages);

        var receiver = CreateReceiver();
        var result = await receiver.GetQueueMessagesAsync(100);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Count);
        Assert.All(result, b => Assert.IsType<GrainStreamBatchContainer>(b));
    }

    [Fact]
    public async Task GetQueueMessagesAsync_RespectsMaxBatchSize()
    {
        _mockGrain
            .Setup(g => g.DequeueAsync(50)) // Our options say MaxBatchSize=50
            .ReturnsAsync([]);

        var receiver = CreateReceiver();
        await receiver.GetQueueMessagesAsync(500); // asks for 500 but option caps at 50

        _mockGrain.Verify(g => g.DequeueAsync(50), Times.Once);
    }

    [Fact]
    public async Task MessagesDeliveredAsync_AcknowledgesMessages()
    {
        var msg1 = new GrainStreamBatchContainer { MessageId = Guid.NewGuid() };
        var msg2 = new GrainStreamBatchContainer { MessageId = Guid.NewGuid() };
        var messages = new List<IBatchContainer> { msg1, msg2 };

        var receiver = CreateReceiver();
        await receiver.MessagesDeliveredAsync(messages);

        _mockGrain.Verify(g => g.AcknowledgeAsync(It.Is<List<Guid>>(
            ids => ids.Count == 2 && ids.Contains(msg1.MessageId) && ids.Contains(msg2.MessageId))),
            Times.Once);
    }

    [Fact]
    public async Task MessagesDeliveredAsync_DoesNothing_WhenNull()
    {
        var receiver = CreateReceiver();
        await receiver.MessagesDeliveredAsync(null!);
        _mockGrain.Verify(g => g.AcknowledgeAsync(It.IsAny<List<Guid>>()), Times.Never);
    }

    [Fact]
    public async Task MessagesDeliveredAsync_DoesNothing_WhenEmpty()
    {
        var receiver = CreateReceiver();
        await receiver.MessagesDeliveredAsync([]);
        _mockGrain.Verify(g => g.AcknowledgeAsync(It.IsAny<List<Guid>>()), Times.Never);
    }

    [Fact]
    public async Task Shutdown_CompletesSuccessfully()
    {
        var receiver = CreateReceiver();
        await receiver.Shutdown(TimeSpan.FromSeconds(5));
        // Should not throw
    }

    [Fact]
    public void Constructor_ThrowsOnNullArguments()
    {
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapterReceiver(null!, _options, _logger));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapterReceiver(_mockGrain.Object, null!, _logger));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamQueueAdapterReceiver(_mockGrain.Object, _options, null!));
    }
}
