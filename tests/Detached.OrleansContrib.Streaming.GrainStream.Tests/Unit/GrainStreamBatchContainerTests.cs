// -----------------------------------------------------------------------
// <copyright file="GrainStreamBatchContainerTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using System.Text.Json;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

public sealed class GrainStreamBatchContainerTests
{
    [Fact]
    public void SerializeEvent_ProducesValidJson()
    {
        var testEvent = new TestEvent { Id = 42, Name = "Hello" };
        var bytes = GrainStreamBatchContainer.SerializeEvent(testEvent);

        Assert.NotNull(bytes);
        Assert.NotEmpty(bytes);

        var deserialized = JsonSerializer.Deserialize<TestEvent>(bytes);
        Assert.NotNull(deserialized);
        Assert.Equal(42, deserialized.Id);
        Assert.Equal("Hello", deserialized.Name);
    }

    [Fact]
    public void ToMessage_CreatesValidStreamMessage()
    {
        var streamId = StreamId.Create("TestNs", "key1");
        var testEvent = new TestEvent { Id = 1, Name = "Test" };

        var message = GrainStreamBatchContainer.ToMessage(streamId, testEvent);

        Assert.NotEqual(Guid.Empty, message.MessageId);
        Assert.Equal(streamId, message.StreamId);
        Assert.NotEmpty(message.Payload);
        Assert.Null(message.RequestContext);
    }

    [Fact]
    public void ToMessage_WithRequestContext_PropagatesContext()
    {
        var streamId = StreamId.Create("TestNs", "key1");
        var testEvent = new TestEvent { Id = 1, Name = "Test" };
        var ctx = new Dictionary<string, object> { ["traceId"] = "abc123" };

        var message = GrainStreamBatchContainer.ToMessage(streamId, testEvent, ctx);

        Assert.NotNull(message.RequestContext);
        Assert.Equal("abc123", message.RequestContext!["traceId"]);
    }

    [Fact]
    public void FromMessage_CreatesValidBatchContainer()
    {
        var streamId = StreamId.Create("TestNs", "key1");
        var testEvent = new TestEvent { Id = 7, Name = "FromMsg" };
        var msg = GrainStreamBatchContainer.ToMessage(streamId, testEvent);
        msg.SequenceNumber = 42;

        var container = GrainStreamBatchContainer.FromMessage(msg);

        Assert.Equal(streamId, container.StreamId);
        Assert.Equal(42L, container.SequenceNumber);
        Assert.Equal(msg.MessageId, container.MessageId);
        Assert.Equal(msg.Payload, container.Payload);
    }

    [Fact]
    public void FromMessage_ThrowsOnNull()
    {
        Assert.Throws<ArgumentNullException>(() => GrainStreamBatchContainer.FromMessage(null!));
    }

    [Fact]
    public void GetEvents_ReturnsDeserializedEvent()
    {
        var streamId = StreamId.Create("TestNs", "key1");
        var testEvent = new TestEvent { Id = 99, Name = "Get" };
        var msg = GrainStreamBatchContainer.ToMessage(streamId, testEvent);
        msg.SequenceNumber = 5;

        var container = GrainStreamBatchContainer.FromMessage(msg);
        var events = container.GetEvents<TestEvent>().ToList();

        Assert.Single(events);
        Assert.Equal(99, events[0].Item1.Id);
        Assert.Equal("Get", events[0].Item1.Name);
        Assert.IsType<EventSequenceTokenV2>(events[0].Item2);
    }

    [Fact]
    public void SequenceToken_ReturnsEventSequenceTokenV2()
    {
        var container = new GrainStreamBatchContainer { SequenceNumber = 123 };
        var token = ((IBatchContainer)container).SequenceToken;

        Assert.NotNull(token);
        Assert.IsType<EventSequenceTokenV2>(token);
    }

    [Fact]
    public void ImportRequestContext_ReturnsFalse_WhenNull()
    {
        var container = new GrainStreamBatchContainer { RequestContext = null };
        Assert.False(container.ImportRequestContext());
    }

    [Fact]
    public void ImportRequestContext_ReturnsFalse_WhenEmpty()
    {
        var container = new GrainStreamBatchContainer { RequestContext = [] };
        Assert.False(container.ImportRequestContext());
    }

    [Fact]
    public void ImportRequestContext_SetsContextValues()
    {
        var container = new GrainStreamBatchContainer
        {
            RequestContext = new Dictionary<string, object> { ["key1"] = "value1" }
        };

        var result = container.ImportRequestContext();
        Assert.True(result);
    }

    public sealed class TestEvent
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
