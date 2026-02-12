// -----------------------------------------------------------------------
// <copyright file="StreamMessageTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Models;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

public sealed class StreamMessageTests
{
    [Fact]
    public void NewMessage_HasUniqueId()
    {
        var msg1 = new StreamMessage();
        var msg2 = new StreamMessage();

        Assert.NotEqual(Guid.Empty, msg1.MessageId);
        Assert.NotEqual(msg1.MessageId, msg2.MessageId);
    }

    [Fact]
    public void NewMessage_HasDefaultValues()
    {
        var msg = new StreamMessage();

        Assert.Empty(msg.Payload);
        Assert.Equal(0, msg.SequenceNumber);
        Assert.Null(msg.RequestContext);
        Assert.True(msg.EnqueuedAtUtc <= DateTime.UtcNow);
    }

    [Fact]
    public void Properties_CanBeSet()
    {
        var streamId = StreamId.Create("ns", "key");
        var payload = new byte[] { 1, 2, 3 };
        var ctx = new Dictionary<string, object> { ["k"] = "v" };

        var msg = new StreamMessage
        {
            StreamId = streamId,
            Payload = payload,
            SequenceNumber = 42,
            RequestContext = ctx
        };

        Assert.Equal(streamId, msg.StreamId);
        Assert.Equal(payload, msg.Payload);
        Assert.Equal(42, msg.SequenceNumber);
        Assert.Same(ctx, msg.RequestContext);
    }
}

public sealed class InFlightMessageTests
{
    [Fact]
    public void NewInFlightMessage_HasDefaultTimestamp()
    {
        var inFlight = new InFlightMessage();
        Assert.True(inFlight.DeliveredAtUtc <= DateTime.UtcNow);
    }

    [Fact]
    public void Properties_CanBeSet()
    {
        var msg = new StreamMessage { SequenceNumber = 10 };
        var inFlight = new InFlightMessage
        {
            Message = msg,
            DeliveredAtUtc = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc)
        };

        Assert.Same(msg, inFlight.Message);
        Assert.Equal(new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), inFlight.DeliveredAtUtc);
    }
}
