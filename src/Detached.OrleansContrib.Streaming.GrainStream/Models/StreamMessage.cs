// -----------------------------------------------------------------------
// <copyright file="StreamMessage.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

namespace Detached.OrleansContrib.Streaming.GrainStream.Models;

/// <summary>
/// Represents a single message enqueued into a grain-backed stream queue.
/// </summary>
[GenerateSerializer]
[Alias("Detached.OrleansContrib.Streaming.GrainStream.Models.StreamMessage")]
public sealed class StreamMessage
{
    /// <summary>Unique identifier for this message.</summary>
    [Id(0)]
    public Guid MessageId { get; set; } = Guid.NewGuid();

    /// <summary>The Orleans StreamId this message belongs to.</summary>
    [Id(1)]
    public StreamId StreamId { get; set; }

    /// <summary>Serialised event payload.</summary>
    [Id(2)]
    public byte[] Payload { get; set; } = [];

    /// <summary>Monotonically increasing sequence number within the queue partition.</summary>
    [Id(3)]
    public long SequenceNumber { get; set; }

    /// <summary>UTC timestamp when the message was enqueued.</summary>
    [Id(4)]
    public DateTime EnqueuedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>Optional request context propagated from the producer.</summary>
    [Id(5)]
    public Dictionary<string, object>? RequestContext { get; set; }
}
