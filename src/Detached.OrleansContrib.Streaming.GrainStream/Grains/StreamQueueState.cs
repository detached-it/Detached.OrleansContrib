// -----------------------------------------------------------------------
// <copyright file="StreamQueueState.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

namespace Detached.OrleansContrib.Streaming.GrainStream.Grains;

/// <summary>
/// Persistent state for a single queue partition grain.
/// </summary>
[GenerateSerializer]
[Alias("Detached.OrleansContrib.Streaming.GrainStream.Grains.StreamQueueState")]
public sealed class StreamQueueState
{
    /// <summary>Messages waiting to be delivered.</summary>
    [Id(0)]
    public List<Models.StreamMessage> PendingMessages { get; set; } = [];

    /// <summary>Messages that have been dequeued but not yet acknowledged, keyed by MessageId.</summary>
    [Id(1)]
    public Dictionary<Guid, Models.InFlightMessage> InFlightMessages { get; set; } = [];

    /// <summary>Monotonically increasing sequence counter for this partition.</summary>
    [Id(2)]
    public long NextSequenceNumber { get; set; }
}
