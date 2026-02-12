// -----------------------------------------------------------------------
// <copyright file="StreamQueueGrainTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Grains;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

/// <summary>
/// Tests for StreamQueueState (the persistent state object used by StreamQueueGrain).
/// The StreamQueueGrain itself requires the full Orleans runtime and is tested via integration tests.
/// </summary>
public sealed class StreamQueueStateTests
{
    [Fact]
    public void DefaultState_HasEmptyCollections()
    {
        var state = new StreamQueueState();
        Assert.Empty(state.PendingMessages);
        Assert.Empty(state.InFlightMessages);
        Assert.Equal(0, state.NextSequenceNumber);
    }

    [Fact]
    public void NextSequenceNumber_CanBeIncremented()
    {
        var state = new StreamQueueState
        {
            NextSequenceNumber = 42
        };
        Assert.Equal(42, state.NextSequenceNumber);
    }
}
