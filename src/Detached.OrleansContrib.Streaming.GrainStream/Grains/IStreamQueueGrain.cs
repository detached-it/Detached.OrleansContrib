// -----------------------------------------------------------------------
// <copyright file="IStreamQueueGrain.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Orleans.Concurrency;

namespace Detached.OrleansContrib.Streaming.GrainStream.Grains;

/// <summary>
/// Grain interface for a single queue partition.
/// Each queue partition is identified by a string key matching the <see cref="Orleans.Streams.QueueId"/> ToString().
/// </summary>
public interface IStreamQueueGrain : IGrainWithStringKey
{
    /// <summary>
    /// Enqueues a batch of messages into the queue.
    /// </summary>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task EnqueueAsync(List<StreamMessage> messages);

    /// <summary>
    /// Dequeues up to <paramref name="maxCount"/> messages from the pending queue,
    /// moving them to the in-flight set.
    /// </summary>
    /// <returns>The batch of dequeued messages.</returns>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task<List<StreamMessage>> DequeueAsync(int maxCount);

    /// <summary>
    /// Acknowledges successful delivery of messages, removing them from the in-flight set.
    /// </summary>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task AcknowledgeAsync(List<Guid> messageIds);

    /// <summary>
    /// Returns the total number of pending + in-flight messages (for monitoring).
    /// </summary>
    [Transaction(TransactionOption.CreateOrJoin)]
    Task<int> GetQueueLengthAsync();

    /// <summary>
    /// Processes dead letters (timed-out in-flight messages).
    /// Internal method exposed for transactional self-calls.
    /// </summary>
    [AlwaysInterleave]
    [Transaction(TransactionOption.CreateOrJoin)]
    Task ProcessDeadLetterCheckAsync();
}
