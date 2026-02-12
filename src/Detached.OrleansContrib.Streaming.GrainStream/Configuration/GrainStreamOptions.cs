// -----------------------------------------------------------------------
// <copyright file="GrainStreamOptions.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

namespace Detached.OrleansContrib.Streaming.GrainStream.Configuration;

/// <summary>
/// Configuration options for the grain-based stream provider.
/// </summary>
public sealed class GrainStreamOptions
{
    /// <summary>Number of queue partitions (grain shards). Default: 8.</summary>
    public int NumQueues { get; set; } = 8;

    /// <summary>Maximum number of messages returned per dequeue batch. Default: 100.</summary>
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Seconds before an in-flight message is considered timed out and returned to the pending queue.
    /// Default: 60.
    /// </summary>
    public int InFlightTimeoutSeconds { get; set; } = 60;

    /// <summary>
    /// Interval in seconds for the reminder that checks for timed-out in-flight messages.
    /// Orleans reminders require a minimum of 60 seconds,
    /// so this value will be enforced to at least 60 at runtime.
    /// Default: 60 (enforced to 60 minimum).
    /// </summary>
    public int ReminderIntervalSeconds { get; set; } = 60;

    /// <summary>
    /// Name of the grain storage provider to use for queue state persistence.
    /// This should match the named storage provider configured on the silo (e.g. "GrainStreamStore").
    /// </summary>
    public string StorageProviderName { get; set; } = "GrainStreamStore";

    /// <summary>Size of the <c>SimpleQueueAdapterCache</c>. Default: 4096.</summary>
    public int CacheSize { get; set; } = 4096;
}
