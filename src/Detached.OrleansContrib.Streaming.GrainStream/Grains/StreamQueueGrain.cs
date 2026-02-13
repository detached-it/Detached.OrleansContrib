// -----------------------------------------------------------------------
// <copyright file="StreamQueueGrain.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Detached.OrleansContrib.Streaming.GrainStream.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Transactions.Abstractions;

namespace Detached.OrleansContrib.Streaming.GrainStream.Grains;

/// <summary>
/// Grain implementation for a single queue partition.
/// Uses transactional state for persistence and reminders for dead-letter redelivery.
/// </summary>
public sealed class StreamQueueGrain : Grain, IStreamQueueGrain, IRemindable
{
    internal const string ReminderName = "DeadLetterCheck";

    private readonly ITransactionalState<StreamQueueState> _state;
    private readonly GrainStreamOptions _options;
    private readonly ILogger<StreamQueueGrain> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamQueueGrain"/> class.
    /// </summary>
    /// <param name="state">The transactional state for the queue.</param>
    /// <param name="options">Grain stream options.</param>
    /// <param name="logger">Logger instance.</param>
    public StreamQueueGrain(
        [TransactionalState("queueState", "GrainStreamStore")]
        ITransactionalState<StreamQueueState> state,
        IOptions<GrainStreamOptions> options,
        ILogger<StreamQueueGrain> logger)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        await base.OnActivateAsync(cancellationToken);
        // Orleans requires reminder period to be at least 60 seconds
        var intervalSeconds = Math.Max(_options.ReminderIntervalSeconds, 60);
        var interval = TimeSpan.FromSeconds(intervalSeconds);
        await this.RegisterOrUpdateReminder(ReminderName, interval, interval);
        _logger.LogDebug("StreamQueueGrain {GrainId} activated, reminder registered at {Interval}s interval.",
            this.GetPrimaryKeyString(), intervalSeconds);
    }

    /// <inheritdoc/>
    public async Task EnqueueAsync(List<StreamMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);

        await _state.PerformUpdate(state =>
        {
            foreach (var msg in messages)
            {
                msg.SequenceNumber = state.NextSequenceNumber++;
                state.PendingMessages.Add(msg);
            }
        });

        _logger.LogDebug("Enqueued {Count} messages into queue {GrainId}.", messages.Count, this.GetPrimaryKeyString());
    }

    /// <inheritdoc/>
    public async Task<List<StreamMessage>> DequeueAsync(int maxCount)
    {
        var result = new List<StreamMessage>();

        await _state.PerformUpdate(state =>
        {
            var count = Math.Min(maxCount, state.PendingMessages.Count);
            if (count == 0) return;

            var batch = state.PendingMessages.GetRange(0, count);
            state.PendingMessages.RemoveRange(0, count);

            foreach (var msg in batch)
            {
                state.InFlightMessages[msg.MessageId] = new InFlightMessage
                {
                    Message = msg,
                    DeliveredAtUtc = DateTime.UtcNow
                };
                result.Add(msg);
            }
        });

        if (result.Count > 0)
        {
            _logger.LogDebug("Dequeued {Count} messages from queue {GrainId}.", result.Count, this.GetPrimaryKeyString());
        }

        return result;
    }

    /// <inheritdoc/>
    public async Task AcknowledgeAsync(List<Guid> messageIds)
    {
        ArgumentNullException.ThrowIfNull(messageIds);

        await _state.PerformUpdate(state =>
        {
            foreach (var id in messageIds)
            {
                state.InFlightMessages.Remove(id);
            }
        });

        _logger.LogDebug("Acknowledged {Count} messages in queue {GrainId}.", messageIds.Count, this.GetPrimaryKeyString());
    }

    /// <inheritdoc/>
    public async Task<int> GetQueueLengthAsync()
    {
        return await _state.PerformRead(state =>
            state.PendingMessages.Count + state.InFlightMessages.Count);
    }

    /// <inheritdoc/>
    [Transaction(TransactionOption.CreateOrJoin)]
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != ReminderName) return;
        await this.AsReference<IStreamQueueGrain>().ProcessDeadLetterCheckAsync();
    }

    /// <inheritdoc/>
    [Transaction(TransactionOption.CreateOrJoin)]
    public async Task ProcessDeadLetterCheckAsync()
    {
        var timeout = TimeSpan.FromSeconds(_options.InFlightTimeoutSeconds);
        var now = DateTime.UtcNow;
        var requeued = 0;

        await _state.PerformUpdate(state =>
        {
            var timedOut = state.InFlightMessages
                .Where(kvp => now - kvp.Value.DeliveredAtUtc > timeout)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var id in timedOut)
            {
                if (state.InFlightMessages.Remove(id, out var inFlight))
                {
                    state.PendingMessages.Add(inFlight.Message);
                    requeued++;
                }
            }
        });

        if (requeued > 0)
        {
            _logger.LogWarning("Re-queued {Count} timed-out in-flight messages in queue {GrainId}.",
                requeued, this.GetPrimaryKeyString());
        }
    }
}
