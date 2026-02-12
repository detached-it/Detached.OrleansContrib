// -----------------------------------------------------------------------
// <copyright file="TestConsumerGrain.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.TestGrains;

/// <summary>
/// Interface for a test consumer grain that subscribes to a stream and records received events.
/// </summary>
[Alias("Detached.OrleansContrib.Streaming.GrainStream.Tests.TestGrains.ITestConsumerGrain")]
public interface ITestConsumerGrain : IGrainWithGuidKey
{
    [Alias("Subscribe")]
    Task Subscribe(string streamProviderName, string streamNamespace, Guid streamKey);
    [Alias("GetReceivedEvents")]
    Task<List<string>> GetReceivedEvents();
    [Alias("GetReceivedCount")]
    Task<int> GetReceivedCount();
}

/// <summary>
/// Test consumer grain that subscribes to a stream and collects received events.
/// </summary>
public sealed class TestConsumerGrain : Grain, ITestConsumerGrain
{
    private readonly List<string> _receivedEvents = [];
    private StreamSubscriptionHandle<string>? _subscription;

    public async Task Subscribe(string streamProviderName, string streamNamespace, Guid streamKey)
    {
        var streamProvider = this.GetStreamProvider(streamProviderName);
        var stream = streamProvider.GetStream<string>(StreamId.Create(streamNamespace, streamKey.ToString()));
        _subscription = await stream.SubscribeAsync(OnNextAsync);
    }

    private Task OnNextAsync(string item, StreamSequenceToken? token)
    {
        _receivedEvents.Add(item);
        return Task.CompletedTask;
    }

    public Task<List<string>> GetReceivedEvents() =>
        Task.FromResult(new List<string>(_receivedEvents));

    public Task<int> GetReceivedCount() =>
        Task.FromResult(_receivedEvents.Count);
}
