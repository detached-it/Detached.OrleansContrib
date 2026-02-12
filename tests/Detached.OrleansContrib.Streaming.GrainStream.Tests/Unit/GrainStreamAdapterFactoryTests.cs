// -----------------------------------------------------------------------
// <copyright file="GrainStreamAdapterFactoryTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Adapter;
using Detached.OrleansContrib.Streaming.GrainStream.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using Orleans.Configuration;
using Orleans.Streams;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

public sealed class GrainStreamAdapterFactoryTests
{
    [Fact]
    public async Task CreateAdapter_ReturnsGrainStreamQueueAdapter()
    {
        var options = new GrainStreamOptions { NumQueues = 4, CacheSize = 1024 };
        var grainFactory = Mock.Of<IGrainFactory>();
        var loggerFactory = new LoggerFactory();
        var queueMapperOptions = new HashRingStreamQueueMapperOptions { TotalQueueCount = 4 };
        var cacheOptions = new SimpleQueueCacheOptions { CacheSize = 1024 };

        var factory = new GrainStreamAdapterFactory(
            "TestProvider", options, grainFactory, loggerFactory, queueMapperOptions, cacheOptions);

        var adapter = await factory.CreateAdapter();

        Assert.NotNull(adapter);
        Assert.IsType<GrainStreamQueueAdapter>(adapter);
        Assert.Equal("TestProvider", adapter.Name);
    }

    [Fact]
    public void GetQueueAdapterCache_ReturnsNonNull()
    {
        var factory = CreateFactory();
        var cache = factory.GetQueueAdapterCache();
        Assert.NotNull(cache);
    }

    [Fact]
    public void GetStreamQueueMapper_ReturnsNonNull()
    {
        var factory = CreateFactory();
        var mapper = factory.GetStreamQueueMapper();
        Assert.NotNull(mapper);
    }

    [Fact]
    public async Task GetDeliveryFailureHandler_ReturnsNoOp()
    {
        var factory = CreateFactory();
        var queueId = QueueId.GetQueueId("TestProvider", 0, 0);
        var handler = await factory.GetDeliveryFailureHandler(queueId);
        Assert.NotNull(handler);
    }

    [Fact]
    public void GetStreamQueueMapper_ReturnsCorrectQueueCount()
    {
        var factory = CreateFactory(numQueues: 4);
        var mapper = factory.GetStreamQueueMapper();
        var queues = mapper.GetAllQueues().ToList();
        Assert.Equal(4, queues.Count);
    }

    [Fact]
    public void Constructor_ThrowsOnNullArguments()
    {
        var options = new GrainStreamOptions();
        var grainFactory = Mock.Of<IGrainFactory>();
        var loggerFactory = new LoggerFactory();
        var qmo = new HashRingStreamQueueMapperOptions();
        var co = new SimpleQueueCacheOptions();

        Assert.Throws<ArgumentNullException>(() => new GrainStreamAdapterFactory(null!, options, grainFactory, loggerFactory, qmo, co));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamAdapterFactory("n", null!, grainFactory, loggerFactory, qmo, co));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamAdapterFactory("n", options, null!, loggerFactory, qmo, co));
        Assert.Throws<ArgumentNullException>(() => new GrainStreamAdapterFactory("n", options, grainFactory, null!, qmo, co));
    }

    private static GrainStreamAdapterFactory CreateFactory(int numQueues = 8)
    {
        return new GrainStreamAdapterFactory(
            "TestProvider",
            new GrainStreamOptions { NumQueues = numQueues },
            Mock.Of<IGrainFactory>(),
            new LoggerFactory(),
            new HashRingStreamQueueMapperOptions { TotalQueueCount = numQueues },
            new SimpleQueueCacheOptions { CacheSize = 1024 });
    }
}
