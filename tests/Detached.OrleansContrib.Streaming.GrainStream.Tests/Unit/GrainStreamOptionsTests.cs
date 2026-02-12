// -----------------------------------------------------------------------
// <copyright file="GrainStreamOptionsTests.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Configuration;

namespace Detached.OrleansContrib.Streaming.GrainStream.Tests.Unit;

public sealed class GrainStreamOptionsTests
{
    [Fact]
    public void DefaultValues_AreCorrect()
    {
        var options = new GrainStreamOptions();

        Assert.Equal(8, options.NumQueues);
        Assert.Equal(100, options.MaxBatchSize);
        Assert.Equal(60, options.InFlightTimeoutSeconds);
        Assert.Equal(60, options.ReminderIntervalSeconds);
        Assert.Equal("GrainStreamStore", options.StorageProviderName);
        Assert.Equal(4096, options.CacheSize);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    [InlineData(64)]
    public void NumQueues_CanBeSet(int numQueues)
    {
        var options = new GrainStreamOptions { NumQueues = numQueues };
        Assert.Equal(numQueues, options.NumQueues);
    }

    [Fact]
    public void AllProperties_CanBeCustomised()
    {
        var options = new GrainStreamOptions
        {
            NumQueues = 4,
            MaxBatchSize = 50,
            InFlightTimeoutSeconds = 120,
            ReminderIntervalSeconds = 15,
            StorageProviderName = "CustomStore",
            CacheSize = 2048
        };

        Assert.Equal(4, options.NumQueues);
        Assert.Equal(50, options.MaxBatchSize);
        Assert.Equal(120, options.InFlightTimeoutSeconds);
        Assert.Equal(15, options.ReminderIntervalSeconds);
        Assert.Equal("CustomStore", options.StorageProviderName);
        Assert.Equal(2048, options.CacheSize);
    }
}
