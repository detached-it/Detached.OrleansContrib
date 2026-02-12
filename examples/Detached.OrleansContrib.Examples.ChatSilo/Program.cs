// -----------------------------------------------------------------------
// <copyright file="Program.cs" company="Detached IT">
//     ©2026 Detached IT. All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using Detached.OrleansContrib.Streaming.GrainStream.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseOrleans(silo =>
{

    silo.UseTransactions();
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.AddMemoryGrainStorage("ChatStreamProvider");
    silo.AddMemoryGrainStorage("GrainStreamStore");
    silo.UseInMemoryReminderService();
    silo.AddGrainStream("ChatStreamProvider", options =>
    {
        options.StorageProviderName = "GrainStreamStore";
    });
});

var app = builder.Build();

app.MapGet("/", () => "Orleans Chat Silo is running!");

app.Run();
