using System;
using System.Linq;
using System.Threading.Tasks;
using NodaTime;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace Tail
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // Configuration section
            const int NumberOfProducers = 200;
            const int NumberOfConsumers = 10;
            const ProducerAppendBehavior Behavior = ProducerAppendBehavior.Singular;
            const bool UseSnapshotIsolation = true;

            var container = new SqlServerContainer();
            try
            {
                Console.WriteLine("Creating sql server container ...");
                await container.InitializeAsync();
                Console.WriteLine("Created.");
                var db = await container.CreateDatabaseAsync(UseSnapshotIsolation);
                Console.WriteLine("ConnectionString={0}", db.ConnectionString);
                using (var store = new MsSqlStreamStore(new MsSqlStreamStoreSettings(db.ConnectionString)
                {
                    Schema = "dbo"
                }))
                {
                    Console.WriteLine("Creating sql stream store schema ...");
                    await store.CreateSchema();
                    Console.WriteLine("Created.");
                    using(var scheduler = new Scheduler(SystemClock.Instance))
                    {
                        var producers = Enumerable
                            .Range(1, NumberOfProducers)
                            .Select(id => new Producer(id, Behavior, store, scheduler))
                            .ToArray();
                        
                        var consumers = Enumerable
                            .Range(1, NumberOfConsumers)
                            .Select(id => new Consumer(id, store, scheduler))
                            .ToArray();
                        
                        Console.WriteLine("Starting {0} producers ...", NumberOfProducers);
                        Array.ForEach(producers, producer => producer.Start());
                        Console.WriteLine("Started.");
                        
                        Console.WriteLine("Starting {0} consumers ...", NumberOfConsumers);
                        Array.ForEach(consumers, consumer => consumer.Start());
                        Console.WriteLine("Started.");
                        
                        Console.WriteLine("Press enter to exit");
                        Console.ReadLine();
                        
                        Console.WriteLine("Stopping {0} producers ...", NumberOfProducers);
                        Array.ForEach(producers, producer => producer.Stop());
                        Console.WriteLine("Stopped.");

                        Console.WriteLine("Stopping {0} consumers ...", NumberOfConsumers);
                        Array.ForEach(consumers, consumer => consumer.Stop());
                        Console.WriteLine("Stopped.");
                    }
                }
            }
            finally
            {
                Console.WriteLine("Removing sql server container ...");
                await container.DisposeAsync();
                Console.WriteLine("Removed.");
            }
        }
    }
}