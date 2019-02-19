using System;
using System.Linq;
using System.Threading.Tasks;
using NodaTime;
using SqlStreamStore;

namespace Tail
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            const int NumberOfProducers = 100;

            var container = new SqlServerContainer();
            try
            {
                Console.WriteLine("Creating sql server container ...");
                await container.InitializeAsync();
                Console.WriteLine("Created.");
                var db = await container.CreateDatabaseAsync();
                Console.WriteLine("ConnectionString={0}", db.ConnectionString);
                using (var store = new MsSqlStreamStore(new MsSqlStreamStoreSettings(db.ConnectionString)
                {
                    Schema = "dbo"
                }))
                {
                    Console.WriteLine("Creating sql stream store schema ...");
                    await store.CreateSchema();
                    Console.WriteLine("Created.");

                    var clock = SystemClock.Instance;
                    var scheduler = new Scheduler(clock);
                    var producers = Enumerable
                        .Range(0, NumberOfProducers)
                        .Select(id => new Producer(id, store, scheduler))
                        .ToArray();
                    Console.WriteLine("Starting producers ...");
                    Array.ForEach(producers, producer => producer.Start());
                    Console.WriteLine("Started.");
                    Console.WriteLine("Press enter to exit");
                    Console.ReadLine();
                    Console.WriteLine("Stopping producers ...");
                    Array.ForEach(producers, producer => producer.Stop());
                    Console.WriteLine("Stopped.");
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