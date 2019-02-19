using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tail
{
    public interface IScheduler
    {
        Task ScheduleTellOnceAsync(Action action, TimeSpan due, CancellationToken token = default);
    }
}
