using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tail
{
    public interface IScheduler
    {
        void ScheduleTellOnce(Action action, TimeSpan due);
        Task ScheduleTellOnceAsync(Action action, TimeSpan due, CancellationToken token = default);
    }
}
