using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Tail
{
    public class DockerContainerConfiguration
    {
        public ImageSettings Image { get; set; }

        public ContainerSettings Container { get; set; }

        public Func<int, Task<TimeSpan>> WaitUntilAvailable { get; set; } = attempts => Task.FromResult(TimeSpan.Zero);
    }
}
