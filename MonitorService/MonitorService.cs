using System;
using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace MonitorService
{
    public class MonitorService : IService, IDisposable
    {
        private string _SeviceName = "MonitorService";
        private readonly ILogger<MonitorService> _logger;

        public string ServiceName
        {
            get
            {
                return this._SeviceName;
            }
        }

        public void Init()
        {

        }

        public void Dispose()
        {

        }

        public MonitorService(ILoggerFactory loggerFactory, IServiceProvider serviceProvider)
        {
            _logger = loggerFactory.CreateLogger<MonitorService>();
        }

    }
}
