using System;

using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ObjectManager;

using System.Linq;


namespace DBService
{
    public class DBService : IService, IDisposable
    {

        private string _SeviceName = "DBService";
        public string ServiceName
        {
            get
            {
                return this._SeviceName;
            }
        }


        // For IOC/DI Used
        private readonly IQueueManager _QueueManager;
        private readonly IManagement _ObjectManager;
        private ObjectManager.ObjectManager _objectmanager = null;
        private readonly ILogger<DBService> _logger;
        private bool _run = false;

        public DBService(ILoggerFactory loggerFactory, IServiceProvider serviceProvider)
        {
            _QueueManager = serviceProvider.GetService<IQueueManager>();
            _ObjectManager = serviceProvider.GetServices<IManagement>().Where(o => o.ManageName.Equals("ObjectManager")).FirstOrDefault();
            _logger = loggerFactory.CreateLogger<DBService>();
        }

        public void Init()
        {

            try
            {
                _run = true;

                using (var db = new DBContext.IOT_DbContext("MY", "server= localhost;database=IoTDB;user=root;password=qQ123456"))
                {
                    var vIOT_Device_Result = db.IOT_DEVICE.AsQueryable();
                    var _IOT_Status = vIOT_Device_Result.Where(c => (c.device_id == "VIBR002")).FirstOrDefault();
                   
                }




            }
            catch (Exception ex)
            {
                Dispose();
               // _logger.LogError("EDC Service Initial Faild, Exception Msg = " + ex.Message);
            }

        }

        public void Dispose()
        {
            _run = false;
        }






         



    }
}
