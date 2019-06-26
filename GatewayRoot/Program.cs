using System;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;

using ObjectManager;
using Kernel.MessageManager;
using Kernel.MQTTManager;
using Kernel.Interface;
using Kernel.QueueManager;
using GatewayService;


namespace Dotnet_JOB_Client
{
    class Program
    {

        //每支程式以不同GUID當成Mutex名稱，可避免執行檔同名同姓的風險
        static string appGuid = "{B19DAFCB-729C-43A6-8232-F3C31BB4E404}";

        private static bool keepRunning = true;

        //--- Main Processing -----------
        static void Main(string[] args)
        {

            // 1. setup our DI
            using ( var serviceProvider = new ServiceCollection()
                .AddSingleton<IQueueManager, Kernel.QueueManager.QueueManager>()
                .AddSingleton<IMessageManager, Kernel.MessageManager.MessageManager>()
                .AddSingleton<IMQTTManager, Kernel.MQTTManager.MQTTManager>()
                .AddSingleton<IManagement,  ObjectManager.ObjectManager>()
                .AddSingleton<IService, GatewayService.GatewayService >()
                .AddLogging(builder =>
                {
                  //  builder.SetMinimumLevel(LogLevel.Trace);
                    builder.AddNLog(new NLogProviderOptions
                    {
                        CaptureMessageTemplates = true,
                        CaptureMessageProperties = true
                    });
                }).BuildServiceProvider() )
               {

                ILogger<Program> logger = serviceProvider.GetService<ILoggerFactory>().CreateLogger<Program>();
                logger.LogInformation("Starting application...Please key in Ctrl + C to quit application");

                // MessageManager.MessageDispatch("/IEW/+/+/ReplyData", new object[] { "this is testing of Message chris to do " });
                //Server need check only one process on going
                //如果要做到跨Session唯一，名稱可加入"Global\"前綴字
                //如此即使用多個帳號透過Terminal Service登入系統
                //整台機器也只能執行一份
                using (Mutex m = new Mutex(false, "Global\\" + appGuid))
                {
                    //檢查是否同名Mutex已存在(表示另一份程式正在執行)
                    if (!m.WaitOne(0, false))
                    {
                        logger.LogError("Duplicate Run, Only one instance is allowed!");
                        return;
                    }

                    //如果是Windows Form，Application.Run()要包在using Mutex範圍內,確保WinForm執行期間Mutex一直存在
                    logger.LogInformation("Initial System...");

                    var Management = serviceProvider.GetServices<IManagement>();
                    var Services = serviceProvider.GetServices<IService>();

                    var QueueManager =  serviceProvider.GetService<IQueueManager>();
                    var MessageManager = serviceProvider.GetService<IMessageManager>();
                    var MQTTManager = serviceProvider.GetService<IMQTTManager>();

                    //------All Service Initial -------
                    Services.ToList().ForEach(o => o.Init());

                    //------All Management Initial -------
                    Management.ToList().ForEach(o => o.Init());

                    try
                    {
                        //-. Handle process Abort Event (Ctrl - C )
                        Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e)
                        {
                            e.Cancel = true;
                            Program.keepRunning = false;
                        };

                        AppDomain.CurrentDomain.ProcessExit += delegate (object sender, EventArgs e)
                        {
                            logger.LogWarning("Process is exiting!");
                        };

                        logger.LogInformation("System Start!!");

                        //-  set thread pool max
                        ThreadPool.SetMaxThreads(16, 16);
                        ThreadPool.SetMinThreads(4, 4);

                        //-  執行無窮迴圈等待 
                        while (Program.keepRunning)
                        {
                            System.Threading.Thread.Sleep(100);
                        }

                        logger.LogWarning("Process exited successfully");
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("Process Error, Msg = "+ ex.Message);
                    }
                }
            }
        }
  }

}

