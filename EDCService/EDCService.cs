using System;
using System.Collections.Generic;
using System.Text;

//Add
using System.IO;
using System.IO.Ports;
using System.Xml;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;

using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ObjectManager;


namespace EDCService
{

    public class EDCService : IService, IDisposable
    {
        private string _SeviceName = "EDCService";
        public string ServiceName
        {
            get
            {
                return this._SeviceName;
            }
        }


        public delegate void Put_Write_Event(string Path, string Body);
      

        private ConcurrentQueue<Tuple<string, string>> _Write_EDC_File = new ConcurrentQueue<Tuple<string, string>>();
        private bool _run = false;

        private Thread th_ReportEDC = null;

        // For IOC/DI Used
        private readonly IQueueManager _QueueManager;
        private readonly IManagement _ObjectManager;
        private ObjectManager.ObjectManager _objectmanager = null;
        private readonly ILogger<EDCService> _logger;


        public EDCService(ILoggerFactory loggerFactory, IServiceProvider serviceProvider)
        {
            _QueueManager = serviceProvider.GetService<IQueueManager>();
            _ObjectManager = serviceProvider.GetServices<IManagement>().Where(o => o.ManageName.Equals("ObjectManager")).FirstOrDefault();
            _logger = loggerFactory.CreateLogger<EDCService>();
        }

        public void Init()
        {
            bool ret = false;
            try
            {
                _run = true;
                this.th_ReportEDC = new Thread(new ThreadStart(EDC_Writter));
                this.th_ReportEDC.Start();
               // NLogManager.Logger.LogInfo(LogName, GetType().Name, MethodInfo.GetCurrentMethod().Name + "()", "Initial Finished");
                ret = true;
            }
            catch (Exception ex)
            {
                // NLogManager.Logger.LogError(LogName, GetType().Name, MethodInfo.GetCurrentMethod().Name + "()", ex);
                Dispose();
                ret = false;
            }
            //NLogManager.Logger.LogInfo(LogName, GetType().Name, MethodInfo.GetCurrentMethod().Name + "()", "MQTT_Service_Initial Finished");
        }

        public void Dispose()
        {
            _run = false;
        }

        public void EDC_File_Enqueue(string SavePath, string EDC_string)
        {

            _Write_EDC_File.Enqueue(Tuple.Create(SavePath, EDC_string));
           
        }

        private void EDC_Writter()
        {
            while (_run)
            {
                if (_Write_EDC_File.Count > 0)
                {
                    Tuple<string, string> _msg = null;
                    while (_Write_EDC_File.TryDequeue(out _msg))
                    {

                        string Timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
                        string save_file_path = _msg.Item1.Replace("{datetime}", Timestamp);


                        string EDC_Data = _msg.Item2;

                        try
                        {
                            if (!Directory.Exists(Path.GetDirectoryName(save_file_path)))
                            {
                                Directory.CreateDirectory(Path.GetDirectoryName(save_file_path));
                            }
                            string temp_name = save_file_path + ".tmp";
                            using (FileStream fs = System.IO.File.Open(temp_name, FileMode.Create))
                            {
                                using (StreamWriter EDCWriter = new StreamWriter(fs, Encoding.UTF8))
                                {
                                    EDCWriter.Write(EDC_Data);
                                    EDCWriter.Flush();
                                    fs.Flush();
                                }
                            }

                            if (System.IO.File.Exists(save_file_path))
                                System.IO.File.Delete(save_file_path);
                            while (System.IO.File.Exists(save_file_path))
                                Thread.Sleep(1);
                            System.IO.File.Move(temp_name, save_file_path);
                           // NLogManager.Logger.LogInfo("Service", "EDC_Writter", MethodInfo.GetCurrentMethod().Name + "()", string.Format("Save EDC File Successful Path: {0}.", save_file_path));

                        }
                        catch (Exception ex)
                        {
                          //  NLogManager.Logger.LogError("Service", "EDC_Writter", MethodInfo.GetCurrentMethod().Name + "()", string.Format("Write EDC File Faild Exception Msg : {0}. ", ex.Message));
                        }
                    }
                }

                Thread.Sleep(10);
            }
        }

        public void ReceiveMQTTData(xmlMessage InputData)
        {
            // Parse Mqtt Topic
            string Topic = InputData.MQTTTopic;
            string Payload = InputData.MQTTPayload;

            ProcEDCData EDCProc = new ProcEDCData(Payload, EDC_File_Enqueue);
            if (EDCProc != null)
            {
                ThreadPool.QueueUserWorkItem(o => EDCProc.ThreadPool_Proc());
            }


        }

    }
        public class ProcEDCData
        {
            EDCPartaker objEDC = null;
            public EDCService.Put_Write_Event _Put_Write;
            public ProcEDCData(string inputdata, EDCService.Put_Write_Event _delegate_Method )
            {
                try
                {
                    this.objEDC = JsonConvert.DeserializeObject<EDCPartaker>(inputdata.ToString());
                    _Put_Write = _delegate_Method;
                }
                catch (Exception ex)
                {
                    this.objEDC = null;

                }

            }
            public void ThreadPool_Proc()
            {
                try
                {
                    switch (objEDC.report_tpye)
                    {
                        // Trigger 代表一來直接寫檔案.
                        case "trigger":

                            string EDC_string = string.Empty;
                            EDC_string = objEDC.xml_string();

                            if (EDC_string != string.Empty)
                            {

                                string SavePath = objEDC.ReportEDCPath;
                                 _Put_Write(SavePath, EDC_string);
                               
                            }

                            break;

                        case "interval":

                            // keep interval report 需要討論要上報什麼內容 (MAX, Min, AVG or others...)
                            break;

                        default:
                            break;

                    }
                }
                catch (Exception ex)
                {

                }
            }
        }
}
