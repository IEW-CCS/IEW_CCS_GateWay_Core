using System;

using Kernel.Interface;
using Kernel.Common;
using Kernel.QueueManager;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ObjectManager;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Linq;

using System.Collections.Generic;
using System.Collections.Concurrent;



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

        // For DB Used Queue
        public  ConcurrentDictionary<string, Tuple<string, string>> _ProcDB_List = null;   // Key = DB Serial ID, Value = Privader + ConnectString  ;
  
        //------ Key = SerialID_Gateway_Device    value <itemName, index>
        public  ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _EDC_Label_Data = null;

        //------ Key = SerialID_Gateway_Device    value <itemName, index>
        //       Use key from ProcDB List get Connect string to connect 
        public ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _Sync_EDC_Label_Data = null;

        //------ Key = SerialID_Gateway_Device   Value DeviceObject
        public ConcurrentDictionary<string, DBContext.IOT_DEVICE> _IOT_Device = null;


        // public static ConcurrentQueue<Link_sensor_Data> _IOT_DB = new ConcurrentQueue<Link_sensor_Data>();

        private static Timer timer_refresh_database;




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

                _ProcDB_List = new ConcurrentDictionary<string, Tuple<string, string>>();
                _IOT_Device = new ConcurrentDictionary<string, DBContext.IOT_DEVICE>();

                int DB_Refresh_Interval = 10;

                Timer_Refresh_DB(DB_Refresh_Interval);

                //_EDC_Label_Data = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
                // _Sync_EDC_Label_Data = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
                // _IOT_Device = new ConcurrentDictionary<string, DBContext.IOT_DEVICE>();



               


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

        public void Timer_Refresh_DB(int interval)
        {
            if (interval == 0)
                interval = 60000;  // 60s

            //使用匿名方法，建立帶有參數的委派
            System.Threading.Thread Thread_Timer_Refresh_DB = new System.Threading.Thread
            (
               delegate (object value)
               {
                   int Interval = Convert.ToInt32(value);
                   timer_refresh_database = new Timer(new TimerCallback(Builde_Update_DB_Information), null, 0, Interval);
               }
            );
            Thread_Timer_Refresh_DB.Start(interval);
        }


        //----------- Threading Timer Function implement --------------
        public void Builde_Update_DB_Information(object timerState)
        {
            try
            {
                foreach (KeyValuePair<string, Tuple<string, string>> kvp in _ProcDB_List)
                {
                   
                    // "MS SQL" / "My SQL"  ""MS SQL"", "server= localhost;database=IoTDB;user=root;password=qQ123456")
                    using (var db = new DBContext.IOT_DbContext(kvp.Value.Item1, kvp.Value.Item2))
                    {
                        var vIOT_Device_Result = db.IOT_DEVICE.AsQueryable();
                        var _IOT_Status = vIOT_Device_Result.ToList();

                        foreach(DBContext.IOT_DEVICE Device in _IOT_Status)
                        {
                            string _IOT_Device_key = string.Concat(kvp.Key, "_", Device.gateway_id, "_", Device.device_id);


                        }





                    }


                }

                /*
                using (var db = new IOT_DbContext())
                {
                    var vEDC_Label_Result = db.IOT_DEVICE_EDC_LABEL.AsQueryable().ToList();
                    foreach (IOT_DEVICE_EDC_LABEL _EDC_Label_key in vEDC_Label_Result)
                    {
                        ConcurrentDictionary<string, int> _Sub_EDC_Labels = Program._EDC_Label_Data.GetOrAdd(_EDC_Label_key.device_id, new ConcurrentDictionary<string, int>());
                        _Sub_EDC_Labels.AddOrUpdate(_EDC_Label_key.data_name, _EDC_Label_key.data_index, (key, oldvalue) => _EDC_Label_key.data_index);
                        Program._EDC_Label_Data.AddOrUpdate(_EDC_Label_key.device_id, _Sub_EDC_Labels, (key, oldvalue) => _Sub_EDC_Labels);

                        //---------------------------------------------------
                        ConcurrentDictionary<string, Tuple<double, double>> _Sub_EDC_Spec = Program._EDC_Item_Spec.GetOrAdd(_EDC_Label_key.device_id, new ConcurrentDictionary<string, Tuple<double, double>>());
                        _Sub_EDC_Spec.AddOrUpdate(_EDC_Label_key.data_name, Tuple.Create(_EDC_Label_key.lspec, _EDC_Label_key.uspec), (key, oldvalue) => Tuple.Create(_EDC_Label_key.lspec, _EDC_Label_key.uspec));
                        Program._EDC_Item_Spec.AddOrUpdate(_EDC_Label_key.device_id, _Sub_EDC_Spec, (key, oldvalue) => _Sub_EDC_Spec);

                    }

                    var vIOT_Device_Result = db.IOT_DEVICE.AsQueryable().ToList();
                    foreach (IOT_DEVICE _IOT_Device_key in vIOT_Device_Result)
                    {
                        Program._IOT_Device.AddOrUpdate(_IOT_Device_key.device_id, _IOT_Device_key, (key, oldvalue) => _IOT_Device_key);
                    }

                    var vIOT_EDC_Gls_Info_Result = db.IOT_EDC_GLS_INFO.AsQueryable().ToList();
                    foreach (IOT_EDC_GLS_INFO _IOT_EDC_Gls_Info_key in vIOT_EDC_Gls_Info_Result)
                    {
                        Program._IOT_EDC_GlassInfo.AddOrUpdate(_IOT_EDC_Gls_Info_key.sub_eqp_id, _IOT_EDC_Gls_Info_key, (key, oldvalue) => _IOT_EDC_Gls_Info_key);
                    }

                    if (_Sync_EDC_Label_Data.Count != 0)
                    {
                        foreach (KeyValuePair<string, ConcurrentDictionary<string, int>> kvp in _Sync_EDC_Label_Data)
                        {
                            ConcurrentDictionary<string, int> _Process;
                            string Key = kvp.Key;
                            _Sync_EDC_Label_Data.TryRemove(Key, out _Process);

                            foreach (KeyValuePair<string, int> _EDC_item in _Process)
                            {
                                IOT_DEVICE_EDC_LABEL oIoT_Device_EDC_label = new IOT_DEVICE_EDC_LABEL();
                                oIoT_Device_EDC_label.device_id = Key;
                                oIoT_Device_EDC_label.data_name = _EDC_item.Key;
                                oIoT_Device_EDC_label.data_index = _EDC_item.Value;
                                oIoT_Device_EDC_label.clm_date_time = DateTime.Now;
                                oIoT_Device_EDC_label.clm_user = "system";
                                db.Add(oIoT_Device_EDC_label);

                            }
                        }
                        db.SaveChanges();
                    }

                }

                if (Load_DB_Finished == false)
                {
                    Console.WriteLine("Download DB Finished Starting Processing");
                    Console.WriteLine("==========================================");
                }

                log.InfoFormat("[{0}] {2}-({1})", "Program", MethodBase.GetCurrentMethod().Name, "Update_DB Finished");
                Load_DB_Finished = true;
            }
            catch (Exception ex)
            {
                log.ErrorFormat("[{0}] {2}-({1})", "Program", MethodBase.GetCurrentMethod().Name, "Update_DB Faild ex : " + ex.Message);
                Console.WriteLine(ex.Message);
            }*/
            }

            catch (Exception ex)
            { 
            }

        }

        //------ Use ProcDB 預先抓DB 設定到Local ----
        public void Update_ProcDB_List(string Serial_ID, string DBPrivoder, string ConnectString)
        {
            _ProcDB_List.AddOrUpdate(Serial_ID, Tuple.Create(DBPrivoder, ConnectString), (key, oldvalue) => Tuple.Create(DBPrivoder, ConnectString));
        }


        // public void CheckEDC_Label()
        /*
         private void Create_EDC_Label_Data(string PointID)
         {

             int Count = 1;
             ConcurrentDictionary<string, int> _Add_EDC_Labels = new ConcurrentDictionary<string, int>();
             foreach (string item_name in Program.item_names)
             {
                 _Add_EDC_Labels.AddOrUpdate(item_name, Count, (key, oldvalue) => Count);
                 Count++;
             }
             Program._EDC_Label_Data.AddOrUpdate(_Collect_data.PointID, _Add_EDC_Labels, (key, oldvalue) => _Add_EDC_Labels);
             Program._Sync_EDC_Label_Data.AddOrUpdate(_Collect_data.PointID, _Add_EDC_Labels, (key, oldvalue) => _Add_EDC_Labels);

         }
         private void Update_EDC_Label_Data(string PointID)
         {
             ConcurrentDictionary<string, int> _Current_Device_EDC_Label = Program._EDC_Label_Data.GetOrAdd(PointID, new ConcurrentDictionary<string, int>());
             ConcurrentDictionary<string, int> _Sync_Device_EDC_Label = new ConcurrentDictionary<string, int>();

             foreach (string EDC_items in Program.item_names)
             {
                 if (_Current_Device_EDC_Label.ContainsKey(EDC_items) == false)
                 {
                     int _insert_index = _Current_Device_EDC_Label.Count + 1;
                     _Current_Device_EDC_Label.AddOrUpdate(EDC_items, _insert_index, (key, oldvalue) => _insert_index);
                     _Sync_Device_EDC_Label.AddOrUpdate(EDC_items, _insert_index, (key, oldvalue) => _insert_index);
                 }
             }

             Program._EDC_Label_Data.AddOrUpdate(_Collect_data.PointID, _Current_Device_EDC_Label, (key, oldvalue) => _Current_Device_EDC_Label);
             Program._Sync_EDC_Label_Data.AddOrUpdate(_Collect_data.PointID, _Sync_Device_EDC_Label, (key, oldvalue) => _Sync_Device_EDC_Label);

         }
         */



        public void ReceiveMQTTData(xmlMessage InputData)
        {
            // Parse Mqtt Topic
            string Topic = InputData.MQTTTopic;
            string Payload = InputData.MQTTPayload;

            try
            {
                ProcDBData DBProc = new ProcDBData(Payload);
              
                if (DBProc != null)
                {
                    ThreadPool.QueueUserWorkItem(o => DBProc.ThreadPool_Proc());
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(string.Format("EDC Service Handle ReceiveMQTTData Exception Msg : {0}. ", ex.Message));
            }

        }

    }

    // 擴充方法
    public static class PropertyExtension
    {
        public static void SetPropertyValue(this object obj, string propName, object value)
        {
            obj.GetType().GetProperty(propName).SetValue(obj, value, null);
        }
    }

    public class ProcDBData
    {

       

        DBPartaker objDB = null;

        public ProcDBData(string inputdata)
        {
            try
            {
                this.objDB = JsonConvert.DeserializeObject<DBPartaker>(inputdata.ToString());
                //_Put_Write = _delegate_Method;
                //_Interval_Event = _delegate_Interval_Event;
            }
            catch
            {
                this.objDB = null;
            }

        }
        public void ThreadPool_Proc()
        {

           
            //---- 解析完確認 Dictionary 是否存在

            /*
            try
            {
                //----
                if (Program._EDC_Label_Data.ContainsKey(_Collect_data.PointID) == false)
                    Create_EDC_Label_Data(_Collect_data.PointID);
                else if (Program._EDC_Label_Data[_Collect_data.PointID].Values.Count != Program.item_names.Length)
                    Update_EDC_Label_Data(_Collect_data.PointID);

                try
                {
                    Link_sensor_Data temp_link_sensor = new Link_sensor_Data();
                    temp_link_sensor = (Link_sensor_Data)_Collect_data.Clone();
                    Program._IOT_DB.Enqueue(temp_link_sensor);
                    Program.log.InfoFormat("[{0}] {2}-({1})", GetType().Name, MethodBase.GetCurrentMethod().Name, "Insert DB Struct Device : " + _Device_ID);

                }
                catch (Exception ex)
                {
                    Program.log.ErrorFormat("[{0}] {2}-({1})", GetType().Name, MethodBase.GetCurrentMethod().Name, "Insert DB Faild Deiver : " + _Device_ID + " ex : " + ex.Message);
                    Console.WriteLine(ex.Message);
                }

            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Handle Process DB Data Error Message Error [{0}].", ex.Message));
            }*/
        }

    }
}
