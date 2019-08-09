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
using DBService.DBContext;
using System.Diagnostics;
using System.Xml.Linq;


// 20190723 Database 1st connected   then to do receive data actions.

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


        private const string _GatewayID = "GatewayID"; 
        private const string _DeviceID = "DeviceID";

        // For IOC/DI Used
        private readonly IQueueManager _QueueManager;
        private readonly IManagement _ObjectManager;
        private ObjectManager.ObjectManager _objectmanager = null;
        private readonly ILogger<DBService> _logger;

        //------ Key = SerialID_Gateway_Device   Value DeviceObject
        public  ConcurrentDictionary<string, DBContext.IOT_DEVICE> _IOT_Device = null;
       
        //------ Key = SerialID_Gateway_Device    value <itemName, index>
        public  ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _EDC_Label_Data = null;
        public  ConcurrentDictionary<string, ConcurrentDictionary<string, int>> _Sync_EDC_Label_Data = null;

        private static Timer timer_refresh_database;
        private static Timer timer_report_DB;

        //----- Key = DB Provider_ConnectString ------
        private ConcurrentDictionary<string, ConcurrentQueue<DBPartaker>> _dic_DB_Partaker =  null;

        // -- Delegate Method
        public delegate void Add_DBPartaker_to_dict_Event( DBPartaker DBP);

        // -- read config .ini
        private Dictionary<string, string> _dic_Basicsetting = new Dictionary<string, string>();


        private bool _Initial_Finished = false;
   


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

                string Config_Path = AppContext.BaseDirectory + "/settings/System.xml";
                Load_Xml_Config_To_Dict(Config_Path);

                _objectmanager = (ObjectManager.ObjectManager)_ObjectManager.GetInstance;

                _IOT_Device          = new ConcurrentDictionary<string, DBContext.IOT_DEVICE>();
                _EDC_Label_Data      = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
                _Sync_EDC_Label_Data = new ConcurrentDictionary<string, ConcurrentDictionary<string, int>>();
                _dic_DB_Partaker     = new ConcurrentDictionary<string, ConcurrentQueue<DBPartaker>>();

                int DB_Refresh_Interval = 60000;  // 60 秒
                int DB_Report_Interval =   5000;    // 5 秒
                Timer_Refresh_DB(DB_Refresh_Interval);  //定時Refresh DB Information to local
                Timer_Report_DB(DB_Report_Interval);    //定時Report Data to DB 

                _Initial_Finished = false; // 等待IOT Client 與 CCS 完成通訊以後才開始動作

                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0000", "this is testing", "ERROR");


            }

            catch (Exception ex)
            {
                Dispose();
                string ErrorLog = "DB Service Initial Faild, Exception Msg = " + ex.Message;
                 _logger.LogError(ErrorLog);
                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0001", ErrorLog, "ERROR");


            }
        }

        public void Dispose()
        {
            
        }

        #region Timer Thread
        public void Timer_Refresh_DB(int interval)
        {
            if (interval == 0)
                interval = 10000;  

            //使用匿名方法，建立帶有參數的委派
            System.Threading.Thread Thread_Timer_Refresh_DB = new System.Threading.Thread
            (
               delegate (object value)
               {
                   int Interval = Convert.ToInt32(value);
                   timer_refresh_database = new Timer(new TimerCallback(Routine_Update_DB_Information), null, 0, Interval);

               }
            );
            Thread_Timer_Refresh_DB.Start(interval);
        }

        public void Timer_Report_DB(int interval)
        {
            if (interval == 0)
                interval = 10000;  
            //使用匿名方法，建立帶有參數的委派
            System.Threading.Thread Thread_Timer_Report_DB = new System.Threading.Thread
            (
               delegate (object value)
               {
                   int Interval = Convert.ToInt32(value);
                   timer_report_DB = new Timer(new TimerCallback(DB_TimerTask), null, 1000, Interval);
               }
            );
            Thread_Timer_Report_DB.Start(interval);
        }
        #endregion

        #region Time Thread Method 
        // Real Insert DB 的部分
        public void DB_TimerTask(object timerState)
        {
            try
            {
                foreach( var key in _dic_DB_Partaker.Keys)
                {
                    ConcurrentQueue<DBPartaker> _DBProcess;
                   
                    _dic_DB_Partaker.TryRemove(key, out _DBProcess);
                    string[] temp = key.Split('_');
                    string Provider = temp[0].ToString();
                    string ConnectionStr = temp[1].ToString();

                    if (_DBProcess.Count > 0)
                    {
                        DBPartaker DBP = null;

                        Stopwatch stopwatch = new Stopwatch();
                        stopwatch.Start();
                        int _DBProcess_Deep = _DBProcess.Count();

                        using (var db = new DBContext.IOT_DbContext(Provider, ConnectionStr))
                        {
                            while (_DBProcess.TryDequeue(out DBP))
                            {
                                DBContext.IOT_DEVICE Device = null;
                                ConcurrentDictionary<string, int> Dict_EDC_Label = null;
                                string remove_key = string.Concat(DBP.serial_id, "_", DBP.gateway_id, "_", DBP.device_id);
                                if (_IOT_Device.TryGetValue(remove_key, out Device))
                                {
                                    if (_EDC_Label_Data.TryGetValue(remove_key, out Dict_EDC_Label))
                                    {
                                        DBContext.IOT_DEVICE_EDC oIoT_DeviceEDC = new DBContext.IOT_DEVICE_EDC();
                                        oIoT_DeviceEDC.device_id = DBP.device_id;
                                        string InsertDBInfo = string.Empty;
                                        int ReportIndex = 0;
                                        foreach (Tuple<string, string, string> items in DBP.Report_Item)
                                        {
                                            if (Dict_EDC_Label.TryGetValue(items.Item1, out ReportIndex))
                                            {
                                                string ReportValue = string.Empty;

                                                switch (items.Item3)
                                                {
                                                    case "ASC":
                                                        ReportValue = (items.Item2.Length > 16) ? items.Item2.Substring(16) : items.Item2;
                                                        break;

                                                    case "DATETIME":
                                                        DateTime parsedDate;
                                                        if (DateTime.TryParse(items.Item2, out parsedDate))
                                                        {
                                                            ReportValue = parsedDate.ToString("yyyyMMddHHmmss");
                                                        }
                                                        else
                                                        {
                                                            ReportValue = "999999";
                                                        }

                                                        break;
                                                    default:
                                                        ReportValue = (IsNumeric(items.Item2) == true) ? items.Item2 : "999999";
                                                        break;
                                                }
                                                string ReportPropertyName = string.Concat("data_value_", ReportIndex.ToString("00"));
                                                oIoT_DeviceEDC.SetPropertyValue(ReportPropertyName, ReportValue);
                                                InsertDBInfo = string.Concat(InsertDBInfo, "_", string.Format("ItemName:{0}, ItemValue:{1}, ItemPosi:{2}.", items.Item1, items.Item2, ReportIndex.ToString("00")));
                                            }
                                        }

                                        _logger.LogInformation(string.Format("DB Service Insert DB : {0}, Device : {1}. ", key, remove_key));
                                        _logger.LogTrace(string.Format("DB Insert Trace {0}. ", InsertDBInfo));

                                        oIoT_DeviceEDC.clm_date_time = DateTime.Now;
                                        oIoT_DeviceEDC.clm_user = "SYSADM";
                                        oIoT_DeviceEDC.AddDB(db, Device, oIoT_DeviceEDC);
                                        db.SaveChanges();
                                    }
                                }
                            }
                        }

                        stopwatch.Stop();
                        _logger.LogInformation(string.Format("Execute Update DB Key {0} DataCount : {1}, Spect Time {2} ms > " , key, _DBProcess_Deep, stopwatch.ElapsedMilliseconds));
                    }  
                }
            }
            catch (Exception ex)
            {
                string ErrorLog = string.Format("Insert_DB Faild ex :{0}. ", ex.Message);
                _logger.LogError(ErrorLog);
                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0001", ErrorLog, "ERROR");

            }
        }

        public void _First_Initial_DB_Information()
        {
            try
            {
                foreach (cls_DB_Info DB_info in _objectmanager.DBManager.dbconfig_list)
                {
                    _logger.LogInformation(string.Format("Initial_DB_Info IOT_DEVICE and EDC_LABEL Info, SerialNo = {0}, GatewayID = {1}, DeviceID = {2}", DB_info.serial_id, DB_info.gateway_id, DB_info.device_id));
                    string _IOT_Device_key = string.Empty;

                    // "MS SQL" / "My SQL"  ""MS SQL"", "server= localhost;database=IoTDB;user=root;password=qQ123456")
                    using (var db = new DBContext.IOT_DbContext(DB_info.db_type, DB_info.connection_string))
                    {
                        _logger.LogInformation(string.Format("Initial DB Context Object"));

                        _logger.LogInformation(string.Format("Download IOT Device Object start"));
                        // 建置更新 IOT Device Information 
                        var _IOT_Status = db.IOT_DEVICE.AsQueryable().Where(p => p.gateway_id == DB_info.gateway_id && p.device_id == DB_info.device_id).ToList();
                        foreach (DBContext.IOT_DEVICE Device in _IOT_Status)
                        {
                            _IOT_Device_key = string.Concat(DB_info.serial_id, "_", DB_info.gateway_id, "_", DB_info.device_id);
                            DBContext.IOT_DEVICE _IOT_Device_Value = (DBContext.IOT_DEVICE)Device.Clone();
                            this._IOT_Device.AddOrUpdate(_IOT_Device_key, _IOT_Device_Value, (key, oldvalue) => _IOT_Device_Value);
                        }

                        _logger.LogInformation(string.Format("Download IOT Device Object end"));

                        _logger.LogInformation(string.Format("Download IOT EDC Level Object start"));
                        // 建置更新 EDC Label to Local  (DB)
                        var vEDC_Label_Result = db.IOT_DEVICE_EDC_LABEL.AsQueryable().Where(o => o.device_id == DB_info.device_id).ToList();
                        _IOT_Device_key = string.Concat(DB_info.serial_id, "_", DB_info.gateway_id, "_", DB_info.device_id);
                        ConcurrentDictionary<string, int> _Sub_EDC_Labels = this._EDC_Label_Data.GetOrAdd(DB_info.device_id, new ConcurrentDictionary<string, int>());
                        foreach (DBContext.IOT_DEVICE_EDC_LABEL _EDC_Label_key in vEDC_Label_Result)
                        {
                            _Sub_EDC_Labels.AddOrUpdate(_EDC_Label_key.data_name, _EDC_Label_key.data_index, (key, oldvalue) => _EDC_Label_key.data_index);
                        }
                        this._EDC_Label_Data.AddOrUpdate(_IOT_Device_key, _Sub_EDC_Labels, (key, oldvalue) => _Sub_EDC_Labels);

                        _logger.LogInformation(string.Format("Download IOT EDC Level Object end"));

                        _logger.LogInformation(string.Format("Upload IOT EDC Level Object start"));

                        // 建置 CCS Config 設定資料
                        List<string> ConfigSetting_NormalTag = DB_info.tag_info.Select(o => o.Item1).ToList();
                        List<string> ConfigSetting_CalcTag = DB_info.calc_tag_info.Select(o => o.Item1).ToList();
                        List<string> ConfigSetting_Tag = ConfigSetting_NormalTag.Concat(ConfigSetting_CalcTag).ToList();

                        ConcurrentDictionary<string, int> _DB_EDC_Labels = this._EDC_Label_Data.GetOrAdd(DB_info.device_id, new ConcurrentDictionary<string, int>());
                        List<string> DBSetting_Tag = _DB_EDC_Labels.Keys.ToList();

                        List<string> Diff = ConfigSetting_Tag.Except(DBSetting_Tag).ToList();

                        if (Diff.Count > 0)
                        {
                            ConcurrentDictionary<string, int> _Sync_Device_EDC_Label = this._Sync_EDC_Label_Data.GetOrAdd(_IOT_Device_key, new ConcurrentDictionary<string, int>());
                            List<int> _lstIndex = _DB_EDC_Labels.Select(t => t.Value).ToList();

                            //---- 找出最大值 --
                            int _CurrentIndex = 0;
                            if (_lstIndex.Count() == 0)
                            {
                                _CurrentIndex = 0;
                            }
                            else
                            {
                                _CurrentIndex = _lstIndex.Max();
                            }

                            foreach (string New_EDC_items in Diff)
                            {
                                _CurrentIndex++;
                                _DB_EDC_Labels.AddOrUpdate(New_EDC_items, _CurrentIndex, (key, oldvalue) => _CurrentIndex);
                                _Sync_Device_EDC_Label.AddOrUpdate(New_EDC_items, _CurrentIndex, (key, oldvalue) => _CurrentIndex);
                            }
                            this._EDC_Label_Data.AddOrUpdate(_IOT_Device_key, _DB_EDC_Labels, (key, oldvalue) => _DB_EDC_Labels);
                            this._Sync_EDC_Label_Data.AddOrUpdate(_IOT_Device_key, _Sync_Device_EDC_Label, (key, oldvalue) => _Sync_Device_EDC_Label);
                        }

                        _logger.LogInformation(string.Format("Upload IOT EDC Level Object end"));

                    }

                }
            }

            catch (Exception ex)
            {
                string ErrorLog = "First_Initial_DB_Information, error :" + ex.Message;
                _logger.LogError(ErrorLog);
                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0002", ErrorLog, "ERROR");
            }

        }

        public void Routine_Update_DB_Information(object timerState)
        {

            if (_Initial_Finished == false)
                return;

            try
            {
                // 先upload 再 Download
                if (_Sync_EDC_Label_Data.Count != 0)
                {
                    foreach (var Key in _Sync_EDC_Label_Data.Keys)
                    {
                        ConcurrentDictionary<string, int> _Process;
                        _Sync_EDC_Label_Data.TryRemove(Key, out _Process);

                        _logger.LogInformation(string.Format("Upload_DB_Info EDC_LABEL Info, (SerialNo,GatewayID,DeviceID ) = ({0})", Key));

                        string[] temp = Key.Split('_');
                        string SerialID = temp[0].ToString();
                        string GatewayID = temp[1].ToString();
                        string DeviceID = temp[2].ToString();

                        cls_DB_Info DB_info = _objectmanager.DBManager.dbconfig_list.Where(p => p.serial_id == SerialID).FirstOrDefault();

                        if (DB_info == null)
                        {
                            _logger.LogWarning(string.Format("Upload_DB_Info Faild DB Serial {0} not Exist in DBManager)", SerialID));
                        }
                        else
                        {
                            using (var db = new DBContext.IOT_DbContext(DB_info.db_type, DB_info.connection_string))
                            {
                                string InsertEDCLabelInfo = string.Empty;
                                foreach (KeyValuePair<string, int> _EDC_item in _Process)
                                {
                                    DBContext.IOT_DEVICE_EDC_LABEL oIoT_Device_EDC_label = new DBContext.IOT_DEVICE_EDC_LABEL();
                                    oIoT_Device_EDC_label.device_id = DB_info.device_id;
                                    oIoT_Device_EDC_label.data_name = _EDC_item.Key;
                                    oIoT_Device_EDC_label.data_index = _EDC_item.Value;
                                    oIoT_Device_EDC_label.clm_date_time = DateTime.Now;
                                    oIoT_Device_EDC_label.clm_user = "system";
                                    db.Add(oIoT_Device_EDC_label);
                                    InsertEDCLabelInfo = string.Concat(InsertEDCLabelInfo, "_", string.Format("ItemName:{0}, ItemPosi:{1}.", _EDC_item.Key, _EDC_item.Value));

                                }
                                db.SaveChanges();
                                _logger.LogTrace(string.Format("DB Insert EDC Label Trace {0}. ", InsertEDCLabelInfo));
                            }
                        }
                    }
                }

                //-------- Get Device and EDC Label information

                foreach (cls_DB_Info DB_info in _objectmanager.DBManager.dbconfig_list)
                {
                    _logger.LogTrace(string.Format("Update_DB_Info IOT_DEVICE and EDC_LABEL Info, SerialNo = {0}, GatewayID = {1}, DeviceID = {2}", DB_info.serial_id, DB_info.gateway_id, DB_info.device_id));
                    string _IOT_Device_key = string.Empty;

                    // "MS SQL" / "My SQL"  ""MS SQL"", "server= localhost;database=IoTDB;user=root;password=qQ123456")

                    using (var db = new DBContext.IOT_DbContext(DB_info.db_type, DB_info.connection_string))
                    {
                        var _IOT_Status = db.IOT_DEVICE.AsQueryable().Where(p => p.gateway_id == DB_info.gateway_id && p.device_id == DB_info.device_id).ToList();
                        foreach (DBContext.IOT_DEVICE Device in _IOT_Status)
                        {
                            _IOT_Device_key = string.Concat(DB_info.serial_id, "_", DB_info.gateway_id, "_", DB_info.device_id);
                            DBContext.IOT_DEVICE _IOT_Device_Value = (DBContext.IOT_DEVICE)Device.Clone();
                            this._IOT_Device.AddOrUpdate(_IOT_Device_key, _IOT_Device_Value, (key, oldvalue) => _IOT_Device_Value);
                        }

                        var vEDC_Label_Result = db.IOT_DEVICE_EDC_LABEL.AsQueryable().Where(o=>o.device_id == DB_info.device_id).ToList();
                        _IOT_Device_key = string.Concat(DB_info.serial_id, "_", DB_info.gateway_id, "_", DB_info.device_id);
                        ConcurrentDictionary<string, int> _Sub_EDC_Labels = this._EDC_Label_Data.GetOrAdd(DB_info.device_id, new ConcurrentDictionary<string, int>());
                        foreach (DBContext.IOT_DEVICE_EDC_LABEL _EDC_Label_key in vEDC_Label_Result)
                        {
                           _Sub_EDC_Labels.AddOrUpdate(_EDC_Label_key.data_name, _EDC_Label_key.data_index, (key, oldvalue) => _EDC_Label_key.data_index);  
                        }
                        this._EDC_Label_Data.AddOrUpdate(_IOT_Device_key, _Sub_EDC_Labels, (key, oldvalue) => _Sub_EDC_Labels);
                    }
                }
            }

            catch (Exception ex)
            {
                string ErrorLog = "Routine_Update_DB_Information, error :" + ex.Message;
                _logger.LogError(ErrorLog);
                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0003", ErrorLog, "ERROR");
            }

        }
        #endregion

        #region Delegate Method

        public void Add_DBPartaker_to_dict( DBPartaker DBP )
        {
            if(_Initial_Finished == false)
               return;

            string Key = string.Concat(DBP.db_type, "_", DBP.connection_string);
            ConcurrentQueue<DBPartaker> _Current = this._dic_DB_Partaker.GetOrAdd(Key, new ConcurrentQueue<DBPartaker>());
            _Current.Enqueue(DBP);
            this._dic_DB_Partaker.AddOrUpdate(Key, _Current, (key, oldvalue) => _Current);  
        }

        public List<string > Get_EDC_Label_Data(string _Serial_ID, string _GateWayID, string _DeviceID)
        {
            string _Dictkey = string.Concat(_Serial_ID, "_", _GateWayID, "_", _DeviceID);
            ConcurrentDictionary<string, int> _Sub_EDC_Labels = this._EDC_Label_Data.GetOrAdd(_Dictkey, new ConcurrentDictionary<string, int>());

            //---判斷空的Label 馬上再去DB下載一次
            if(_Sub_EDC_Labels.Count() ==0)
            {
                cls_DB_Info DB_info = _objectmanager.DBManager.dbconfig_list.Where(o => o.serial_id == _Serial_ID).FirstOrDefault();
                if(DB_info !=null)
                {
                    using (var db = new DBContext.IOT_DbContext(DB_info.db_type, DB_info.connection_string))
                    {
                        var vEDC_Label_Result = db.IOT_DEVICE_EDC_LABEL.AsQueryable().Where(o=>o.device_id == _DeviceID).ToList();
                        _Sub_EDC_Labels = this._EDC_Label_Data.GetOrAdd(_Dictkey, new ConcurrentDictionary<string, int>());
                        foreach (DBContext.IOT_DEVICE_EDC_LABEL _EDC_Label_key in vEDC_Label_Result)
                        {
                            _Sub_EDC_Labels.AddOrUpdate(_EDC_Label_key.data_name, _EDC_Label_key.data_index, (key, oldvalue) => _EDC_Label_key.data_index);
                        }
                        this._EDC_Label_Data.AddOrUpdate(_Dictkey, _Sub_EDC_Labels, (key, oldvalue) => _Sub_EDC_Labels);

                    }
                }
            }

            _Sub_EDC_Labels = this._EDC_Label_Data.GetOrAdd(_Dictkey, new ConcurrentDictionary<string, int>());
            return _Sub_EDC_Labels.Keys.ToList();
        }



        #endregion


        public void ReceiveMQTTData(xmlMessage InputData)
        {
            // Parse Mqtt Topic
            string Topic = InputData.MQTTTopic;
            string Payload = InputData.MQTTPayload;
            _logger.LogDebug(string.Format("DB Service Handle ReceiveMQTTData Data: {0}. ", Payload));
            try
            {
                ProcDBData DBProc = new ProcDBData(Payload,  Add_DBPartaker_to_dict);
                if (DBProc != null)
                {
                    ThreadPool.QueueUserWorkItem(o => DBProc.ThreadPool_Proc());
                }
            }
            catch (Exception ex)
            {
                string ErrorLog = string.Format("DB Service Handle ReceiveMQTTData Exception Msg : {0}. ", ex.Message);
                _logger.LogError(ErrorLog);
                ReportAlarm(_dic_Basicsetting[_GatewayID], _dic_Basicsetting[_DeviceID], "0004", ErrorLog, "ERROR");

            }

        }

        public void Receive_Work_Recv_Event(xmlMessage InputData)
        {
            
            _logger.LogInformation(string.Format("Receive ReadData Event Topic : {0}, Msg : {1}. ", InputData.MQTTTopic, InputData.MQTTPayload));
            _logger.LogInformation("First Load DB Info Start");
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            _First_Initial_DB_Information();
            stopwatch.Stop();
            _logger.LogInformation("First Load DB Info Finished Spect Time " + stopwatch.ElapsedMilliseconds + " ms");
            _Initial_Finished = true;


        }

        #region MISC
        public bool IsNumeric(object Expression)
        {
           
            bool isNum;
           
            double retNum;

            isNum = Double.TryParse(Convert.ToString(Expression), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out retNum);

            return isNum;
        }
        #endregion

        public void ReportAlarm(string gateway_id, string device_id, string al_code, string al_desc, string al_level)
        {
            xmlMessage SendOutMsg = new xmlMessage();
            SendOutMsg.GatewayID = gateway_id;     // GateID
            SendOutMsg.DeviceID = device_id;   // DeviceID
            SendOutMsg.MQTTTopic = "IOT_Alarm";

            SendOutMsg.MQTTPayload = JsonConvert.SerializeObject(new
            {
                AlarmCode = al_code,
                Datetime = DateTime.Now.ToString("yyyyMMddHHmmssfff"),
                AlarmLevel = al_level,
                AlarmApp = "IOTCLIENT",
                AlarmDesc = al_desc

            }, Formatting.Indented);
            _QueueManager.PutMessage(SendOutMsg);
        }



        private void Load_Xml_Config_To_Dict(string config_path)
        {
            XElement SettingFromFile = XElement.Load(config_path);
            XElement System_Setting = SettingFromFile.Element("system");

            _dic_Basicsetting.Clear();

            if (System_Setting != null)
            {
                foreach (var el in System_Setting.Elements())
                {
                    _dic_Basicsetting.Add(el.Name.LocalName, el.Value);
                }
            }
        }



    }

    public class ProcDBData
    {
        DBPartaker objDB = null;
        public DBService.Add_DBPartaker_to_dict_Event _Add_DBPartakertoDict;
        public ProcDBData(string inputdata,  DBService.Add_DBPartaker_to_dict_Event Add_DBPartakertoDict)
        {
            try
            {
                this.objDB = JsonConvert.DeserializeObject<DBPartaker>(inputdata.ToString());
                _Add_DBPartakertoDict = Add_DBPartakertoDict;
            }
            catch
            {
                this.objDB = null;
            }

        }
        public void ThreadPool_Proc()
        {
            _Add_DBPartakertoDict(this.objDB);
        }
    }
}
