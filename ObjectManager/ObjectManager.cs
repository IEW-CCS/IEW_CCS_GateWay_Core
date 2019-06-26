using System;
using System.IO;
using System.Reflection;
using Newtonsoft.Json;
using System.Linq;
using Kernel.Interface;
using Kernel.Common;
using Kernel.MQTTManager;
using Kernel.QueueManager;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;


namespace ObjectManager
{


    public class ObjectManager :IManagement
    {
        private string _ManageName = "ObjectManager";

        private readonly ILogger<ObjectManager> _logger;
        public ObjectManager(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ObjectManager>();
           
        }

        public GateWayManager GatewayManager = null;
        public TagSetManager TagSetManager = null;
        public EDCManager EDCManager = null;
        public DBManager DBManager = null;
        public MonitorManager MonitorManager = null;

        #region DI Defined Method of interface
        public string ManageName
        {
            get
            {
                return this._ManageName;
            }
           
        }

        public object GetInstance
        {
            get
            {
                return this;
            }

        }

        public void Init()
        {
            GatewayManager_Initial();
            EDCManager_Initial();

        }

        #endregion

        #region Load Local Config
        private bool LoadGatewayConfig(string config_path)
        {
            try
            {
                if (!System.IO.File.Exists(config_path))
                {
                    GatewayManager_Initial();
                    return true;
                }

                StreamReader inputFile = new StreamReader(config_path);
                string json_string = inputFile.ReadToEnd();
                GatewayManager_Initial(json_string);

                if (GatewayManager.gateway_list == null)
                {
                    _logger.LogWarning("No gateway exists!");
                    return false;
                }

                inputFile.Close();
            }
            catch
            {
                _logger.LogError("Gateway config file loading error!");
                return false;
            }

            return true;
        }
        private bool LoadEDCXmlConfig(string config_EDC_path)
        {
            try
            {
                if (!System.IO.File.Exists(config_EDC_path))
                {
                  
                    EDCManager_Initial();
                    return true;
                }

                StreamReader inputFile = new StreamReader(config_EDC_path);

                string json_string = inputFile.ReadToEnd();

                EDCManager_Initial(json_string);

                if (EDCManager.gateway_edc == null)
                {
                   _logger.LogWarning("No EDC XML config exists!");
                    return false;
                }

                inputFile.Close();
            }
            catch
            {
                _logger.LogError("EDC XML config file loading error!!");
                return false;
            }

            return true;
        }
        #endregion


        #region Gateway Constructor / Method
        public void GatewayManager_Initial()
        {
            this.GatewayManager = new GateWayManager();
        }

        public void GatewayManager_Initial(string Json)
        {
            this.GatewayManager = JsonConvert.DeserializeObject<GateWayManager>(Json);
        }

        public void GatewayManager_Config(string gatewayid, string deviceid, string Json)
        {
            if (this.GatewayManager == null)
            {
                GatewayManager_Initial();
            }

            cls_Gateway_Info tmp_gw_info = JsonConvert.DeserializeObject<cls_Gateway_Info>(Json);
            var tmp_gateway = this.GatewayManager.gateway_list.Where(o => o.gateway_id.Equals(tmp_gw_info.gateway_id)).FirstOrDefault();
            if(tmp_gateway != null)
            {
                foreach(cls_Device_Info tmp_device in tmp_gw_info.device_info)
                {
                    var exist = tmp_gateway.device_info.Where(o => o.device_name.Equals(tmp_device.device_name)).FirstOrDefault();
                    if(exist == null)
                    {
                        tmp_gateway.device_info.Add(tmp_device);
                    }
                    else
                    {
                        _logger.LogWarning(string.Format("gateway {0} device {1} is Exist, So Skip Update GatewayManagement", gatewayid, deviceid));
                    }
                  
                }

            }
            else
            {
                this.GatewayManager.gateway_list.Add(tmp_gw_info);
            }

        }

       
        #endregion

        #region Tagset Constructor
        public void TagSetManager_Initial()
        {
            this.TagSetManager = new TagSetManager();
        }

        public void TagSetManager_Initial(string Json)
        {
            this.TagSetManager = JsonConvert.DeserializeObject<TagSetManager>(Json);
        }
        #endregion

        #region EDCManager Constructor / Method
        public void EDCManager_Initial()
        {
            this.EDCManager = new EDCManager();
        }

        public void EDCManager_Initial(string Json)
        {
            this.EDCManager = JsonConvert.DeserializeObject<EDCManager>(Json);
        }

        public void EDCManager_Config(string gatewayid, string deviceid, string Json)
        {
            if (this.EDCManager == null)
            {
                EDCManager_Initial();
            }

            List<cls_EDC_Info> edc_list = JsonConvert.DeserializeObject<List<cls_EDC_Info>>(Json);
            if (edc_list != null)
            {
                foreach (cls_EDC_Info edcitem in edc_list)
                {

                    var exist = this.EDCManager.gateway_edc.Where(o => o.serial_id.Equals(edcitem.serial_id)).FirstOrDefault();
                    if (exist == null)
                    {
                        this.EDCManager.gateway_edc.Add(edcitem);

                    }
                }
            }
        }

        #endregion

        #region MonitorManager Constructor

        public void MonitorManager_Initial()
        {
            this.MonitorManager = new MonitorManager();
        }

        public void MonitorManager_Initial(string Json)
        {
            this.MonitorManager = JsonConvert.DeserializeObject<MonitorManager>(Json);
        }

        #endregion

        #region Gateway Method
        public void GatewayManager_Set_TagValue(cls_ProcRecv_CollectData ProcRecv_CollectData)
        {
            string _GateWayID = ProcRecv_CollectData.GateWayID;
            string _DeviceID = ProcRecv_CollectData.Device_ID;

            cls_Gateway_Info Gateway = GatewayManager.gateway_list.Where(p => p.gateway_id == _GateWayID).FirstOrDefault();
            if (Gateway != null)
            {
                cls_Device_Info Device = Gateway.device_info.Where(p => p.device_name == _DeviceID).FirstOrDefault();
                if (Device != null)
                {
                    Tuple<string, string> _tag = null;
                    while (ProcRecv_CollectData.Prod_EDC_Data.TryDequeue(out _tag))
                    {
                        if (Device.tag_info.ContainsKey(_tag.Item1))
                        {
                            cls_Tag tag = Device.tag_info[_tag.Item1];
                            tag.Value = _tag.Item2;
                            Device.tag_info.AddOrUpdate(_tag.Item1, tag, (key, oldvalue) => tag);
                        }
                    }
                }
                else
                {
                 //   NLogManager.Logger.LogError("Service", GetType().Name, MethodInfo.GetCurrentMethod().Name, string.Format("Device {0} Not Exist, So Skip Update Tag Value", _DeviceID));
                }
            }
            else
            {
             //   NLogManager.Logger.LogError("Service", GetType().Name, MethodInfo.GetCurrentMethod().Name, string.Format("Gateway {0} Not Exist, So Skip Update Tag Value", _GateWayID));

            }


        }
        public string GatewayManager_ToJson_String()
        {

            try
            {
                return JsonConvert.SerializeObject(GatewayManager, Newtonsoft.Json.Formatting.Indented);
            }
            catch
            {
                return null;
            }
        }

        public string TagSetManager_ToJson_String()
        {

            try
            {
                return JsonConvert.SerializeObject(TagSetManager, Newtonsoft.Json.Formatting.Indented);
            }
            catch
            {
                return null;
            }
        }

        public string MonitorManager_ToJson_String()
        {
            try
            {
                return JsonConvert.SerializeObject(MonitorManager, Newtonsoft.Json.Formatting.Indented);
            }
            catch
            {
                return null;
            }
        }

        public string GatewayCommand_Json(string _Cmd_Type, string _Report_Interval, string _Trace_ID, string _GateWayID, string _DeviceID)
        {
            cls_Gateway_Info Gateway = GatewayManager.gateway_list.Where(p => p.gateway_id == _GateWayID).FirstOrDefault();
            if (Gateway == null)
            {
                return null;
            }
            else
            {
                cls_Device_Info Device = Gateway.device_info.Where(p => p.device_name == _DeviceID).FirstOrDefault();
                if (Device == null)
                {
                    return null;
                }
                else
                {

                    cls_Collect collect_cmd = new cls_Collect(_Cmd_Type, _Report_Interval, _Trace_ID, Device);
                    return JsonConvert.SerializeObject(collect_cmd, Newtonsoft.Json.Formatting.Indented);
                }
            }
        }
        #endregion

        #region DBManager Constructor
        public void DBManager_Initial()
        {
            this.DBManager = new DBManager();
        }

        public void DBManager_Initial(string Json)
        {
            this.DBManager = JsonConvert.DeserializeObject<DBManager>(Json);
        }

        public void DBManager_Config(string gatewayid, string deviceid, string Json)
        {
            if (this.DBManager == null)
            {
                DBManager_Initial();
            }

            cls_DB_Info db_info = JsonConvert.DeserializeObject<cls_DB_Info>(Json);
            if (db_info != null)
            {
                this.DBManager.dbconfig_list.Add(db_info);
            }
        }

        #endregion

    }
}
