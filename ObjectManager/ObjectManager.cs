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
        public MonitorManager MonitorManager = null;


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
            string _config_path = @"C:\\Gateway\\Config\\Gateway_Device_Config.json";
            if ( !LoadGatewayConfig(_config_path))
            {
                _logger.LogError("Config File not exist, Path = " + _config_path);

            }

            string _config_EDC_path = @"C:\\Gateway\\Config\\EDC_Xml_Config.json";
            if (!LoadEDCXmlConfig(_config_EDC_path))
            {
                _logger.LogError("EDC File not exist, Path = " + _config_path);

            }

        }


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



        #region Gateway Constructor
        public void GatewayManager_Initial()
        {
            this.GatewayManager = new GateWayManager();
        }

        public void GatewayManager_Initial(string Json)
        {
            this.GatewayManager = JsonConvert.DeserializeObject<GateWayManager>(Json);
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

        #region EDCManager Constructor
        public void EDCManager_Initial()
        {
            this.EDCManager = new EDCManager();
        }

        public void EDCManager_Initial(string Json)
        {
            this.EDCManager = JsonConvert.DeserializeObject<EDCManager>(Json);
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


    }
}
