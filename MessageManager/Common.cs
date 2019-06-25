using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kernel.Common
{


    public class xmlMessage
	{

        private string _fromService;
        private string _deviceId;
        private string _lineId;
        private DateTime _createTime;

        private string _mqtttopic;
        private string _name;
        private string _data;
        private string _trxid;
		private Dictionary<string, object> _useField;

        public string LineID
        {
            get
            {
                return this._lineId;
            }
            set
            {
                this._lineId = value;
            }
        }

        public string DeviceID
        {
            get
            {
                return this._deviceId;
            }
            set
            {
                this._deviceId = value;
            }
        }

        public string MQTTTopic
        {
            get
            {
                return this._mqtttopic;
            }
            set
            {
                this._mqtttopic = value;
            }
        }

        public string FromService
        {
            get
            {
                return this._fromService;
            }
            set
            {
                this._fromService = value;
            }


        }

        public DateTime CreateTime
		{
			get
			{
				return this._createTime;
			}
			set
			{
				this._createTime = value;
			}
		}

		public string Name
		{
			get
			{
				return this._name;
			}
			set
			{
				this._name = value;
			}
		}

        public string MQTTPayload
        {
            get
            {
                return this._data;
            }
            set
            {
                this._data = value;
            }
        }

        public Dictionary<string, object> UseField
		{
			get
			{
				return this._useField;
			}
			set
			{
				this._useField = value;
			}
		}

		public string TransactionID
		{
			get
			{
				return this._trxid;
			}
			set
			{
				this._trxid = value;
			}
		}

		public xmlMessage()
		{
            this._deviceId = string.Empty;
            this._createTime = DateTime.Now;
			this._useField = new Dictionary<string, object>();
		}

		public string MessageDetail()
		{
			return string.Format("Message name={0},createTime={1}.", this.Name, this.CreateTime);
		}
	}

}
