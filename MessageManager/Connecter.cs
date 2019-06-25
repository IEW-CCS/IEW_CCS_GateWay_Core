using System;
using System.Collections.Generic;
using System.Text;
using Kernel.Common;
using System.Reflection;
using System.Diagnostics;
using System.Threading;
using Kernel.Interface;
using Microsoft.Extensions.Logging;


namespace Kernel.MessageManager
{
    public class Connector
    {
     
        private string _methodName;

        private string _objectId;

        private bool _isInitRun;

        private MethodInfo _methodInfo;

        private IService _service;

        private object _syncObject = new object();



        private const long _limit_RunTime = (long)200;

        private bool _enabled;

        public bool IsEnabled
        {
            get
            {
                return this._enabled;
            }
        }

        public bool IsInitRun
        {
            get
            {
                return this._isInitRun;
            }
        }

        public string ObjectId
        {
            get
            {
                return this._objectId;
            }
            set
            {
                this._objectId = value;
            }
        }

        public string MethodName
        {
            get
            {
                return this._methodName;
            }
            set
            {
                this._methodName = value;
            }
        }

        public IService Service
        {
            get
            {
                return this._service;
            }
            set
            {
                this._service = value;
            }
        }

        private readonly ILogger _logger;

        //建構子
        public Connector( ILogger logger)  
        {
            
            _logger = logger;

        }

        public void Init(string objectId, string m)
        {

            this._objectId = objectId;
            this._methodName = m;
            this._isInitRun = true;
            this._methodInfo = null;
            this._enabled = true;

        }

        public void Disable()
        {
            this._enabled = false;
        }

        public void Enable()
        {
            this._enabled = true;
        }

        private void Exe_Reflection(object o)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            string method = string.Empty;
            int param_count = 0;
            try
            {
                try
                {
                    object[] objects = o as object[];
                    object[] param = objects[1] as object[];
                    MethodInfo mi = objects[0] as MethodInfo;
                    method = mi.Name;
                    param_count = (param != null ? (int)param.Length : 0);  //  Record Log for debug used.
                    mi.Invoke(this._service, param);
                }
                catch (Exception exception)
                {
                    Exception ex = exception;
                    _logger.LogError(string.Format("Method Name :'{0}' Paramater Count :'{1}'", method, param_count));
                    _logger.LogError(ex.Message);
                  }
            }
            finally
            {
                stopwatch.Stop();
                if (stopwatch.ElapsedMilliseconds >= _limit_RunTime)
                {
                    _logger.LogWarning(string.Format("Execute {0} Method Spect Time {1} ms > " + _limit_RunTime.ToString() + " ms. ", this._methodName, stopwatch.ElapsedMilliseconds));
                }
                else
                {
                    _logger.LogTrace(string.Format("Execute {0} Method Spect Time {1} ms.", this._methodName, stopwatch.ElapsedMilliseconds));
                   
                }
                
            }
        }

        //非同步處理Invoke
        public bool BeginInvoke(object[] param)
        {
            bool flag;
            if (!this._enabled)
            {
                return false;
            }
            if (this._service == null)
            {
                throw new Exception("Handler  is not create.");
            }
            lock (this._syncObject)
            {
                if (this._isInitRun)
                {
                    MethodInfo mi = this._service.GetType().GetMethod(this._methodName);
                    this._isInitRun = false;
                    if (mi == null)
                    {
                        throw new Exception(string.Format("Methed {0} is not exist in class {1}.", this._methodName, this._service.GetType().ToString()));
                    }
                    this._methodInfo = mi;
                }
                if (this._methodInfo == null)
                {
                    throw new Exception(string.Format("Methed {0} is not exist in class {1}.", this._methodName, this._service.GetType().ToString()));
                }
                WaitCallback waitCallback = new WaitCallback(this.Exe_Reflection);
                object[] objArray = new object[] { this._methodInfo, param };
                flag = ThreadPool.QueueUserWorkItem(waitCallback, objArray);
            }
            return flag;
        }



       
    }
}
