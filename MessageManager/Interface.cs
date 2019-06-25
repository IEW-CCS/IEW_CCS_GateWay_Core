using System;
using System.Collections.Generic;
using System.Text;

namespace Kernel.Interface
{
    public interface IService
    {
        void Init();
        string ServiceName
        {
            get;
        }
    }

    public interface IManagement
    {
        void Init();
        string ManageName
        {
            get;
        }
        object GetInstance
        {
            get;
        }
    }





}
