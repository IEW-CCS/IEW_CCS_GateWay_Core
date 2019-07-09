using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ObjectManager
{

    public class DBPartaker
    {
        public string serial_id { get; set; }
        public string gateway_id { get; set; }
        public string device_id { get; set; }
        public string db_type { get; set; }
        public bool enable { get; set; }
        public string connection_string { get; set; }
        public List<Tuple<string, string, string>> Report_Item = new List<Tuple<string,string, string>>();

        public DBPartaker()
        {

        }
        public DBPartaker(cls_DB_Info DB)
        {
            this.serial_id = DB.serial_id;
            this.gateway_id = DB.gateway_id;
            this.device_id = DB.device_id;
            this.enable = DB.enable;
            this.db_type = DB.db_type;
            this.connection_string = DB.connection_string;
       
        }
        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }

    public class DBManager
    {
        public int serial_id_index;
        public List<cls_DB_Info> dbconfig_list = new List<cls_DB_Info>();

        public object Clone()
        {
            return this.MemberwiseClone();
        }
    }

    public class cls_DB_Info
    {
        public string serial_id { get; set; }
        public string gateway_id { get; set; }
        public string device_id { get; set; }
        public string db_type { get; set; }
        public string data_source { get; set; }
        public string port_id { get; set; }
        public string db_name { get; set; }
        public string user_name { get; set; }
        public string password { get; set; }
        public string provider_name { get; set; }
        public bool enable { get; set; }
        public string connection_string { get; set; }
        public List<Tuple<string, string>> tag_info = new List<Tuple<string, string>>();
        public List<Tuple<string, string>> calc_tag_info = new List<Tuple<string, string>>();
    }

    public static class EncryptionHelper
    {
        public static string Encrypt(string clearText)
        {
            string EncryptionKey = "IEW-CCS";
            byte[] clearBytes = Encoding.Unicode.GetBytes(clearText);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateEncryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(clearBytes, 0, clearBytes.Length);
                        cs.Close();
                    }
                    clearText = Convert.ToBase64String(ms.ToArray());
                }
            }
            return clearText;
        }

        public static string Decrypt(string cipherText)
        {
            string EncryptionKey = "IEW-CCS";
            cipherText = cipherText.Replace(" ", "+");
            byte[] cipherBytes = Convert.FromBase64String(cipherText);
            using (Aes encryptor = Aes.Create())
            {
                Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(EncryptionKey, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                encryptor.Key = pdb.GetBytes(32);
                encryptor.IV = pdb.GetBytes(16);
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor.CreateDecryptor(), CryptoStreamMode.Write))
                    {
                        cs.Write(cipherBytes, 0, cipherBytes.Length);
                        cs.Close();
                    }
                    cipherText = Encoding.Unicode.GetString(ms.ToArray());
                }
            }
            return cipherText;
        }
    }
}
