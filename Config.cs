using NEL.NNS.lib;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Linq;

namespace NEL_FutureDao_BT
{
    class Config
    {
        // 主配置文件
        private static JObject config;
        public static JObject getConfig()
        {
            return config;
        }
        public static void loadConfig(string filename)
        {
            LogHelper.initLogger("log4net.config");
            if (config == null)
            {
                config = JObject.Parse(File.ReadAllText(filename));
                initDb();
                initEmail();

            }
        }

        // DB连接信息
        public static DbConnInfo remoteDbConnInfo;
        public static DbConnInfo localDbConnInfo;
        public static DbConnInfo blockDbConnInfo;
        public static DbConnInfo analyDbConnInfo;
        public static DbConnInfo notifyDbConnInfo;
        public static DbConnInfo daoDbConnInfo;
        public static bool fromApiFlag;
        private static void initDb()
        {
            string startNetType = config["startNetType"].ToString();
            var connInfo = config["DBConnInfoList"].Children().Where(p => p["netType"].ToString() == startNetType).First();
            remoteDbConnInfo = GetDbConnInfo(connInfo, "remoteConnStr", "remoteDatabase");
            localDbConnInfo = GetDbConnInfo(connInfo, "localConnStr", "localDatabase");
            daoDbConnInfo = GetDbConnInfo(connInfo, "daoConnStr", "daoDatabase");
        }

        private static DbConnInfo GetDbConnInfo(JToken conn, string key1, string key2)
        {
            return new DbConnInfo
            {
                connStr = conn[key1].ToString(),
                connDB = conn[key2].ToString()
            };
        }

        public string getNetType()
        {
            return config["startNetType"].ToString();
        }

        // Address
        public static HashAddress hashAddressInfo;
        public static void initAddress()
        {
            var cfg = config["AddressConfig"];
            hashAddressInfo = new HashAddress
            {
                nnsSellingAddr = cfg["nnsSellingAddr"].ToString(),
                dexSellingAddr = cfg["dexSellingAddr"].ToString()
            };
        }

        // Email
        public static EmailConfig emailInfo;
        public static void initEmail()
        {
            var cfg = config["EmailConfig"];
            emailInfo = new EmailConfig
            {
                mailFrom = cfg["mailFrom"].ToString(),
                mailPwd = cfg["mailPwd"].ToString(),
                smtpHost = cfg["smtpHost"].ToString(),
                smtpPort = int.Parse(cfg["smtpPort"].ToString()),
                smtpEnableSsl = bool.Parse(cfg["smtpEnableSsl"].ToString()),
                subject = cfg["subject"].ToString(),
                body = cfg["body"].ToString(),
                listener = cfg["listener"].ToString(),
            };
        }
    }
    class DbConnInfo
    {
        public string connStr { set; get; }
        public string connDB { set; get; }
    }
    class HashAddress
    {
        public string nnsSellingAddr { get; set; }
        public string dexSellingAddr { get; set; }
    }
}
