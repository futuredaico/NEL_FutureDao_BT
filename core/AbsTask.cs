using System;
using System.Threading;
using NEL.NNS.lib;
using Newtonsoft.Json.Linq;

namespace NEL_FutureDao_BT.core
{
    abstract class AbsTask : ITask, INetType
    {
        private string taskName;
        private string startNetType;
        protected bool hasInitSuccess { get; set; }
        protected bool hasCreateIndex { get; set; }
        private string now => DateTime.Now.ToString("u");

        public AbsTask(string name)
        {
            taskName = name;
        }

        public string name()
        {
            return taskName;
        }

        public void Init(JObject config)
        {
            startNetType = config["startNetType"].ToString();
            try
            {
                initConfig(config);
                hasInitSuccess = true;
                LogHelper.debug("InitTask success:" + name() + "_" + networkType());
            }
            catch (Exception ex)
            {
                LogHelper.warn("InitTask failed:" + name() + "_" + networkType() + ",exMsg:" + ex.Message + ",exStack:" + ex.StackTrace);
            }
        }
        public abstract void initConfig(JObject config);

        public void Start()
        {
            LogHelper.initThread(taskName);
            try
            {
                startTask();
            }
            catch (Exception ex)
            {
                LogHelper.printEx(ex);
            }
        }
        public void startTask() {
            if (!isRuning()) return;
            while(true)
            {
                try
                {
                    process();
                } catch(Exception ex)
                {
                    LogHelper.printEx(ex);
                    Thread.Sleep(10 * 1000);
                }
            }
        }
        public abstract void process();

        public void Sleep(int batchInterval)
        {
            Thread.Sleep(batchInterval);
        }
        public void Log(int batchInterval)
        {
            LogHelper.printLog(string.Format("{0} {1} running...", now, name()));
            Sleep(batchInterval);
        }
        public void Log(long lh, long rh, int batchInterval=0)
        {
            LogHelper.printLog(string.Format("{0} {1} processed: {2}/{3}", now, name(), lh, rh));
            if (batchInterval > 0) Sleep(batchInterval);
        }

        public bool isRuning() => hasInitSuccess;
        public string networkType()
        {
            return startNetType;
        }
    }
}
