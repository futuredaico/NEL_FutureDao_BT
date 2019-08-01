using log4net;
using log4net.Config;
using log4net.Repository;
using System;
using System.IO;
using System.Threading;

namespace NEL.NNS.lib
{
    class LogHelper
    {
        private static string repositoryName;
        private static ILog log;
        public static void initLogger(string config)
        {
            ILoggerRepository repository = LogManager.CreateRepository("LogHelper");
            XmlConfigurator.Configure(repository, new FileInfo(config));
            repositoryName = repository.Name;
            log = getLogger(typeof(LogHelper));
        }

        public static ILog getLogger(Type t)
        {
            return LogManager.GetLogger(repositoryName, t);
        }

        public static void debug(string fmt, object[] args = null)
        {
            if (args == null)
            {
                log.Debug(fmt);
                return;
            }
            log.DebugFormat(fmt, args);
        }
        public static void warn(string fmt, object[] args = null)
        {
            if (args == null)
            {
                log.Warn(fmt);
                return;
            }
            log.WarnFormat(fmt, args);
        }
        public static void error(string fmt, object[] args = null)
        {
            if (args == null)
            {
                log.Error(fmt);
                return;
            }
            log.ErrorFormat(fmt, args);
        }

        public static void initThread(string name)
        {
            Thread.CurrentThread.Name = name + "_" + Thread.CurrentThread.ManagedThreadId;
        }

        private static string now => DateTime.Now.ToString("u");
        public static void printLog(string fmt)
        {
            Console.WriteLine(string.Format("{0} {1}", now, fmt));
        }

        public static void printEx(Exception ex, bool flag = false)
        {
            string threadName = Thread.CurrentThread.Name;
            Console.WriteLine(threadName + " failed, errMsg:" + ex.Message);
            Console.WriteLine(ex.GetType());
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
            Console.WriteLine();
            warn("{0} failed, errMsg:{1}\n{2}\n{3}\n", new object[] { threadName, ex.Message, ex.GetType(), ex.StackTrace });
            if (flag)
            {
                error(threadName + " exit");
            }
            else
            {
                warn(threadName + " continue");
            }
        }
    }
}
