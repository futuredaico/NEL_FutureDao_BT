using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.task;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NEL_FutureDao_BT
{
    class Program
    {
        private static void InitTask()
        {/*
            AddTask(new EmailVerifyTask("EmailVerifyTask"));
            AddTask(new ProjCountTask("ProjCountTask"));
            //
            AddTask(new ProjContractTask("ProjContractTask"));
            AddTask(new ProjFinanceTask("ProjFinanceTask"));
            AddTask(new ProjFinancePriceTask("ProjFinancePriceTask"));
            AddTask(new ProjFinanceBalanceTask("ProjFinanceBalanceTask"));
            //AddTask(new ProjProposalTask("ProjProposalTask"));
            AddTask(new ProjOrderTask("ProjOrderTask"));
            */
            AddTask(new ProjMoloContractTaskNewV2Stream("ProjMoloContractTaskNewV2Stream"));
            AddTask(new ProjMoloProposalTaskNewV2Stream("ProjMoloProposalTaskNewV2Stream"));
            AddTask(new ProjMoloDiscussTask("ProjMoloDiscussTask"));
            //
            AddTask(new ProjFutureCountTask("ProjFutureCountTask"));

        }
        private static void StartTask()
        {
            foreach (var func in list)
            {
                func.Init(Config.getConfig());
            }
            foreach (var func in list)
            {
                //Task.Run(func.Start);
                Task.Run(() => func.Start());
            }
        }

        private static List<ITask> list = new List<ITask>();
        private static void AddTask(ITask task) => list.Add(task);


        private static string appName = "FutureDao_BT";
        private static void head()
        {
            
            string[] info = new string[] {
                "*** Start to run "+appName,
                "*** Auth:tsc",
                "*** Version:0.0.0.1",
                "*** CreateDate:2019-07-22",
                "*** LastModify:2019-07-23"
            };
            foreach (string ss in info)
            {
                LogHelper.debug(ss);
            }
        }
        private static void tail()
        {
            LogHelper.debug(string.Format("{0} started, TaskCount:{1}/{2}/{3}", appName, 
                list.Count(p => p.isRuning()),
                list.Count(p => !p.isRuning()),
                list.Count));
        }
        static void Main(string[] args)
        {
            Config.loadConfig("config.json");
            head();
            InitTask();
            StartTask();
            tail();
            while (true)
            {
                System.Threading.Thread.Sleep(1000);
            }
        }
    }
}
