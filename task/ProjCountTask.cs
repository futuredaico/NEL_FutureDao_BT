using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjCountTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string daoCounterCol = "daoCounter";
        private string projInfoCol;
        private string projStarInfoCol;
        private string projUpdateInfoCol;
        private string projDiscussInfoCol;
        private int batchSize;
        private int batchInterval;
        public ProjCountTask(string name) : base(name) { }
        public override void initConfig(JObject config)
        {
            JToken cfg = config["TaskList"].Where(p => p["taskName"].ToString() == name() && p["taskNet"].ToString() == networkType()).ToArray()[0]["taskInfo"];
            projInfoCol = cfg["projInfoCol"].ToString();
            projStarInfoCol = cfg["projStarInfoCol"].ToString();
            projUpdateInfoCol = cfg["projUpdateInfoCol"].ToString();
            projDiscussInfoCol = cfg["projDiscussInfoCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            daoConn = Config.daoDbConnInfo;
        }

        public override void process()
        {
            Log(batchInterval);
            processCount(handleProjStarCount);
            processCount(handleProjUpdateCount);
            processCount(handleProjDiscussCount);
        }
        private void processCount(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        private void handleProjStarCount()
        {
            string key = "starCount";
            long lt = GetLTime(key);
            //
            string findStr = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } }.ToString();
            string sortStr = "{'lastUpdateTime':-1}";
            string fieldStr = "{'lastUpdateTime':1}";
            var queryRes = mh.GetDataPages(daoConn.connStr, daoConn.connDB, projStarInfoCol, findStr, sortStr, 0, 1, fieldStr);
            if (queryRes.Count == 0) return;
            long rt = long.Parse(queryRes[0]["lastUpdateTime"].ToString());
            //
            var updateDict = new Dictionary<string, JObject>();
            var list = new List<string>();
            list.Add(new JObject { { "$match", new JObject { { "starState", StarState .StarYes},{ "lastUpdateTime", new JObject { { "$gt", lt }, { "$lte", rt } } } } } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, projStarInfoCol, list);
            if (queryRes.Count > 0)
            {
                updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { "starCount", long.Parse(v["sum"].ToString())} });
            }
            //
            list = new List<string>();
            list.Add(new JObject { { "$match", new JObject { { "supportState", StarState.SupportYes }, { "lastUpdateTime", new JObject { { "$gt", lt }, { "$lte", rt } } } } } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, projStarInfoCol, list);
            if(queryRes.Count > 0)
            {
                foreach(var item in queryRes)
                {
                    var skey = item["_id"].ToString();
                    var sval = long.Parse(item["sum"].ToString());
                    if(updateDict.ContainsKey(skey))
                    {
                        updateDict.GetValueOrDefault(skey)["supportCount"] = sval;
                    } else
                    {
                        updateDict.Add(skey, new JObject { { "supportCount", sval} });
                    }
                }
            }
            //
            if(updateDict.Count > 0)
            {
                var updateJa = updateDict.ToDictionary(
                    k => new JObject { { "projId", k.Key} }.ToString(),
                    v => new JObject { { "$set", v.Value} }.ToString()
                    );
                foreach(var item in updateJa)
                {
                    mh.UpdateData(daoConn.connStr, daoConn.connDB, projInfoCol, item.Value, item.Key);
                }
                //
                UpdateLTime(key, rt);
            }
        }
        private void handleProjUpdateCount()
        {
            //
        }
        private void handleProjDiscussCount()
        {
            //
        }

        private void UpdateLTime(string key, long time)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, daoCounterCol, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key }, { "lastUpdateTime", time } };
                mh.PutData(daoConn.connStr, daoConn.connDB, daoCounterCol, newdata);
                return;
            }
            string updateStr = new JObject { { "$set", new JObject { { key, time } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, daoCounterCol, updateStr, findStr);
        }
        private long GetLTime(string key)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, daoCounterCol, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastUpdateTime"].ToString());
        }

    }
    class StarState
    {
        public const string StarYes = "10131";
        public const string StarNot = "10132";
        public const string SupportYes = "10133";
        public const string SupportNot = "10134";
    }
}
