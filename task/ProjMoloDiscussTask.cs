using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjMoloDiscussTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo lConn;
        private string moloProjCounter = "molocounters";
        private string moloProjInfoCol = "moloprojinfos";
        private string moloProjDiscussInfoCol = "moloprojdiscussinfos";
        private string moloProjDiscussZanInfoCol = "moloprojdiscusszaninfos";
        private string moloProposalInfoCol = "moloproposalinfos";
        private string moloProposalDiscussInfoCol = "moloproposaldiscussinfos";
        private string moloProposalDiscussZanInfoCol = "moloproposaldiscusszaninfos";

        public ProjMoloDiscussTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            lConn = Config.daoDbConnInfo;
        }

        public override void process()
        {
            Log(2000);
            processCount(handleProjDiscussCount);
            processCount(handleProjDiscussZanCount);
            processCount(handleProposalDiscussCount);
            processCount(handleProposalDiscussZanCount);
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

        // moloProjInfoCol.discussCount
        private void handleProjDiscussCount()
        {
            var key = "moloProjDiscussCount";
            var rColl = moloProjDiscussInfoCol;
            var lColl = moloProjInfoCol;
            handleProjDiscuss(key, rColl, lColl);
        }
        private void handleProjDiscuss(string key, string rColl, string lColl)
        {
            long lt = GetLTime(key);
            if (!GetMaxTmAndProjIds(rColl, lt, out long rt, out string[] ids, true)) return;
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("projId");
                findJo.Add("lastUpdateTime", new JObject { { "$lte", rt } });
                findJo.Add("preDiscussId", "");
                GetCountAndUpdateByProjId(findJo, rColl, lColl, "discussCount");
            }
            //
            UpdateLTime(key, rt);
        }
        private long GetLTime(string key)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjCounter, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastUpdateTime"].ToString());
        }
        private void UpdateLTime(string key, long time)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", key }, { "lastUpdateTime", time } };
                mh.PutData(lConn.connStr, lConn.connDB, moloProjCounter, newdata);
                return;
            }
            string updateStr = new JObject { { "$set", new JObject { { "lastUpdateTime", time } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjCounter, updateStr, findStr);
        }
        private bool GetMaxTmAndProjIds(string coll, long lt, out long rt, out string[] ids, bool hasPreId = false)
        {
            rt = 0;
            ids = null;
            var findJo = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } };
            if (hasPreId) findJo.Add("preDiscussId", "");
            string findStr = findJo.ToString();
            string fieldStr = "{'projId':1,'lastUpdateTime':1}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, coll, findStr, fieldStr);
            if (queryRes.Count == 0) return false;
            rt = queryRes.Max(p => long.Parse(p["lastUpdateTime"].ToString()));
            ids = queryRes.Select(p => p["projId"].ToString()).Distinct().ToArray();
            return true;
        }
        private void GetCountAndUpdateByProjId(JObject findJo, string rColl, string lColl, string rKey)
        {
            var updateDict = new Dictionary<string, JObject>();
            var list = new List<string>();
            list.Add(new JObject { { "$match", findJo } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, rColl, list);
            if (queryRes.Count > 0)
            {
                updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { rKey, long.Parse(v["sum"].ToString()) } });
            }
            if (updateDict.Count > 0)
            {
                var updateJa = updateDict.ToDictionary(
                    k => new JObject { { "projId", k.Key } }.ToString(),
                    v => new JObject { { "$set", v.Value } }.ToString()
                    );
                foreach (var item in updateJa)
                {
                    mh.UpdateData(lConn.connStr, lConn.connDB, lColl, item.Value, item.Key);
                }
            }
        }


        // moloProjDiscussZanInfoCol.zanCount
        private void handleProjDiscussZanCount()
        {
            string key = "moloProjDiscussZanCount";
            string rColl = moloProjDiscussZanInfoCol;
            string lColl = moloProjDiscussInfoCol;
            handleDiscussZanCount(key, rColl, lColl);
        }
        private void handleDiscussZanCount(string key, string rColl, string lColl)
        {
            long lt = GetLTime(key);
            if (!GetMaxTmAndDiscussIds(rColl, lt, out long rt, out string[] ids)) return;
            //
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("discussId");
                findJo.Add("time", new JObject { { "$lte", rt } });

                GetCountAndUpdateByDiscussId(findJo, rColl, lColl);
            }
            //
            UpdateLTime(key, rt);
        }
        private bool GetMaxTmAndDiscussIds(string coll, long lt, out long rt, out string[] ids)
        {
            rt = 0;
            ids = null;
            string findStr = new JObject { { "time", new JObject { { "$gt", lt } } } }.ToString();
            string fieldStr = "{'discussId':1,'time':1}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, coll, findStr, fieldStr);
            if (queryRes.Count == 0) return false;
            rt = queryRes.Max(p => long.Parse(p["time"].ToString()));
            ids = queryRes.Select(p => p["discussId"].ToString()).Distinct().ToArray();
            return true;
        }
        private void GetCountAndUpdateByDiscussId(JObject findJo, string rColl, string lColl)
        {
            var updateDict = new Dictionary<string, JObject>();
            var list = new List<string>();
            list.Add(new JObject { { "$match", findJo } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$discussId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, rColl, list);
            if (queryRes.Count > 0)
            {
                updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { "zanCount", long.Parse(v["sum"].ToString()) } });
            }
            if (updateDict.Count > 0)
            {
                var updateJa = updateDict.ToDictionary(
                    k => new JObject { { "discussId", k.Key } }.ToString(),
                    v => new JObject { { "$set", v.Value } }.ToString()
                    );
                foreach (var item in updateJa)
                {
                    mh.UpdateData(lConn.connStr, lConn.connDB, lColl, item.Value, item.Key);
                }
            }
        }


        // moloPropInfoCol.discussCount
        private void handleProposalDiscussCount()
        {
            string key = "moloProposalDiscussCount";
            string rColl = moloProposalDiscussInfoCol;
            string lColl = moloProposalInfoCol;
            handleProposalDiscussCount(key, rColl, lColl);
        }
        private void handleProposalDiscussCount(string key, string rColl, string lColl)
        {
            long lt = GetLTime(key);
            if (!GetMaxTmAndUpdateIds(rColl, lt, out long rt, out string[] ids, true)) return;
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("proposalId");
                findJo.Add("lastUpdateTime", new JObject { { "$lte", rt } });
                GetCountAndUpdateByProposalId(findJo, rColl, lColl, "discussCount");
            }
            //
            UpdateLTime(key, rt);
        }
        private bool GetMaxTmAndUpdateIds(string coll, long lt, out long rt, out string[] ids, bool hasPreId = false)
        {
            rt = 0;
            ids = null;
            var findJo = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } };
            if (hasPreId) findJo.Add("preDiscussId", "");
            string findStr = findJo.ToString();
            string fieldStr = "{'proposalId':1,'lastUpdateTime':1}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, coll, findStr, fieldStr);
            if (queryRes.Count == 0) return false;
            rt = queryRes.Max(p => long.Parse(p["lastUpdateTime"].ToString()));
            ids = queryRes.Select(p => p["proposalId"].ToString()).Distinct().ToArray();
            return true;
        }
        private void GetCountAndUpdateByProposalId(JObject findJo, string rColl, string lColl, string rKey)
        {
            var updateDict = new Dictionary<string, JObject>();
            var list = new List<string>();
            list.Add(new JObject { { "$match", findJo } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$proposalId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, rColl, list);
            if (queryRes.Count > 0)
            {
                updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { rKey, long.Parse(v["sum"].ToString()) } });
            }
            if (updateDict.Count > 0)
            {
                var updateJa = updateDict.ToDictionary(
                    k => new JObject { { "proposalId", k.Key } }.ToString(),
                    v => new JObject { { "$set", v.Value } }.ToString()
                    );
                foreach (var item in updateJa)
                {
                    mh.UpdateData(lConn.connStr, lConn.connDB, lColl, item.Value, item.Key);
                }
            }
        }


        // moloPropDiscussInfoCol.zanCount
        private void handleProposalDiscussZanCount()
        {
            string key = "moloProposalDiscussZanCount";
            var rColl = moloProposalDiscussZanInfoCol;
            var lColl = moloProposalDiscussInfoCol;
            handleProposalDiscussZan(key, rColl, lColl);
        }
        private void handleProposalDiscussZan(string key, string rColl, string lColl)
        {
            long lt = GetLTime(key);
            if (!GetMaxTmAndDiscussIds(rColl, lt, out long rt, out string[] ids)) return;
            //
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("discussId");
                findJo.Add("time", new JObject { { "$lte", rt } });

                GetCountAndUpdateByDiscussId(findJo, rColl, lColl);
            }
            //
            UpdateLTime(key, rt);
        }
    }
}
