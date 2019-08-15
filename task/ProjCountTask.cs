using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
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
        private string projDiscussZanInfoCol;
        private string projUpdateDiscussInfoCol;
        private string projUpdateDiscussZanInfoCol;
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
            projDiscussZanInfoCol = cfg["projDiscussZanInfoCol"].ToString();
            projUpdateDiscussInfoCol = cfg["projUpdateDiscussInfoCol"].ToString();
            projUpdateDiscussZanInfoCol = cfg["projUpdateDiscussZanInfoCol"].ToString();
            batchSize = int.Parse(cfg["batchSize"].ToString());
            batchInterval = int.Parse(cfg["batchInterval"].ToString());
            daoConn = Config.daoDbConnInfo;
        }

        public override void process()
        {
            Log(batchInterval);
            // 关注/支持
            processCount(handleProjStarCount);
            // 
            processCount(handleProjUpdateCount);
            processCount(handleProjDiscussCount);
            processCount(handleUpdateDiscussCount);
            processCount(handleProjUpdateZanCount);
            processCount(handleProjDiscussZanCount);
            processCount(handleUpdateDiscussZanCount);

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
            // supprotCount + starCount + discussCount + updateCount
            string key = "projStarCount";
            long lt = GetLTime(key);
            //
            string findStr = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } }.ToString();
            string fieldStr = "{'projId':1,'lastUpdateTime':1}";
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projStarInfoCol, findStr, fieldStr);
            if (queryRes.Count == 0) return;
            long rt = queryRes.Max(p => long.Parse(p["lastUpdateTime"].ToString()));
            var ids = queryRes.Select(p => p["projId"].ToString()).Distinct().ToArray();
            //
            int size = 100;
            int cnt = ids.Length;
            for(int skip = 0; skip < cnt; skip+=size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("projId");
                findJo.Add("lastUpdateTime", rt);
                findJo.Add("starState", StarState.StarYes);

                var updateDict = new Dictionary<string, JObject>();
                var list = new List<string>();
                list.Add(new JObject { { "$match",  findJo } }.ToString());
                list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
                queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, projStarInfoCol, list);
                if (queryRes.Count > 0)
                {
                    updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { "starCount", long.Parse(v["sum"].ToString()) } });
                }

                findJo.Remove("starState");
                findJo.Add("supportState", StarState.SupportYes);
                list = new List<string>();
                list.Add(new JObject { { "$match", findJo } }.ToString());
                list.Add(new JObject { { "$group", new JObject { { "_id", "$projId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
                queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, projStarInfoCol, list);
                if (queryRes.Count > 0)
                {
                    foreach (var item in queryRes)
                    {
                        var skey = item["_id"].ToString();
                        var sval = long.Parse(item["sum"].ToString());
                        if (updateDict.ContainsKey(skey))
                        {
                            updateDict.GetValueOrDefault(skey)["supportCount"] = sval;
                        }
                        else
                        {
                            updateDict.Add(skey, new JObject { { "supportCount", sval } });
                        }
                    }
                }
                if (updateDict.Count > 0)
                {
                    var updateJa = updateDict.ToDictionary(
                        k => new JObject { { "projId", k.Key } }.ToString(),
                        v => new JObject { { "$set", v.Value } }.ToString()
                        );
                    foreach (var item in updateJa)
                    {
                        mh.UpdateData(daoConn.connStr, daoConn.connDB, projInfoCol, item.Value, item.Key);
                    }
                }
            }

            //
            UpdateLTime(key, rt);

        }
        private void handleProjUpdateCount()
        {
            //
            string key = "projUpdateCount";
            long lt = GetLTime(key);

            string rColl = projUpdateInfoCol;
            if (!GetMaxTmAndProjIds(rColl, lt, out long rt, out string[] ids)) return;
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("projId");
                findJo.Add("lastUpdateTime", new JObject { { "$lte", rt} });
                //
                GetCountAndUpdateByProjId(findJo, rColl, projInfoCol, "updateCount");
            }

            //
            UpdateLTime(key, rt);
        }

        private bool GetMaxTmAndProjIds(string coll, long lt, out long rt, out string[] ids, bool hasPreId = false)
        {
            rt = 0;
            ids = null;
            var findJo = new JObject { { "lastUpdateTime", new JObject { { "$gt", lt } } } };
            if (hasPreId) findJo.Add("preDiscussId", "");
            string findStr = findJo.ToString();
            string fieldStr = "{'projId':1,'lastUpdateTime':1}";
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, coll, findStr, fieldStr);
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
            var queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, rColl, list);
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
                    mh.UpdateData(daoConn.connStr, daoConn.connDB, lColl, item.Value, item.Key);
                }
            }
        }
        private void handleProjDiscussCount()
        {
            string key = "projDiscussCount";
            long lt = GetLTime(key);

            string rColl = projDiscussInfoCol;
            if (!GetMaxTmAndProjIds(rColl, lt, out long rt, out string[] ids, true)) return;
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("projId");
                findJo.Add("lastUpdateTime", new JObject { { "$lte", rt } });
                findJo.Add("preDiscussId", "");
                GetCountAndUpdateByProjId(findJo, rColl, projInfoCol, "discussCount");
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
            string fieldStr = "{'updateId':1,'lastUpdateTime':1}";
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, coll, findStr, fieldStr);
            if (queryRes.Count == 0) return false;
            rt = queryRes.Max(p => long.Parse(p["lastUpdateTime"].ToString()));
            ids = queryRes.Select(p => p["updateId"].ToString()).Distinct().ToArray();
            return true;
        }
        private void GetCountAndUpdateByUpdateId(JObject findJo, string rColl, string lColl, string rKey)
        {
            var updateDict = new Dictionary<string, JObject>();
            var list = new List<string>();
            list.Add(new JObject { { "$match", findJo } }.ToString());
            list.Add(new JObject { { "$group", new JObject { { "_id", "$updateId" }, { "sum", new JObject { { "$sum", 1 } } } } } }.ToString());
            var queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, rColl, list);
            if (queryRes.Count > 0)
            {
                updateDict = queryRes.ToDictionary(k => k["_id"].ToString(), v => new JObject { { rKey, long.Parse(v["sum"].ToString()) } });
            }
            if (updateDict.Count > 0)
            {
                var updateJa = updateDict.ToDictionary(
                    k => new JObject { { "updateId", k.Key } }.ToString(),
                    v => new JObject { { "$set", v.Value } }.ToString()
                    );
                foreach (var item in updateJa)
                {
                    mh.UpdateData(daoConn.connStr, daoConn.connDB, lColl, item.Value, item.Key);
                }
            }
        }
        private void handleUpdateDiscussCount()
        {
            string key = "projUpdateDiscussCount";
            long lt = GetLTime(key);

            string rColl = projUpdateDiscussInfoCol;
            if (!GetMaxTmAndUpdateIds(rColl, lt, out long rt, out string[] ids, true)) return;
            int size = 100;
            int cnt = ids.Length;
            for (int skip = 0; skip < cnt; skip += size)
            {
                var idArr = ids.Skip(skip).Take(size).ToArray();
                var findJo = idArr.toFilter("updateId");
                findJo.Add("lastUpdateTime", new JObject { { "$lte", rt } });
                GetCountAndUpdateByUpdateId(findJo, rColl, projUpdateInfoCol, "discussCount");
            }
            //
            UpdateLTime(key, rt);
        }


        private void handleProjUpdateZanCount()
        {

        }


        private bool GetMaxTmAndDiscussIds(string coll, long lt, out long rt, out string[] ids)
        {
            rt = 0;
            ids = null;
            string findStr = new JObject { { "time", new JObject { { "$gt", lt } } } }.ToString();
            string fieldStr = "{'discussId':1,'time':1}";
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, coll, findStr, fieldStr);
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
            var queryRes = mh.Aggregate(daoConn.connStr, daoConn.connDB, rColl, list);
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
                    mh.UpdateData(daoConn.connStr, daoConn.connDB, lColl, item.Value, item.Key);
                }
            }
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

        private void handleProjDiscussZanCount()
        {
            string key = "projDiscussZanCount";
            string rColl = projDiscussZanInfoCol;
            string lColl = projDiscussInfoCol;
            handleDiscussZanCount(key, rColl, lColl);
        }
        private void handleUpdateDiscussZanCount()
        {
            string key = "UpdateDiscussZanCount";
            string rColl = projUpdateDiscussZanInfoCol;
            string lColl = projUpdateDiscussInfoCol;
            handleDiscussZanCount(key, rColl, lColl);
        }
        

        // 
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
