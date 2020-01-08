using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjMoloProposalTask : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo lConn;
        private string notifyCounter = "molonotifycounters";
        private string notifyCol = "molonotifyinfos";
        private string moloProjCounter = "molocounters";
        private string moloProjProposalInfoCol = "moloproposalinfos";
        private string moloProjBalanceInfoCol = "moloprojbalanceinfos";
        private string projHashCol = "moloprojhashinfos";
        private string projInfoCol = "moloprojinfos";
        private long votingPeriod = 17280 * 5 * 7;
        private long notingPeriod = 17280 * 5 * 7;
        //private long votingPeriod = 120 * 5; // tmp
        //private long notingPeriod = 120 * 5;
        private string netType = "";

        public ProjMoloProposalTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            lConn = Config.daoDbConnInfo;
            netType = Config.getNetType();
            //
            if(netType == "testnet")
            {
                votingPeriod = 120 * 5; // tmp
                notingPeriod = 120 * 5;
            }
            initIndex();
        }

        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, projInfoCol, "{'projId':1}", "i_projId");
            mh.setIndex(lConn.connStr, lConn.connDB, projHashCol, "{'projId':1,'type':1}", "i_projId_type");
            mh.setIndex(lConn.connStr, lConn.connDB, projHashCol, "{'contractHash':1}", "i_contractHash");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'projId':1,'proposalIndex':1}", "i_projId_proposalIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'proposalId':1}", "i_proposalId");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'proposalState':1,'blockTime':1}", "i_proposalState_blockTime");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, "{'projId':1,'proposalIndex':1,'address':1}", "i_projId_proposalIndex_address");
        }

        public override void process()
        {
            Sleep(2000);
            //
            var counterDict = getAllCounter();
            if (counterDict != null && counterDict.Count > 0)
            {
                foreach (var item in counterDict)
                {
                    handlePropsoal(item.Key, item.Value);
                }
            }
            //
            handleProposalState();
        }
        private void handlePropsoal(string projId, long lh)
        {
            var rh = getRh(projId, out long rt);
            if(lh >= rh)
            {
                log("moloProposalTask", lh, rh);
                return;
            }
            //foreach (var hh in cc.OrderBy(p => p).ToArray())
            //{
            //    var index = hh;
            for (var index = lh + 1; index <= rh; ++index)
            {
                var findStr = new JObject { { "blockNumber", index }, { "projId", projId } }.ToString();
                var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCol, findStr);
                if(queryRes.Count == 0)
                {
                    updateL(projId, index, rt);
                    log("moloProposalTask", index, rh);
                    continue;
                }
                var time = long.Parse(queryRes[0]["blockTime"].ToString());
                // 0.委托
                var r0 =
                queryRes.Where(p => p["event"].ToString() == "UpdateDelegateKey").ToArray();
                handleDelegateKey(r0, projId);
                // 1.项目时间
                var r1 = 
                queryRes.Where(p => p["event"].ToString() == "SummonComplete").ToArray();
                handleSummonComplete(r1, projId);

                // 2.提案
                var r2 = 
                queryRes.Where(p => p["event"].ToString() == "SubmitProposal").ToArray();
                handleSubmitProposal(r2, projId);
                
                // 3.提案-投票数(在此之前的余额)
                var r3 =
                queryRes.Where(p => p["event"].ToString() == "SubmitVote").ToArray();
                handleSubmitVote(r3, projId, index);
                // 4.余额
                var r4 =
                queryRes.ToArray();//.Where(p => p["event"].ToString() == "ProcessProposal").ToArray();
                handleBalance(r4, projId, index);

                // 5.票数统计
                var r5=
                queryRes.Where(p => p["event"].ToString() == "SubmitVote").ToArray();
                handleSubmitVoteCount(r5, projId);

                // 6.提案状态和提案处理结果
                var r6 =
                queryRes.Where(p => p["event"].ToString() == "ProcessProposal").ToArray();
                handleProcessProposalResult(r6, projId);
                r6 =
                queryRes.Where(p => p["event"].ToString() == "Abort").ToArray();
                hanldeAbort(r6, projId);
                //
                updateL(projId, index, time);
                log("moloProposalTask", index, rh);
                //
                clearTempRes(queryRes, projId);
            }
        }

        //
        private void handleSummonComplete(JToken[] jtArr, string projId)
        {
            foreach (var item in jtArr)
            {
                var findStr = new JObject { { "projId", projId } }.ToString();
                var updateStr = new JObject { { "$set",
                        new JObject { { "startTime", item["blockTime"] } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, projInfoCol, updateStr, findStr);
            }
        }
        private void handleDelegateKey(JToken[] jtArr, string projId)
        {
            foreach (var item in jtArr)
            {
                var findStr = new JObject { { "projId", projId }, { "address", item["memberAddress"] } }.ToString();
                if(mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr) == 0)
                {
                    var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", ""},
                        { "address", item["memberAddress"]},
                        { "balance", 0},
                        { "balanceTp", 0},
                        { "type", BalanceType.Balance},
                        { "blockNumber", 0},
                        { "time", 0},
                        { "newDelegateKey",""}
                    }.ToString();
                    mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                    continue;
                }
                var updateStr = new JObject { { "$set",
                        new JObject { { "newDelegateKey", item["newDelegateKey"] } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
            }
        }
        

        // 提案信息
        private void getProposalName(string contractHash, string proposalIndex, out string proposalName, out string proposalDetail)
        {
            proposalName = "";
            proposalDetail = "";
            var res = "";
            try
            {
                res = EthHelper.ethCall(contractHash, proposalIndex, netType);
            }
            catch { }
            if (res.Length == 0) return;

            try
            {
                var resJo = JObject.Parse(res);
                proposalName = resJo["title"].ToString();
                proposalDetail = resJo["description"].ToString();
                return;
            }
            catch { }

            proposalName = res;
            proposalDetail = res;
        } 
        private void handleSubmitProposal(JToken[] jtArr, string projId)
        {
            foreach(var jt in jtArr)
            {
                var proposalIndex = jt["proposalIndex"].ToString();
                var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
                if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0)
                {
                    getProposalName(jt["contractHash"].ToString(), proposalIndex, out string proposalName, out string proposalDetail);
                    var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", proposalIndex},
                        { "proposalId", projId+proposalIndex},
                        { "proposalName", proposalName},
                        { "proposalDetail", proposalDetail},
                        { "sharesRequested", long.Parse(jt["sharesRequested"].ToString())},
                        { "tokenTribute", jt["tokenTribute"].ToString()},
                        { "proposalState", ProposalState.Voting},
                        { "handleState", ProposalHandleState.Not},
                        { "voteYesCount", 0},
                        { "voteNotCount", 0},
                        { "proposer", jt["memberAddress"]},
                        { "delegateKey", jt["delegateKey"]},
                        { "applicant", jt["applicant"]},
                        { "transactionHash", jt["transactionHash"]},
                        { "blockNumber", jt["blockNumber"] },
                        { "blockTime", jt["blockTime"] },
                        {"time", TimeHelper.GetTimeStamp() }
                    }.ToString();
                    mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, newdata);
                }
            }
        }


        // 提案-投票数
        private long getCurrentBalance(string projId, string address)
        {
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", "" }, { "address", address } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            return long.Parse(item["balance"].ToString());
        }
        private void handleSubmitVote(JToken[] jtArr, string projId, long blockNumber)
        {
            var balanceDict = jtArr.Select(p => p["memberAddress"].ToString()).Distinct().ToDictionary(k => k, v => getCurrentBalance(projId, v));
            var rr = jtArr.Select(p => {
                return new
                {
                    proposalIndex = p["proposalIndex"].ToString(),
                    address = p["memberAddress"].ToString(),
                    balance = balanceDict.GetValueOrDefault(p["memberAddress"].ToString()),
                    type = p["uintVote"]
                };
            }).ToArray();
            var now = TimeHelper.GetTimeStamp();
            foreach(var item in rr)
            {
                var findStr = new JObject { {"projId", projId },{ "proposalIndex", item.proposalIndex}, { "address", item.address} }.ToString();
                if(mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr) == 0)
                {
                    var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", item.proposalIndex},
                        { "address", item.address},
                        { "balance", item.balance},
                        { "balanceTp", 0},
                        { "type", item.type},
                        { "blockNumber", blockNumber },
                        { "time", now},
                    }.ToString();
                    mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                    continue;
                }
                var updateStr = new JObject {{"$set", new JObject{
                    { "balance", item.balance},
                    { "balanceTp", 0},
                    { "type", item.type},
                    { "blockNumber", blockNumber },
                    { "time", now},
                } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
            }
        }


        // 余额
        private void handleBalance(JToken[] jtArr, string projId, long blockNumber)
        {
            //
            var r0 = jtArr
                .Where(p => p["event"].ToString() == "SummonComplete")
                .GroupBy(p => p["summoner"].ToString(), (k, g) => {
                    return new JObject
                    {
                        { "address", k},
                        { "balance", g.Sum(pg => long.Parse(pg["shares"].ToString()))},
                        { "sig", 1}
                    };
                }).ToList();
            //
            var r1 = jtArr
                .Where(p => p["event"].ToString() == "ProcessProposal" && p["didPass"].ToString() == "1")
                .GroupBy(p => p["applicant"].ToString(), (k, g) => {
                    return new JObject
                    {
                        { "address", k},
                        { "balance", g.Sum(pg => long.Parse(pg["sharesRequested"].ToString()))},
                        { "sig", 1}
                    };
                }).ToList();
            //
            var r2 = jtArr
                .Where(p => p["event"].ToString() == "Ragequit")
                .GroupBy(p => p["memberAddress"].ToString(), (k,g) => {
                    return new JObject
                    {
                        { "address", k},
                        { "balance", g.Sum(pg => long.Parse(pg["sharesToBurn"].ToString()))},
                        { "sig", 0}
                    };
                }).ToList();
            //
            r0.AddRange(r1);
            r0.AddRange(r2);
            var rr = r0.GroupBy(p => p["address"].ToString(), (k, g) =>
            {
                return new
                {
                    address = k,
                    balance = g.Sum(pg =>
                    {
                        var balance = long.Parse(pg["balance"].ToString());
                        if ((int)pg["sig"] == 0)
                        {
                            balance *= -1;
                        }
                        return balance;
                    })
                };
            }).ToArray();
            var now = TimeHelper.GetTimeStamp();
            foreach (var item in rr)
            {
                var findStr = new JObject { { "projId", projId }, { "proposalIndex", "" }, { "address", item.address } }.ToString();
                var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
                if (queryRes.Count == 0)
                {
                    var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", ""},
                        { "address", item.address},
                        { "balance", item.balance},
                        { "balanceTp", item.balance},
                        { "type", BalanceType.Balance},
                        { "blockNumber", blockNumber},
                        { "time", now},
                        { "newDelegateKey",""}
                    }.ToString();
                    mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                    continue;
                }
                //
                var rItem = queryRes[0];
                var balance = long.Parse(rItem["balance"].ToString());
                var balanceTp = long.Parse(rItem["balanceTp"].ToString());
                if (tempNotClearAllFlag) balance = 0;
                balance += item.balance - balanceTp;
                balanceTp = item.balance;
                var updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "balanceTp", balanceTp},
                    { "blockNumber", blockNumber},
                    { "time", now}
                } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
            }
        }
        private void clearTempRes(JArray queryRes, string projId)
        {
            tempNotClearAllFlag = false;
            if (queryRes.All(p => p["memberAddress"] == null)) return;

            var rr = queryRes.Where(p => p["event"].ToString() == "SubmitVote").Select(p => new {
                address = p["memberAddress"].ToString(),
                proposalIndex = p["proposalIndex"].ToString()
            }).ToArray();

            var rb =
            queryRes.Select(p =>
            {
                var eventName = p["event"].ToString();
                if (eventName == "SummonComplete") return p["summoner"].ToString();
                if (eventName == "ProcessProposal") return p["applicant"].ToString();
                if (eventName == "Ragequit") return p["memberAddress"].ToString();
                return "";
            }).Where(p => p != "").Distinct().ToArray();
            // 清除临时字段数据
            tempNotClearAllFlag = false;
            var dbZERO = decimal.Zero.format();
            try
            {
                foreach(var item in rr)
                {
                    var findStr = new JObject { { "projId", projId }, { "proposalIndex", item.proposalIndex }, { "address", item.address } }.ToString();
                    var updateStr = new JObject { { "$set", new JObject { {"balanceTp",0 }} } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
                }
                foreach(var item in rb)
                {
                    var findStr = new JObject { { "projId", projId }, { "proposalIndex", "" }, { "address", item } }.ToString();
                    var updateStr = new JObject { { "$set", new JObject { { "balanceTp", 0 } } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
                }
            }
            catch (Exception ex)
            {
                // 
                Console.WriteLine(ex);
                tempNotClearAllFlag = true;
            }
        }
        private bool tempNotClearAllFlag = true;

        // 票数统计
        private Dictionary<string, long> getCurrentVoteCount(string projId, string proposalIndex)
        {
            var match = new JObject { { "$match", new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } } } }.ToString();
            var group = new JObject { { "$group", new JObject { { "_id", "$type" }, { "sum", new JObject { { "$sum", "$balance" } } } } } }.ToString();
            var list = new List<string> { match, group };
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, list);
            if (queryRes.Count == 0) return new Dictionary<string, long>();

            return queryRes.ToDictionary(k => k["_id"].ToString(), v=> long.Parse(v["sum"].ToString()));
        }
        private void handleSubmitVoteCount(JToken[] jtArr, string projId)
        {
            var rr =
            jtArr.Select(p => p["proposalIndex"].ToString()).Distinct().Select(p => new { proposalIndex = p, voteDict = getCurrentVoteCount(projId, p) }).ToArray();
            foreach(var item in rr)
            {
                var zanYesCount = item.voteDict.GetValueOrDefault(BalanceType.ZanYes);
                var zanNotCount = item.voteDict.GetValueOrDefault(BalanceType.ZanNot);
                var findStr = new JObject { { "projId", projId }, { "proposalIndex", item.proposalIndex} }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "voteYesCount", zanYesCount}, { "voteNotCount", zanNotCount} } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
            }
        }


        // 处理结果
        private string getProposalState(string didPass)
        {
            return didPass == "1" ? ProposalState.PassYes : ProposalState.PassNot;
        }
        private void handleProcessProposalResult(JToken[] jtArr, string projId)
        {
            foreach(var item in jtArr)
            {
                var didPass = getProposalState(item["didPass"].ToString());
                if(didPass == ProposalState.PassYes)
                {
                    // 受益人收到股份, 自动退回权限
                    resetDelegateKey(item["applicant"].ToString());
                }
                var findStr = new JObject { { "projId", projId }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "proposalState", getProposalState(item["didPass"].ToString()) }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
            }
        }
        private void resetDelegateKey(string delegateKey)
        {
            var findStr = new JObject { { "newDelegateKey", delegateKey } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr) == 0) return;
            //
            var updateStr = new JObject { { "$set", new JObject {
                { "newDelegateKey", ""}
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }
        private void hanldeAbort(JToken[] jtArr, string projId)
        {
            foreach(var item in jtArr)
            {
                var findStr = new JObject { { "projId", projId }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "proposalState", ProposalState.Aborted }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
            }
        }


        // 状态变动
        private void handleProposalState()
        {
            var now = TimeHelper.GetTimeStamp();
            // Voting -> Noting/PassNot
            var timeLimit = votingPeriod;
            var findStr = new JObject { { "proposalState", ProposalState.Voting},{ "blockTime", new JObject { { "$lt", now - timeLimit } } } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            var res = filterProjHasProccedBlockTime(queryRes, timeLimit);
            var updateStr = "";
            if(res != null && res.Count() >0)
            {
                foreach (var item in res)
                {
                    var yesCount = long.Parse(item["voteYesCount"].ToString());
                    var notCount = long.Parse(item["voteNotCount"].ToString());
                    var state = yesCount > notCount ? ProposalState.Noting : ProposalState.PassNot;
                    findStr = new JObject { { "projId", item["projId"] }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                    updateStr = new JObject { { "$set", new JObject { { "proposalState", state } } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
                }
            }
            
            // Noting -> PassYes
            timeLimit = votingPeriod + notingPeriod;
            findStr = new JObject { { "proposalState", ProposalState.Noting }, { "blockTime", new JObject { { "$lt", now - timeLimit } } } }.ToString();
            queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            res = filterProjHasProccedBlockTime(queryRes, timeLimit);
            updateStr = "";
            if (res != null && res.Count() > 0)
            {
                foreach (var item in queryRes)
                {
                    findStr = new JObject { { "projId", item["projId"] }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                    updateStr = new JObject { { "$set", new JObject { { "proposalState", ProposalState.PassYes } } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
                }
            }
        }

        private JToken[] filterProjHasProccedBlockTime(JArray queryRes, long timeLimit)
        {
            var projIdArr = queryRes.Select(p => p["projId"].ToString()).Distinct().ToArray();
            if (projIdArr.Count() == 0) return null;

            var projIdTimeDict = getProjLastUpdateTime(projIdArr);
            var res =
                queryRes.Where(p => {
                    var projId = p["projId"].ToString();
                    var blockTime = long.Parse(p["blockTime"].ToString());
                    if (!projIdTimeDict.ContainsKey(projId)) return false;
                    if (projIdTimeDict.GetValueOrDefault(projId) < timeLimit + blockTime) return false;
                    return true;
                }).ToArray();
            return res;
        }

        private Dictionary<string, long> getProjLastUpdateTime(string[] projIdArr)
        {
            var findStr = MongoFieldHelper.toFilter(projIdArr, "counter").ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjCounter, findStr);
            if (queryRes.Count == 0) return new Dictionary<string, long>();


            return queryRes.ToDictionary(k => k["counter"].ToString(), v => (long)v["lastUpdateTime"]);
        }
        

        private void log(string key, long lh, long rh)
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void updateL(string projId, long index, long time=0)
        {
            var findStr = new JObject { { "counter", projId } }.ToString();
            if(mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjCounter, findStr) == 0)
            {
                var newdata = new JObject { { "counter", projId }, { "lastBlockIndex", index },{ "lastUpdateTime", time} }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjCounter, newdata);
                return;
            }
            var updateJo = new JObject { { "lastBlockIndex", index } };
            if (time > 0) updateJo.Add("lastUpdateTime", time);
            var updateStr = new JObject { { "$set",  updateJo} }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjCounter, updateStr, findStr);
        }
        private long getRh(string projId, out long rt)
        {
            rt = 0;
            var findStr = new JObject { { "counter", projId} }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            rt = long.Parse(item["lastBlockTime"].ToString());
            return long.Parse(item["lastBlockIndex"].ToString());
        }
        private Dictionary<string, long> getAllCounter()
        {
            var match = new JObject { { "$match", new JObject { { "type", MoloType.Init } } } }.ToString();
            var lookup = new JObject { { "$lookup", new JObject {
                {"from", moloProjCounter},
                {"localField", "projId" },
                {"foreignField", "counter" },
                {"as", "cs" }
            } } }.ToString();
            var list = new List<string> { match, lookup };
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, projHashCol, list);
            if (queryRes.Count == 0) return null;

            var rr = 
            queryRes.GroupBy(p=> p["projId"].ToString(), (k,g)=> {
                return g.ToArray()[0];
            }).ToDictionary(k => k["projId"].ToString(), v => {
                var cs = (JArray)v["cs"];
                if (cs.Count == 0) return 0;
                return long.Parse(cs[0]["lastBlockIndex"].ToString());
            });
            return rr;
        }
    }
    class ProposalState
    {
        public const string PreVote = "10150";       // 预发布
        public const string Voting = "10151";       // 投票中
        public const string Noting = "10152";       // 公示中
        public const string PassYes = "10153";      // 已通过
        public const string PassNot = "10154";      // 未通过
        public const string Aborted = "10155";      // 已终止
        public const string HandleTimeOut = "10156";      // 处理超时
    }
    class ProposalHandleState
    {
        public const string Not = "0"; // 未处理
        public const string Yes = "1"; // 已处理
    }
    class BalanceType
    {
        public const string Balance = "0";
        public const string ZanYes = "1";
        public const string ZanNot = "2";
    }
}
