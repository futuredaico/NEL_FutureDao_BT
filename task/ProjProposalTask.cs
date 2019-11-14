using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using NEL_FutureDao_BT.task.dao;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjProposalTask: AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo daoConn;
        private string notifyCounter = "daonotifycounters";
        private string notifyCol = "daonotifyinfos";
        private string projProposalCol = "daoprojproposalinfos";
        private string projProposalVoteCol = "daoprojproposalvoteinfos";
        private long OneDaySeconds = 24 * 60 * 60;
        private long VotingSeconds;
        private long BullettiningSeconds;

        public ProjProposalTask(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            VotingSeconds = OneDaySeconds * 5;
            BullettiningSeconds = OneDaySeconds * 7;
            daoConn = Config.daoDbConnInfo;
            initIndex();
        }
        private void initIndex()
        {
            mh.setIndex(daoConn.connStr, daoConn.connDB, projProposalCol, "{'projId':1,'index':1,'state':1}", "i_projId_index_state");
        }


        public override void process()
        {
            Sleep(2000);
            handleProposal();
            handleVoteState();
        }
        // 提案
        private void handleProposal()
        {
            string key = "proposal";
            long lh = getL(key);
            long rh = getR();
            if(lh >= rh)
            {
                return;
            }
            for(var index=lh+1; index<=rh; ++index)
            {
                var findStr = new JObject { { "blockNumber", index} }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCol, findStr);
                if(queryRes.Count == 0)
                {
                    updateL(key, index);
                    log(key, index, rh);
                    continue;
                }
                // 
                foreach(var item in queryRes)
                {
                    handleProposal(item, index);
                }
                //
                var voteArr = queryRes.Where(p => p["event"].ToString() == "OnVote").ToArray();
                handleVoteCount(voteArr);
                //
                updateL(key, index);
                log(key, index, rh);
                tempNotClearAllFlag = false;
                try
                {
                    handleClearTp(voteArr);
                } catch 
                {
                    tempNotClearAllFlag = true;
                }

            }
        }
        private void handleProposal(JToken jt, long blockindex)
        {
            string projId = jt["projId"].ToString();
            string eventName = jt["event"].ToString();
            if (eventName == "OnApplyProposal")
            {
                var index = jt["index"].ToString();
                var findStr = new JObject { { "projId", projId }, { "index", index } }.ToString();
                if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, projProposalCol, findStr) == 0)
                {
                    var newdata = new JObject {
                        { "projId", projId },
                        { "index", index},
                        { "proposalName", jt["proposalName"] },
                        { "proposalType", ProposalType.ApplyFund },
                        { "proposalerAddress", jt["proposalerAddress"] },
                        { "proposalFund", jt["proposalFund"] },
                        { "recipientAdress", jt["recipientAdress"] },
                        { "timeConsuming", jt["timeConsuming"] },
                        { "proposalDetail", jt["proposalDetail"] },
                        { "proposalState", ProposalState.Voting },
                        { "blockindex", blockindex},
                        { "startTime", jt["startTime"] },
                        { "bulletinTime", long.Parse(jt["startTime"].ToString()) + 5 * OneDaySeconds },
                        { "voteProCount", 0 },
                        { "voteConCount", 0 }
                    }.ToString();
                    mh.PutData(daoConn.connStr, daoConn.connDB, projProposalCol, newdata);
                }
                return;
            }
            if (eventName == "OnVote")
            {
                // 投票
                handleVote(jt, projId);
                return;
            }
            if (eventName == "OnProcess")
            {
                var index = long.Parse(jt["index"].ToString());
                var pass = jt["pass"].ToString().ToLower() == "true";
                var proposalState = ProposalState.NoPass;
                if (pass)
                {
                    proposalState = ProposalState.Bullettining;
                }
                var findStr = new JObject { { "projId", projId }, { "index", index } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "proposalState", proposalState } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
                return;
            }
            if (eventName == "OnAbort")
            {
                var index = long.Parse(jt["index"].ToString());
                var findStr = new JObject { { "projId", projId }, { "index", index } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "proposalState", ProposalState.Cancel } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
                return;
            }
            if (eventName == "OnOneTicketRefuse")
            {
                var index = long.Parse(jt["index"].ToString());
                var findStr = new JObject { { "projId", projId }, { "index", index } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "proposalState", ProposalState.Stop } } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
                return;
            }
        }
        // 提案投票
        private void handleVote(JToken jt, string projId)
        {
            var index = long.Parse(jt["index"].ToString());
            var who = jt["who"].ToString();
            var findStr = new JObject { { "projId", projId }, { "index", index }, { "who", who } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, projProposalVoteCol, findStr) == 0)
            {
                var newdata = new JObject {
                        { "projId", projId},
                        { "index", index},
                        { "who", who},
                        { "voteResult", jt["voteResult"].ToString()},
                        { "shares", jt["shares"].ToString()},
                        { "time", jt["blockTime"].ToString()},
                    }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, projProposalVoteCol, newdata);
            }
        }
        // 提案投票数量
        private void handleVoteCount(JToken[] jtArr)
        {
            var res =
            jtArr.Where(p => p["eventName"].ToString() == "OnVote").GroupBy(p => p["projId"].ToString(), (k, g) => {
                var projId = k;
                return g.GroupBy(pg => pg["index"].ToString(), (kg, gg) => {
                    var index = long.Parse(kg);
                    var proCount = gg.Sum(pgg =>
                    {
                        if (pgg["voteResult"].ToString() == "1") return long.Parse(pgg["shares"].ToString());
                        return 0;
                    });
                    var conCount = gg.Sum(pgg =>
                    {
                        if (pgg["voteResult"].ToString() == "0") return long.Parse(pgg["shares"].ToString());
                        return 0;
                    });
                    return new
                    {
                        projId = projId,
                        index = index,
                        proCount = proCount,
                        conCount = conCount
                    };
                });

            }).SelectMany(p => p).ToList();

            foreach (var item in res)
            {
                var findStr = new JObject { { "projId", item.projId }, { "index", item.index } }.ToString();
                var fieldStr = new JObject { { "voteProCount", 1 }, { "voteConCount", 1 }, { "voteProCountTp", 1 }, { "voteConCountTp", 1 } }.ToString();
                var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projProposalCol, findStr, fieldStr);
                if (queryRes.Count == 0) throw new Exception("Not find proposal info at OnVote, projId:" + item.projId + ",index:" + item.index);

                var olddata = queryRes[0];
                var voteProCount = long.Parse(olddata["voteProCount"].ToString());
                var voteProCountTp = long.Parse(olddata["voteProCountTp"].ToString());
                var voteConCount = long.Parse(olddata["voteConCount"].ToString());
                var voteConCountTp = long.Parse(olddata["voteConCountTp"].ToString());
                if (tempNotClearAllFlag)
                {
                    voteProCountTp = 0;
                    voteConCountTp = 0;
                }
                voteProCount += item.proCount - voteProCountTp;
                voteProCountTp = item.proCount;
                voteConCount += item.conCount - voteConCountTp;
                voteConCountTp = item.conCount;
                var updateStr = new JObject {{"$set", new JObject{
                    {"voteProCount", voteProCount },
                    {"voteProCountTp", voteProCountTp },
                    {"voteConCount", voteConCount },
                    {"voteConCountTp", voteConCountTp },
                } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
            }
        }
        //
        private bool tempNotClearAllFlag = false;
        private void handleClearTp(JToken[] jtArr)
        {
            foreach(var jt in jtArr)
            {
                var projId = jt["projId"].ToString();
                var index = long.Parse(jt["index"].ToString());
                var findStr = new JObject { { "projId", projId }, { "index", index } }.ToString();
                var updateStr = new JObject { { "$set", new JObject {
                    { "voteProCountTp", 0},
                    { "voteConCountTp", 0}
                } } }.ToString();
                mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
            }
        }

        // 提案状态
        private void handleVoteState()
        {
            var now = TimeHelper.GetTimeStamp();
            var lastBlockTime = getLastBlockTime();
            if (now > lastBlockTime) now = lastBlockTime;
            // Voting --> Bullettining
            var findStr = new JObject { { "proposalState", ProposalState.Voting }, { "startTime", new JObject { { "$lte", now - VotingSeconds } } } }.ToString();
            var fieldStr = new JObject { { "projId", 1 }, { "index", 1 }, { "voteProCount", 1 }, { "voteConCount", 1 }, { "state", 1 } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projProposalCol, findStr, fieldStr);
            if (queryRes.Count > 0)
            {
                foreach (var item in queryRes)
                {
                    var state = ProposalState.Bullettining;
                    var zanY = long.Parse(item["voteProCount"].ToString());
                    var zanN = long.Parse(item["voteConCount"].ToString());
                    if (zanY < zanN) state = ProposalState.NoPass;
                    handleVoteState(item, state);
                }
            }
            // Bollettinning --> Executing
            findStr = new JObject { { "proposalState", ProposalState.Bullettining }, { "startTime", new JObject { { "$lte", now - VotingSeconds - BullettiningSeconds } } } }.ToString();
            queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, projProposalCol, findStr, fieldStr);
            if (queryRes.Count > 0)
            {
                foreach (var item in queryRes)
                {
                    handleVoteState(item, ProposalState.Executing);
                }
            }
        }
        private void handleVoteState(JToken jt, string state)
        {
            var findStr = new JObject { { "projId", jt["projId"] }, { "index", jt["index"] }, { "proposalState", jt["proposalState"] } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "proposalState", state } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, projProposalCol, updateStr, findStr);
        }
        private long getLastBlockTime()
        {
            var findStr = new JObject { { "counter", "logs" } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastBlockTime"].ToString());
        }

        // #######################
        private void log(string key, long lh, long rh)
        {
            Console.WriteLine("{0}.[{1}]processed: {2}/{3}", name(), key, lh, rh);
        }
        private void updateL(string key, long hh)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(daoConn.connStr, daoConn.connDB, notifyCounter, findStr) == 0)
            {
                string newdata = new JObject { { "counter", key }, { "lastBlockIndex", hh } }.ToString();
                mh.PutData(daoConn.connStr, daoConn.connDB, notifyCounter, newdata);
                return;
            }
            string updateStr = new JObject { { "$set", new JObject { { "lastBlockIndex", hh } } } }.ToString();
            mh.UpdateData(daoConn.connStr, daoConn.connDB, notifyCounter, updateStr, findStr);
        }
        private long getL(string key)
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
        private long getR(string key = "logs")
        {
            string findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(daoConn.connStr, daoConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return 0;
            return long.Parse(queryRes[0]["lastBlockIndex"].ToString());
        }
    }
}
