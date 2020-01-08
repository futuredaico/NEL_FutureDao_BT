using NEL.NNS.lib;
using NEL_FutureDao_BT.core;
using NEL_FutureDao_BT.lib;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace NEL_FutureDao_BT.task
{
    class ProjMoloProposalTaskNewV2Stream : AbsTask
    {
        private MongoDBHelper mh = new MongoDBHelper();
        private DbConnInfo lConn;
        private string notifyCounter = "molonotifycounters";
        private string notifyCol = "molonotifyinfos";
        private string moloProjCounter = "molocounters";
        private string moloProjProposalInfoCol = "moloproposalinfos";
        private string moloProjProposalNameInfoCol = "moloproposalnameinfos";
        private string moloProjBalanceInfoCol = "moloprojbalanceinfos";
        private string projHashCol = "moloprojhashinfos";
        private string projInfoCol = "moloprojinfos";
        private long votingPeriod = 17280 * 5 * 7;
        private long notingPeriod = 17280 * 5 * 7;
        //private long votingPeriod = 120 * 5; // tmp
        //private long notingPeriod = 120 * 5;
        private string netType = Config.getNetType();

        public ProjMoloProposalTaskNewV2Stream(string name) : base(name) { }

        public override void initConfig(JObject config)
        {
            lConn = Config.daoDbConnInfo;
            //
            if (netType == "testnet")
            {
                votingPeriod = 120 * 5; // tmp
                notingPeriod = 120 * 5;
            }
            //addPrefix();
            initIndex();
        }

        private void addPrefix()
        {
            var prefix = "zbak31_";
            notifyCounter = prefix + notifyCounter;
            notifyCol = prefix + notifyCol;
            moloProjCounter = prefix + moloProjCounter;
            moloProjProposalInfoCol = prefix + moloProjProposalInfoCol;
            moloProjProposalNameInfoCol = prefix + moloProjProposalNameInfoCol;
            moloProjBalanceInfoCol = prefix + moloProjBalanceInfoCol;
            //projInfoCol = prefix + projInfoCol;
            //projHashCol = prefix + projHashCol;
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, projInfoCol, "{'projId':1}", "i_projId");
            mh.setIndex(lConn.connStr, lConn.connDB, projHashCol, "{'projId':1,'type':1}", "i_projId_type");
            mh.setIndex(lConn.connStr, lConn.connDB, projHashCol, "{'contractHash':1}", "i_contractHash");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'projId':1,'proposalIndex':1}", "i_projId_proposalIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'projId':1,'proposalQueueIndex':1}", "i_projId_proposalQueueIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'projId':1,'proposalState':1}", "i_projId_proposalState");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'proposalId':1}", "i_proposalId");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, "{'proposalState':1,'blockTime':1}", "i_proposalState_blockTime");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalNameInfoCol, "{'contractHash':1,'proposalIndex':1}", "i_contractHash_proposalIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, "{'projId':1,'proposalQueueIndex':1,'address':1}", "i_projId_proposalQueueIndex_address");
        }

        public override void process()
        {
            Sleep(2000);
            processNew();
            handleProposalState();
        }

        private void processNew()
        {
            var rh = getRh(out long rc, out long rt);
            var lh = getLh(out long lc, out long lt);
            if (lh >= rh) return;

            //
            var batchSize = 500;
            var findStr = new JObject { { "counter", new JObject { { "$gt", lc } } } }.ToString();
            var sortStr = "{'counter':1}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCol, findStr, sortStr, 0, batchSize);
            if (queryRes.Count == 0)
            {
                updateL(0, rh, rt);
                log(lc, rh, rh);
                return;
            }
            //
            var res = queryRes.OrderBy(p => long.Parse(p["counter"].ToString()));
            foreach (var item in res)
            {
                var counter = long.Parse(item["counter"].ToString());
                var blockNumber = long.Parse(item["blockNumber"].ToString());
                var blockTime = long.Parse(item["blockTime"].ToString());
                var topics = item["topics"].ToString();
                if (topics == Topic.SummonComplete.hash) handleSummonComplete(item);
                if (topics == Topic.UpdateDelegateKey.hash) handleUpdateDelegateKey(item);
                if (topics == Topic.SubmitProposal.hash) handleSubmitProposal(item);
                if (topics == Topic.SubmitVote.hash) handleSubmitVote(item);
                if (topics == Topic.ProcessProposal.hash) handleProcessProposal(item);
                if (topics == Topic.Ragequit.hash) handleRagequit(item);
                if (topics == Topic.Abort.hash) handleAbort(item);
                if (topics == Topic.SubmitProposal_v2.hash) handleSubmitProposalV2(item);
                if (topics == Topic.SponsorProposal_v2.hash) handleSponsorProposalV2(item);
                if (topics == Topic.ProcessProposal_v2.hash) handleProcessProposalV2(item);
                if (topics == Topic.CancelProposal_v2.hash) handleCancelProposalV2(item);
                if (topics == Topic.Ragequit_v2.hash) handleRagequitV2(item);

                updateL(counter, blockNumber, blockTime);
                log(counter, blockNumber, blockTime);
                if (topics == Topic.SummonComplete.hash
                    || topics == Topic.SubmitVote.hash
                    || topics == Topic.ProcessProposal.hash
                    || topics == Topic.Ragequit.hash
                    || topics == Topic.SubmitProposal_v2.hash
                    || topics == Topic.ProcessProposal_v2.hash
                    || topics == Topic.Ragequit_v2.hash
                    )
                {
                    clearTempRes(item["projId"].ToString());
                }
            }
        }

        private void handleSummonComplete(JToken jt)
        {
            // 创建时间 + 余额
            var projId = jt["projId"].ToString();
            var summoner = jt["summoner"].ToString();
            var shares = long.Parse(jt["shares"].ToString());

            // 创建时间
            var findStr = new JObject { { "projId", projId } }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                { "startTime", jt["blockTime"]
                } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, projInfoCol, updateStr, findStr);

            // 余额
            var now = TimeHelper.GetTimeStamp();
            findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", summoner } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalQueueIndex", ""},
                        { "type", BalanceType.Balance},
                        { "address", summoner},
                        { "balance", shares},
                        { "sharesBalance", shares},
                        { "sharesBalanceTp", 0},
                        { "lootBalance", 0},
                        { "lootBalanceTp", 0},
                        { "newDelegateKey",""},
                        { "time", now},
                        { "lastUpdateTime", now}
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                return;
            }

            var rItem = queryRes[0];
            var sharesBalance = long.Parse(rItem["sharesBalance"].ToString());
            var sharesBalanceTp = long.Parse(rItem["sharesBalanceTp"].ToString());
            var lootBalance = long.Parse(rItem["lootBalance"].ToString());
            var lootBalanceTp = long.Parse(rItem["lootBalanceTp"].ToString());

            if (tempNotClearAllFlag)
            {
                sharesBalanceTp = 0;
                lootBalanceTp = 0;
            }
            sharesBalance += shares - sharesBalanceTp;
            sharesBalanceTp += shares;
            lootBalance += 0 - lootBalanceTp;
            lootBalanceTp += 0;
            var balance = sharesBalance + lootBalance;
            updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "sharesBalance", sharesBalance},
                    { "sharesBalanceTp", sharesBalanceTp},
                    { "lootBalance", lootBalance},
                    { "lootBalanceTp", lootBalanceTp},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }
        private void handleUpdateDelegateKey(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var memberAddress = jt["memberAddress"].ToString();
            var newDelegateKey = jt["newDelegateKey"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", memberAddress } }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                { "newDelegateKey", newDelegateKey}
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }
        private void handleSubmitProposal(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0)
            {
                getProposalName(jt["contractHash"].ToString(), proposalIndex, out string proposalName, out string proposalDetail);
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", proposalIndex},
                        { "proposalQueueIndex", proposalIndex},
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
                        { "contractHash", jt["contractHash"]},
                        { "blockNumber", jt["blockNumber"] },
                        { "blockTime", jt["blockTime"] },
                        {"time", TimeHelper.GetTimeStamp() }
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, newdata);
            }
        }
        private void handleSubmitVote(JToken jt)
        {
            // 个人投票数 + 提案赞成反对票数
            //
            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"].ToString();
            var memberAddress = jt["memberAddress"].ToString();
            var uintVote = jt["uintVote"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex }, { "address", memberAddress } }.ToString();
            var now = TimeHelper.GetTimeStamp();
            var balance = getCurrentBalance(projId, memberAddress);
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr) == 0)
            {
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalQueueIndex", proposalQueueIndex},
                        { "type", uintVote},
                        { "address", memberAddress},
                        { "balance", balance},
                        { "balanceTp", 0},
                        { "time", now},
                        { "lastUpdateTime", now}
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                return;
            }
            var updateStr = new JObject {{"$set", new JObject{
                    { "type", uintVote},
                    { "balance", balance},
                    { "balanceTp",balance},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
            //
            var rr = getCurrentVoteCount(projId, proposalQueueIndex);
            var zanYesCount = rr.GetValueOrDefault(BalanceType.ZanYes);
            var zanNotCount = rr.GetValueOrDefault(BalanceType.ZanNot);
            findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            updateStr = new JObject { { "$set", new JObject { { "voteYesCount", zanYesCount }, { "voteNotCount", zanNotCount } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }
        private void handleProcessProposal(JToken jt)
        {
            // 提案 + 余额
            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"].ToString();
            var applicant = jt["applicant"].ToString();
            var sharesRequested = long.Parse(jt["sharesRequested"].ToString());
            var didPass = getProposalState(jt["didPass"].ToString());
            if (didPass == ProposalState.PassYes)
            {
                // 受益人收到股份, 自动退回权限
                resetDelegateKey(applicant);
            }
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "proposalState", getProposalState(didPass) }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);

            //
            findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", applicant } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            var now = TimeHelper.GetTimeStamp();
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                    { "projId", projId},
                    { "proposalQueueIndex", ""},
                    { "type", BalanceType.Balance},
                    { "address", applicant},
                    { "balance", sharesRequested},
                    { "sharesBalance", sharesRequested},
                    { "sharesBalanceTp", sharesRequested},
                    { "lootBalance", 0},
                    { "lootBalanceTp", 0},
                    { "newDelegateKey",""},
                    { "time", now},
                    { "lastUpdateTime", now}
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                return;
            }
            var rItem = queryRes[0];
            var sharesBalance = long.Parse(rItem["sharesBalance"].ToString());
            var sharesBalanceTp = long.Parse(rItem["sharesBalanceTp"].ToString());
            var lootBalance = long.Parse(rItem["lootBalance"].ToString());
            var lootBalanceTp = long.Parse(rItem["lootBalanceTp"].ToString());

            if (tempNotClearAllFlag)
            {
                sharesBalanceTp = 0;
                lootBalanceTp = 0;
            }
            sharesBalance += sharesRequested - sharesBalanceTp;
            sharesBalanceTp += sharesRequested;
            lootBalance += 0 - lootBalanceTp;
            lootBalanceTp += 0;
            var balance = sharesBalance + lootBalance;
            updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "sharesBalance", sharesBalance},
                    { "sharesBalanceTp", sharesBalanceTp},
                    { "lootBalance", lootBalance},
                    { "lootBalanceTp", lootBalanceTp},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);

        }
        private void handleAbort(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var propoalQueueIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", propoalQueueIndex } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "proposalState", ProposalState.Aborted }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }
        private void handleRagequit(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var memberAddress = jt["memberAddress"].ToString();
            var sharesToBurn = long.Parse(jt["sharesToBurn"].ToString());
            sharesToBurn *= -1;
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", memberAddress } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            var now = TimeHelper.GetTimeStamp();
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                    { "projId", projId},
                    { "proposalQueueIndex", ""},
                    { "type", BalanceType.Balance},
                    { "address", memberAddress},
                    { "balance", sharesToBurn},
                    { "sharesBalance", sharesToBurn},
                    { "sharesBalanceTp", sharesToBurn},
                    { "lootBalance", 0},
                    { "lootBalanceTp", 0},
                    { "newDelegateKey",""},
                    { "time", now},
                    { "lastUpdateTime", now}
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                return;
            }
            var rItem = queryRes[0];
            var sharesBalance = long.Parse(rItem["sharesBalance"].ToString());
            var sharesBalanceTp = long.Parse(rItem["sharesBalanceTp"].ToString());
            var lootBalance = long.Parse(rItem["lootBalance"].ToString());
            var lootBalanceTp = long.Parse(rItem["lootBalanceTp"].ToString());

            if (tempNotClearAllFlag)
            {
                sharesBalanceTp = 0;
                lootBalanceTp = 0;
            }
            sharesBalance += sharesToBurn - sharesBalanceTp;
            sharesBalanceTp += sharesToBurn;
            lootBalance += 0 - lootBalanceTp;
            lootBalanceTp += 0;
            var balance = sharesBalance + lootBalance;
            var updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "sharesBalance", sharesBalance},
                    { "sharesBalanceTp", sharesBalanceTp},
                    { "lootBalance", lootBalance},
                    { "lootBalanceTp", lootBalanceTp},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);

        }
        //
        private void handleSubmitProposalV2(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0)
            {
                getProposalName(jt["contractHash"].ToString(), proposalIndex, out string proposalName, out string proposalDetail);
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", proposalIndex},
                        { "proposalQueueIndex", ""},
                        { "proposalId", projId+proposalIndex},
                        { "proposalName", proposalName},
                        { "proposalDetail", proposalDetail},
                        //{ "sharesRequested", long.Parse(jt["sharesRequested"].ToString())},
                        //{ "tokenTribute", jt["tokenTribute"].ToString()},
                        { "sharesRequested", long.Parse(jt["sharesRequested"].ToString())},
                        { "lootRequested", long.Parse(jt["sharesRequested"].ToString())},
                        { "tributeOffered", jt["tributeOffered"].ToString()},
                        { "tributeToken", jt["tributeToken"].ToString()},
                        { "paymentRequested", jt["paymentRequested"].ToString()},
                        { "paymentToken", jt["paymentToken"].ToString()},
                        { "startingPeriod", ""},

                        { "proposalState", ProposalState.PreVote}, // -->
                        { "handleState", ProposalHandleState.Not},
                        { "voteYesCount", 0},
                        { "voteNotCount", 0},
                        { "proposer", jt["memberAddress"]},
                        { "delegateKey", jt["delegateKey"]},
                        { "applicant", jt["applicant"]},
                        { "transactionHash", jt["transactionHash"]},
                        { "contractHash", jt["contractHash"]},
                        { "blockNumber", jt["blockNumber"] },
                        { "blockTime", jt["blockTime"] },
                        {"time", TimeHelper.GetTimeStamp() }
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, newdata);
            }
        }
        private void handleSponsorProposalV2(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"];
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0) return;

            var updateStr = new JObject { { "$set", new JObject {
                    { "proposalQueueIndex", jt["proposalQueueIndex"]},
                    { "proposalState", ProposalState.Voting},
                    { "startingPeriod", jt["startingPeriod"]}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }
        private void handleProcessProposalV2(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"];
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            if (queryRes.Count == 0) return;

            var item = queryRes[0];
            var state = item["proposalState"].ToString();
            if (state == ProposalState.HandleTimeOut) return;

            var didPass = jt["didPass"].ToString();
            var voteRes = getProposalState(didPass);
            //
            if (voteRes == ProposalState.PassYes)
            {
                // 受益人收到股份, 自动退回权限
                resetDelegateKey(item["applicant"].ToString());
            }
            var updateStr = new JObject { { "$set", new JObject { { "proposalState", voteRes }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }
        private void handleCancelProposalV2(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"];
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0) return;

            var updateStr = new JObject { { "$set", new JObject {
                    { "proposalState", ProposalState.Aborted},
                    { "handleState", ProposalHandleState.Yes},
                } } }.ToString();
        }
        private void handleRagequitV2(JToken jt)
        {
            var projId = jt["projId"].ToString();
            var memberAddress = jt["memberAddress"].ToString();
            var sharesToBurn = long.Parse(jt["sharesToBurn"].ToString());
            var lootToBurn = long.Parse(jt["lootToBurn"].ToString());
            sharesToBurn *= -1;
            lootToBurn *= -1;
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", memberAddress } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            var now = TimeHelper.GetTimeStamp();
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                    { "projId", projId},
                    { "proposalQueueIndex", ""},
                    { "type", BalanceType.Balance},
                    { "balance", sharesToBurn},
                    { "sharesBalance", sharesToBurn},
                    { "sharesBalanceTp", sharesToBurn},
                    { "sharesBalance", 0},
                    { "sharesBalanceTp", 0},
                    { "newDelegateKey",""},
                    { "time", now},
                    { "lastUpdateTime", now}
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, newdata);
                return;
            }
            var rItem = queryRes[0];
            var sharesBalance = long.Parse(rItem["sharesBalance"].ToString());
            var sharesBalanceTp = long.Parse(rItem["sharesBalanceTp"].ToString());
            var lootBalance = long.Parse(rItem["lootBalance"].ToString());
            var lootBalanceTp = long.Parse(rItem["lootBalanceTp"].ToString());

            if (tempNotClearAllFlag)
            {
                sharesBalanceTp = 0;
                lootBalanceTp = 0;
            }
            sharesBalance += sharesToBurn - sharesBalanceTp;
            sharesBalanceTp += sharesToBurn;
            lootBalance += lootToBurn - lootBalanceTp;
            lootBalanceTp += lootToBurn;
            var balance = sharesBalance + lootBalance;
            var updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "sharesBalance", sharesBalance},
                    { "sharesBalanceTp", sharesBalanceTp},
                    { "lootBalance", lootBalance},
                    { "lootBalanceTp", lootBalanceTp},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }

        // 委托
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

        // 提案信息
        private void putProposalNameToDB(string contractHash, string proposalIndex, string proposalName, string proposalDetail)
        {
            var newdata = new JObject {
                { "contractHash", contractHash },
                { "proposalIndex", proposalIndex },
                { "proposalName", proposalName },
                { "proposalDetail", proposalDetail }
            }.ToString();
            mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalNameInfoCol, newdata);
        }
        private bool getProposalNameFromDB(string contractHash, string proposalIndex, out string proposalName, out string proposalDetail)
        {
            proposalName = "";
            proposalDetail = "";
            var findStr = new JObject { { "contractHash", contractHash }, { "proposalIndex", proposalIndex } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalNameInfoCol, findStr);
            if (queryRes.Count == 0) return false;

            var item = queryRes[0];
            proposalName = item["proposalName"].ToString();
            proposalDetail = item["proposalDetail"].ToString();
            return true;

        }
        private void getProposalNameFromChain(string contractHash, string proposalIndex, out string proposalName, out string proposalDetail)
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
        private void getProposalName(string contractHash, string proposalIndex, out string proposalName, out string proposalDetail)
        {
            if (getProposalNameFromDB(contractHash, proposalIndex, out proposalName, out proposalDetail)) return;
            getProposalNameFromChain(contractHash, proposalIndex, out proposalName, out proposalDetail);
            putProposalNameToDB(contractHash, proposalIndex, proposalName, proposalDetail);
        }


        // 提案-个人投票数
        private long getCurrentBalance(string projId, string address)
        {
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", "" }, { "address", address } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            if (queryRes.Count == 0) return 0;

            var item = queryRes[0];
            return long.Parse(item["sharesBalance"].ToString());
        }

        // 提案-赞成和反对票数
        private Dictionary<string, long> getCurrentVoteCount(string projId, string proposalIndex)
        {
            var match = new JObject { { "$match", new JObject { { "projId", projId }, { "proposalQueueIndex", proposalIndex } } } }.ToString();
            var group = new JObject { { "$group", new JObject { { "_id", "$type" }, { "sum", new JObject { { "$sum", "$balance" } } } } } }.ToString();
            var list = new List<string> { match, group };
            var queryRes = mh.Aggregate(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, list);
            if (queryRes.Count == 0) return new Dictionary<string, long>();

            return queryRes.ToDictionary(k => k["_id"].ToString(), v => long.Parse(v["sum"].ToString()));
        }

        // 提案结果
        private string getProposalState(string didPass)
        {
            return didPass == "1" ? ProposalState.PassYes : ProposalState.PassNot;
        }

        //
        private void clearTempRes(string projId)
        {
            // 清除临时字段数据
            tempNotClearAllFlag = false;
            try
            {
                var findStr = new JObject { { "projId", projId }, { "balanceTp", new JObject { { "$exists", true }, { "$ne", 0 } } } }.ToString();
                var updateStr = new JObject { { "$set", new JObject { { "balanceTp", 0 } } } }.ToString();
                mh.UpdateDataMany(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
                findStr = new JObject { { "projId", projId }, { "sharesBalanceTp", new JObject { { "$exists", true }, { "$ne", 0 } } } }.ToString();
                updateStr = new JObject { { "$set", new JObject { { "sharesBalanceTp", 0 } } } }.ToString();
                mh.UpdateDataMany(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
                findStr = new JObject { { "projId", projId }, { "lootBalanceTp", new JObject { { "$exists", true }, { "$ne", 0 } } } }.ToString();
                updateStr = new JObject { { "$set", new JObject { { "lootBalanceTp", 0 } } } }.ToString();
                mh.UpdateDataMany(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
            }
            catch (Exception ex)
            {
                // 
                Console.WriteLine(ex);
                tempNotClearAllFlag = true;
            }
        }
        private bool tempNotClearAllFlag = true;

        // 状态变动
        private void handleProposalState()
        {
            var now = TimeHelper.GetTimeStamp();
            // Voting -> Noting/PassNot
            var timeLimit = votingPeriod;
            var findStr = new JObject { { "proposalState", ProposalState.Voting }, { "blockTime", new JObject { { "$lt", now - timeLimit } } } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            var res = filterProjHasProccedBlockTime(queryRes, timeLimit);
            var updateStr = "";
            if (res != null && res.Count() > 0)
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

        //
        private void log(long lastCounter, long lh, long rh, string key = "logs")
        {
            Console.WriteLine("{0}.[{1}]processed: {2}-{3}/{4}", name(), key, lastCounter, lh, rh);
        }
        private void updateL(long lastCounter, long index, long time, string key = "logs")
        {
            var findStr = new JObject { { "counter", key } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjCounter, findStr) == 0)
            {
                var newdata = new JObject {
                    { "counter", key },
                    { "lastCounter", lastCounter },
                    { "lastBlockIndex", index },
                    { "lastBlockTime", time}
                }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjCounter, newdata);
                return;
            }
            var updateJo = new JObject { { "lastBlockIndex", index }, { "lastBlockTime", time } };
            if (lastCounter > 0) updateJo.Add("lastCounter", lastCounter);
            var updateStr = new JObject { { "$set", updateJo } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjCounter, updateStr, findStr);
        }
        private long getLh(out long lastCounter, out long lt, string key = "logs")
        {
            lastCounter = 0;
            lt = 0;
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjCounter, findStr);
            if (queryRes.Count == 0) return -1;
            var item = queryRes[0];
            lastCounter = long.Parse(item["lastCounter"].ToString());
            lt = long.Parse(item["lastBlockTime"].ToString());
            return long.Parse(item["lastBlockIndex"].ToString());
        }
        private long getRh(out long lastCounter, out long rt, string key = "logs")
        {
            lastCounter = 0;
            rt = 0;
            var findStr = new JObject { { "counter", key } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, notifyCounter, findStr);
            if (queryRes.Count == 0) return -1;

            var item = queryRes[0];
            lastCounter = long.Parse(item["lastCounter"].ToString());
            rt = long.Parse(item["lastBlockTime"].ToString());
            return long.Parse(item["lastBlockIndex"].ToString());
        }

    }
}
