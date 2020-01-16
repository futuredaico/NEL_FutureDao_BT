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
        private string moloProjFundInfoCol = "moloprojfundinfos";
        private string projHashCol = "moloprojhashinfos";
        private string projInfoCol = "moloprojinfos";
        private long votingPeriod = 17280 * 5 * 7;
        private long notingPeriod = 17280 * 5 * 7;
        private long handlingPeriod = 17280 * 5 * 7;
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
                handlingPeriod = 120 * 2;
            }
            //addPrefix();
            initIndex();
        }

        private void addPrefix()
        {
            var prefix = "zbak36_";
            //notifyCounter = prefix + notifyCounter;
            //notifyCol = prefix + notifyCol;
            moloProjCounter = prefix + moloProjCounter;
            moloProjProposalInfoCol = prefix + moloProjProposalInfoCol;
            moloProjBalanceInfoCol = prefix + moloProjBalanceInfoCol;
            moloProjFundInfoCol = prefix + moloProjFundInfoCol;
            projInfoCol = prefix + projInfoCol;
            prefix = "";
            moloProjProposalNameInfoCol = prefix + moloProjProposalNameInfoCol;
            projHashCol = prefix + projHashCol;
        }
        private void initIndex()
        {
            mh.setIndex(lConn.connStr, lConn.connDB, projInfoCol, 
                "{'projId':1}", "i_projId");
            //
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, 
                "{'projId':1,'proposalIndex':1}", "i_projId_proposalIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, 
                "{'projId':1,'proposalQueueIndex':1}", "i_projId_proposalQueueIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, 
                "{'projId':1,'proposalState':1}", "i_projId_proposalState");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, 
                "{'proposalId':1}", "i_proposalId");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, 
                "{'proposalState':1,'blockTime':1}", "i_proposalState_blockTime");
            //
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalNameInfoCol, 
                "{'contractHash':1,'proposalIndex':1}", "i_contractHash_proposalIndex");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjProposalNameInfoCol,
                "{'contractHash':1,'proposalIndex':1}", "i_contractHash_proposalIndex");
            //
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, 
                "{'projId':1,'proposalQueueIndex':1,'address':1}", "i_projId_proposalQueueIndex_address");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol,
                "{'projId':1,'proposalQueueIndex':1,'balance':1}", "i_projId_proposalQueueIndex_balance");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol,
              "{'newDelegateKey':1}", "i_newDelegateKey");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol,
               "{'projId':1,'balanceTp'}", "i_projId_balanceTp");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol,
               "{'projId':1,'sharesBalanceTp'}", "i_projId_sharesBalanceTp");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol,
               "{'projId':1,'lootBalanceTp'}", "i_projId_lootBalanceTp");
            //
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjFundInfoCol,
               "{'projId':1,'fundHash'}", "i_projId_fundHash");
            mh.setIndex(lConn.connStr, lConn.connDB, moloProjFundInfoCol,
               "{'projId':1,'fundTotalTp'}", "i_projId_fundTotalTp");
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
                updateL(-1, rh, rt);
                log(lc, rh, rh);
                return;
            }
            //
            var res = queryRes.OrderBy(p => long.Parse(p["counter"].ToString()));
            foreach (var item in res)
            {
                var topics = item["topics"].ToString();
                
                processProjCreateTime(item, topics);
                processDelegateKey(item, topics);
                processProposalInfo(item, topics);
                processProposalQueueInfoV2(item, topics);
                processProposalVote(item, topics);
                processProposalVoteCount(item, topics);
                processProposalResult(item, topics);
                processAbort(item, topics);
                processPersonShare(item, topics);
                processProjShareAndTeamAndFund(item, topics);

                var counter = long.Parse(item["counter"].ToString());
                var blockNumber = long.Parse(item["blockNumber"].ToString());
                var blockTime = long.Parse(item["blockTime"].ToString());
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



        // 0.项目创建时间
        private void processProjCreateTime(JToken jt, string topics)
        {
            if (topics != Topic.SummonComplete.hash) return;
            //
            var projId = jt["projId"].ToString();
            var findStr = new JObject { { "projId", projId } }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                    { "startTime", jt["blockTime"] }
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, projInfoCol, updateStr, findStr);
        }

        // 1.委托地址
        private void processDelegateKey(JToken jt, string topics)
        {
            if (topics != Topic.UpdateDelegateKey.hash) return;
            //
            var projId = jt["projId"].ToString();
            var memberAddress = jt["memberAddress"].ToString();
            var newDelegateKey = jt["newDelegateKey"].ToString();
            var findStr = new JObject {
                { "projId", projId },
                { "proposalQueueIndex", "" },
                { "address", memberAddress }
            }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                { "newDelegateKey", newDelegateKey}
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }

        // 2.提案信息
        private void processProposalInfo(JToken jt, string topics)
        {
            // v2.0
            processProposalInfoV2(jt, topics);

            // v1.0
            if (topics != Topic.SubmitProposal.hash) return;
            //
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0)
            {
                getProposalName(jt["contractHash"].ToString(), proposalIndex, projId, out string proposalName, out string proposalDetail);
                var st = long.Parse(jt["blockTime"].ToString());
                var info = getProjStageInfo(projId);
                var voteExpiredTime = st + info.votePeriod;
                var noteExpiredTime = st + info.notePeriod;
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalIndex", proposalIndex},
                        { "proposalQueueIndex", proposalIndex},
                        { "proposalId", projId+proposalIndex},
                        { "proposalName", proposalName},
                        { "proposalDetail", proposalDetail},
                        { "sharesRequested", long.Parse(jt["sharesRequested"].ToString())},
                        { "tokenTribute", jt["tokenTribute"].ToString()},
                        { "tokenTributeSymbol", jt["tokenTributeSymbol"].ToString()},
                        { "tokenTributeHash", jt["tokenTributeHash"].ToString()},
                        { "proposalState", ProposalState.Voting},
                        { "handleState", ProposalHandleState.Not},
                        { "voteYesCount", 0},
                        { "voteNotCount", 0},
                        { "proposer", getProposer(jt)},
                        { "memberAddress", jt["memberAddress"]},
                        { "delegateKey", jt["delegateKey"]},
                        { "applicant", jt["applicant"]},
                        { "transactionHash", jt["transactionHash"]},
                        { "contractHash", jt["contractHash"]},
                        { "voteExpiredTime", voteExpiredTime},
                        { "noteExpiredTime", noteExpiredTime},
                        { "blockNumber", jt["blockNumber"] },
                        { "blockTime", jt["blockTime"] },
                        {"time", TimeHelper.GetTimeStamp() }
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, newdata);
            }
        }
        private void processProposalInfoV2(JToken jt, string topics)
        {
            if (topics != Topic.SubmitProposal_v2.hash) return;

            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0)
            {
                getProposalName(jt["contractHash"].ToString(), proposalIndex, projId, out string proposalName, out string proposalDetail);

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
                        { "lootRequested", long.Parse(jt["lootRequested"].ToString())},
                        { "tributeOffered", jt["tributeOffered"].ToString()},
                        { "tributeOfferedSymbol", jt["tributeOfferedSymbol"].ToString()},
                        { "tributeToken", jt["tributeToken"].ToString()},
                        { "paymentRequested", jt["paymentRequested"].ToString()},
                        { "paymentRequestedSymbol", jt["paymentRequestedSymbol"].ToString()},
                        { "paymentToken", jt["paymentToken"].ToString()},
                        { "startingPeriod", ""},

                        { "proposalState", ProposalState.PreVote}, // -->
                        { "handleState", ProposalHandleState.Not},
                        { "voteYesCount", 0},
                        { "voteNotCount", 0},
                        { "proposer", getProposer(jt)},
                        { "memberAddress", jt["memberAddress"]},
                        { "delegateKey", jt["delegateKey"]},
                        { "applicant", jt["applicant"]},
                        { "transactionHash", jt["transactionHash"]},
                        { "contractHash", jt["contractHash"]},
                        { "blockNumber", jt["blockNumber"] },
                        { "blockTime", 0 },
                        {"time", TimeHelper.GetTimeStamp() }
                    }.ToString();
                mh.PutData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, newdata);
            }
        }
        private string getProposer(JToken jt)
        {
            var memberAddress = jt["memberAddress"].ToString().ToLower();
            var delegateKey = jt["delegateKey"].ToString().ToLower();
            if (memberAddress == "0x0000000000000000000000000000000000000000")
            {
                return delegateKey;
            }
            return memberAddress;
        }
        private class ProjStageInfo
        {
            public long votePeriod { get; set; }
            public long notePeriod { get; set; }
        }
        private Dictionary<string, ProjStageInfo> stageDict;
        private ProjStageInfo getProjStageInfo(string projId)
        {
            if(stageDict == null)
            {
                stageDict = new Dictionary<string, ProjStageInfo>();
            }
            if(!stageDict.ContainsKey(projId))
            {
                stageDict.Add(projId, getProjStageInfoFromDB(projId));
            }
            var info = stageDict.GetValueOrDefault(projId);
            return info;
        }
        private ProjStageInfo getProjStageInfoFromDB(string projId)
        {
            var findStr = new JObject { { "projId", projId } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projInfoCol, findStr);
            if (queryRes.Count == 0) throw new Exception("Not find projInfo by projId:" + projId);

            
            var item = queryRes[0];
            var info = new ProjStageInfo
            {
                votePeriod = long.Parse(item["votePeriod"].ToString()),
                notePeriod = long.Parse(item["notePeriod"].ToString())
            };
            return info;
        }

        // 3.提案队列信息(v2.0新增)
        private void processProposalQueueInfoV2(JToken jt, string topics)
        {
            if (topics != Topic.SponsorProposal_v2.hash) return;
            //
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"];
            var proposalQueueIndex = jt["proposalQueueIndex"];
            var startingPeriod = jt["startingPeriod"];
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0) return;

            var updateStr = new JObject { { "$set", new JObject {
                    { "proposalQueueIndex", proposalQueueIndex},
                    { "proposalState", ProposalState.Voting},
                    { "blockNumber", jt["blockNumber"] },
                    { "blockTime", jt["blockTime"] },
                    { "startingPeriod", startingPeriod}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }

        // 4.提案个人投票
        private void processProposalVote(JToken jt, string topics)
        {
            if (topics != Topic.SubmitVote.hash) return;
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

        }

        // 5.提案赞成反对票
        private void processProposalVoteCount(JToken jt, string topics)
        {
            if (topics != Topic.SubmitVote.hash) return;
            //
            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"].ToString();
            var rr = getCurrentVoteCount(projId, proposalQueueIndex);
            var zanYesCount = rr.GetValueOrDefault(BalanceType.ZanYes);
            var zanNotCount = rr.GetValueOrDefault(BalanceType.ZanNot);
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "voteYesCount", zanYesCount }, { "voteNotCount", zanNotCount } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }

        // 6.提案处理结果
        private void processProposalResult(JToken jt, string topics)
        {
            //v2.0
            processProposalResultV2(jt, topics);

            //v1.0
            if (topics != Topic.ProcessProposal.hash) return;
            //
            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"].ToString();
            var applicant = jt["applicant"].ToString();
            var didPass = getProposalState(jt["didPass"].ToString());
            if (didPass == ProposalState.PassYes)
            {
                // 受益人收到股份, 自动退回权限
                resetDelegateKey(applicant);
            }
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            var updateStr = new JObject { { "$set", new JObject { { "proposalState", getProposalState(didPass) }, { "handleState", ProposalHandleState.Yes } } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }
        private void processProposalResultV2(JToken jt, string topics)
        {
            if (topics != Topic.ProcessProposal_v2.hash) return;

            var projId = jt["projId"].ToString();
            var proposalQueueIndex = jt["proposalIndex"];
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", proposalQueueIndex } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            if (queryRes.Count == 0) return;

            var item = queryRes[0];
            var state = item["proposalState"].ToString();
            if (state == ProposalState.HandleTimeOut)
            {
                var subUpdateStr = new JObject { { "$set", new JObject { { "handleState", ProposalHandleState.Yes } } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, subUpdateStr, findStr);
                return;
            }

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
        // 7.提案终止
        private void processAbort(JToken jt, string topics)
        {
            // v2.0
            processCancleProposalV2(jt, topics);
            // v1.0
            if (topics != Topic.Abort.hash) return;

            var projId = jt["projId"].ToString();
            var propoalQueueIndex = jt["proposalIndex"].ToString();
            var findStr = new JObject { { "projId", projId }, { "proposalQueueIndex", propoalQueueIndex } }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                { "proposalState", ProposalState.Aborted },
                { "handleState", ProposalHandleState.Yes }
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);

        }
        private void processCancleProposalV2(JToken jt, string topics)
        {
            if (topics != Topic.CancelProposal_v2.hash) return;
            //
            var projId = jt["projId"].ToString();
            var proposalIndex = jt["proposalIndex"];
            var findStr = new JObject { { "projId", projId }, { "proposalIndex", proposalIndex } }.ToString();
            if (mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr) == 0) return;

            var updateStr = new JObject { { "$set", new JObject {
                    { "proposalState", ProposalState.Aborted},
                    { "handleState", ProposalHandleState.Yes},
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
        }


        // 8.个人股份余额
        private void processPersonShare(JToken jt, string topics)
        {
            //v2.0
            processPersonShareV2(jt, topics);
            //v1.0
            if (!(topics == Topic.SummonComplete.hash
                || (topics == Topic.ProcessProposal.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Ragequit.hash
                ))
            {
                return;
            }
            var address = "";
            var shares = 0L;
            if (topics == Topic.SummonComplete.hash)
            {
                address = jt["summoner"].ToString();
                shares = long.Parse(jt["shares"].ToString());
            } else if(topics == Topic.ProcessProposal.hash)
            {
                address = jt["applicant"].ToString();
                shares = long.Parse(jt["sharesRequested"].ToString());

            } else
            {
                address = jt["memberAddress"].ToString();
                shares = long.Parse(jt["sharesToBurn"].ToString());
                shares *= -1;
            }
            var projId = jt["projId"].ToString();
            var now = TimeHelper.GetTimeStamp();
            var findStr = new JObject {
                    { "projId", projId },
                    { "proposalQueueIndex", "" },
                    { "address", address }
                }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalQueueIndex", ""},
                        { "type", BalanceType.Balance},
                        { "address", address},
                        { "balance", shares},
                        { "sharesBalance", shares},
                        { "sharesBalanceTp", shares},
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

            if (tempNotClearAllFlag) sharesBalanceTp = 0;
            sharesBalance += shares - sharesBalanceTp;
            sharesBalanceTp += shares;
            var balance = sharesBalance;
            var updateStr = new JObject { {"$set", new JObject {
                    { "balance", balance},
                    { "sharesBalance", sharesBalance},
                    { "sharesBalanceTp", sharesBalanceTp},
                    { "lastUpdateTime", now}
                } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, updateStr, findStr);
        }
        private void processPersonShareV2(JToken jt, string topics)
        {
            if (!((topics == Topic.ProcessProposal_v2.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Ragequit_v2.hash
                ))
            {
                return;
            }
            //
            var projId = jt["projId"].ToString();
            

            var address = "";
            var shares = 0L;
            var loot = 0L;
            if (topics == Topic.ProcessProposal_v2.hash)
            {
                var proposalQueueIndex = jt["proposalIndex"].ToString();
                var subfindStr = new JObject { { "projId", projId },{ "proposalQueueIndex", proposalQueueIndex} }.ToString();
                var subqueryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, subfindStr);
                if (subqueryRes.Count == 0) return;

                var item = subqueryRes[0];
                shares = long.Parse(item["sharesRequested"].ToString());
                loot = long.Parse(item["lootRequested"].ToString());
                address = item["applicant"].ToString();
            } else
            {
                shares = long.Parse(jt["sharesToBurn"].ToString());
                loot = long.Parse(jt["lootToBurn"].ToString());
                shares *= -1;
                loot *= -1;
                address = jt["memberAddress"].ToString();
            }

            var now = TimeHelper.GetTimeStamp();
            var findStr = new JObject {
                    { "projId", projId },
                    { "proposalQueueIndex", "" },
                    { "address", address }
                }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);
            if (queryRes.Count == 0)
            {
                var newdata = new JObject {
                        { "projId", projId},
                        { "proposalQueueIndex", ""},
                        { "type", BalanceType.Balance},
                        { "address", address},
                        { "balance", shares + loot},
                        { "sharesBalance", shares},
                        { "sharesBalanceTp", shares},
                        { "lootBalance", loot},
                        { "lootBalanceTp", loot},
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
            lootBalance += loot - lootBalanceTp;
            lootBalanceTp += loot;
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


        // 9.项目股份总额、持有人数、资金总额
        private void processProjShareAndTeamAndFund(JToken jt, string topics)
        {
            handleShareAndFund(jt);
        }

        
        //
        private void handleShareAndFund(JToken jt)
        {
            var topics = jt["topics"].ToString();
            // 项目股份总额
            // v1.0 + v2.0
            if (topics == Topic.SummonComplete.hash
                || (topics == Topic.ProcessProposal.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Ragequit.hash
                || (topics == Topic.ProcessProposal_v2.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Ragequit_v2.hash
                )
            {
                var projId = jt["projId"].ToString();
                var tokenDict = getCurrentVoteCount(projId, "");
                var tokenTotal = tokenDict.GetValueOrDefault(BalanceType.Balance);

                var findStr = new JObject { { "projId", projId } }.ToString();
                var updateStr = new JObject { { "$set", new JObject {
                        { "tokenTotal", tokenTotal}
                    } } }.ToString();
                mh.UpdateData(lConn.connStr, lConn.connDB, projInfoCol, updateStr, findStr);

                // 总持有人数
                handleHasTokenCount(projId);
            }

            // 项目资产总额
            List<FundInfo> list = new List<FundInfo>();
            if ((topics == Topic.ProcessProposal.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Withdrawal.hash
                || (topics == Topic.ProcessProposal_v2.hash && getProposalState(jt["didPass"].ToString()) == ProposalState.PassYes)
                || topics == Topic.Withdrawal_v2.hash
                )
            {
                var projId = jt["projId"].ToString();
                if(topics == Topic.ProcessProposal.hash)
                {
                    var tokenTribute = jt["tokenTribute"].ToString();
                    var tokenTributeSymbol = jt["tokenTributeSymbol"].ToString();
                    var tokenTributeHash = jt["tokenTributeHash"].ToString();
                    var tokenTributeDecimals = long.Parse(jt["tokenTributeDecimals"].ToString());
                    list.Add(new FundInfo
                    {
                        projId = projId,
                        amount = tokenTribute,
                        symbol = tokenTributeSymbol,
                        hash  = tokenTributeHash,
                        decimals = tokenTributeDecimals,
                        sig = 1
                    });
                }
                else if (topics == Topic.Withdrawal.hash)
                {
                    var amount = jt["amount"].ToString();
                    var amountSymbol = jt["amountSymbol"].ToString();
                    var amountHash = jt["amountHash"].ToString();
                    var amountDecimals = long.Parse(jt["amountDecimals"].ToString());
                    list.Add(new FundInfo
                    {
                        amount = amount,
                        symbol = amountSymbol,
                        hash = amountHash,
                        decimals = amountDecimals,
                        sig = 0
                    });
                }
                else if(topics == Topic.ProcessProposal_v2.hash)
                {
                    var proposalIndex = jt["proposalIndex"].ToString();
                    var findStr = new JObject {
                        { "projId", projId },{ "proposalQueueIndex", proposalIndex }
                    }.ToString();
                    var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
                    if (queryRes.Count == 0) return;

                    var item = queryRes[0];
                    var tributeOffered = item["tributeOffered"].ToString();
                    var tributeTokenSymbol = item["tributeOfferedSymbol"].ToString();
                    var tributeToken = item["tributeToken"].ToString();
                    var tributeOfferedDecimals = long.Parse(item["tributeOfferedDecimals"].ToString());
                    list.Add(new FundInfo
                    {
                        amount = tributeOffered,
                        symbol = tributeTokenSymbol,
                        hash = tributeToken,
                        decimals = tributeOfferedDecimals,
                        sig = 1
                    });
                    var paymentRequested = item["paymentRequested"].ToString();
                    var paymentTokenSymbol = item["paymentRequestedSymbol"].ToString();
                    var paymentToken = item["paymentToken"].ToString();
                    var paymentRequestedDecimals = long.Parse(item["paymentRequestedDecimals"].ToString());
                    list.Add(new FundInfo
                    {
                        amount = paymentRequested,
                        symbol = paymentTokenSymbol,
                        hash = paymentToken,
                        decimals = paymentRequestedDecimals,
                        sig = 0
                    });
               
                } else
                {
                    var amount = jt["amount"].ToString();
                    var amountSymbol = jt["amountSymbol"].ToString();
                    var tokenAddress = jt["tokenAddress"].ToString();
                    var amountDecimals = long.Parse(jt["amountDecimals"].ToString());
                    list.Add(new FundInfo {
                        amount = amount,
                        symbol = amountSymbol,
                        hash = tokenAddress,
                        decimals = amountDecimals,
                        sig = 0
                    });
                }

                var res = 
                list.GroupBy(p => p.hash, (k, g) =>
                {
                    return new
                    {
                        h = k,
                        s = g.ToArray()[0].symbol,
                        d = g.ToArray()[0].decimals,
                        m = g.Sum(gp =>
                        {
                            if (gp.sig == 1) return decimal.Parse(gp.amount);
                            return decimal.Parse(gp.amount) * -1;
                        })
                    };
                });
                foreach(var item in res)
                {
                    if (item.s.ToLower() == "eth" && item.m == decimal.Zero) continue;
                    var findStr = new JObject { { "projId", projId },{ "fundHash", item.h} }.ToString();
                    var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjFundInfoCol, findStr);
                    if(queryRes.Count == 0)
                    {
                        var newdata = new JObject {
                            {"projId", projId },
                            {"fundHash", item.h },
                            {"fundSymbol", item.s },
                            {"fundDecimals", item.d },
                            {"fundTotal", item.m },
                            {"fundTotalTp", item.m },
                        }.ToString();
                        mh.PutData(lConn.connStr, lConn.connDB, moloProjFundInfoCol, newdata);
                        continue;
                    }
                    var rItem = queryRes[0];
                    var fundTotal = decimal.Parse(rItem["fundTotal"].ToString());
                    var fundTotalTp = decimal.Parse(rItem["fundTotalTp"].ToString());
                    if(tempNotClearAllFlag) fundTotalTp = 0;

                    fundTotal += item.m - fundTotalTp;
                    fundTotalTp = item.m;
                    var updateStr = new JObject { { "$set", new JObject {
                        { "fundTotal",fundTotal.ToString()},
                        { "fundTotalTp",fundTotalTp.ToString()}
                    } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjFundInfoCol, updateStr, findStr);
                }
            }
        }
        private void handleHasTokenCount(string projId)
        {
            var findStr = new JObject {
                { "projId", projId },
                { "proposalQueueIndex", "" },
                { "balance", new JObject { { "$ne", 0 } } }
            }.ToString();
            var count = mh.GetDataCount(lConn.connStr, lConn.connDB, moloProjBalanceInfoCol, findStr);

            findStr = new JObject { { "projId", projId } }.ToString();
            var updateStr = new JObject { { "$set", new JObject {
                { "hasTokenCount", count}
            } } }.ToString();
            mh.UpdateData(lConn.connStr, lConn.connDB, projInfoCol, updateStr, findStr);
        }
        private class FundInfo
        {
            public string projId { get; set; }
            public string amount { get; set; }
            public string symbol { get; set; }
            public long decimals { get; set; }
            public string hash { get; set; }
            public int sig { get; set; }
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
        private void getProposalNameFromChain(string contractHash, string proposalIndex, string version, out string proposalName, out string proposalDetail)
        {
            proposalName = "";
            proposalDetail = "";
            var res = "";
            try
            {
                //res = EthHelper.ethCall(contractHash, proposalIndex, netType);
                res = EthHelper.getProposalInfo(contractHash, proposalIndex, version, netType);
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
        private void getProposalName(string contractHash, string proposalIndex, string projId, out string proposalName, out string proposalDetail)
        {
            if (getProposalNameFromDB(contractHash, proposalIndex, out proposalName, out proposalDetail)) return;
            var version = getProjVersion(projId);
            getProposalNameFromChain(contractHash, proposalIndex, version, out proposalName, out proposalDetail);
            putProposalNameToDB(contractHash, proposalIndex, proposalName, proposalDetail);
        }
        private string getProjVersion(string projId)
        {
            var findStr = new JObject { { "projId", projId} }.ToString();
            var fieldStr = "{'projVersion':1}";
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projInfoCol, findStr, fieldStr);

            return queryRes[0]["projVersion"].ToString();
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

                findStr = new JObject { { "projId", projId }, { "fundTotalTp", new JObject { { "$ne", "0"} } } }.ToString();
                updateStr = new JObject { { "$set", new JObject { { "fundTotalTp", "0" } } } }.ToString();
                mh.UpdateDataMany(lConn.connStr, lConn.connDB, moloProjFundInfoCol, updateStr, findStr);
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
            var lh = getLh(out long lc, out long lt);
            // Voting -> Noting/PassNot
            var timeLimit = lt - votingPeriod;
            var findStr = new JObject {
                { "proposalState", ProposalState.Voting }, { "blockTime", new JObject { { "$lt", timeLimit },{ "$gt",0} } } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            var res = filterProjWithEachConfig(queryRes, lt, StateFilterType.VOTE);
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
            timeLimit = lt - (votingPeriod + notingPeriod);
            findStr = new JObject {
                { "proposalState", ProposalState.Noting }, { "blockTime", new JObject { { "$lt", timeLimit }, { "$gt", 0 } } } }.ToString();
            queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            res = filterProjWithEachConfig(queryRes, lt, StateFilterType.NOTE);
            updateStr = "";
            if (res != null && res.Count() > 0)
            {
                foreach (var item in res)
                {
                    findStr = new JObject { { "projId", item["projId"] }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                    var state = ProposalState.PassYes;
                    if (item["lootRequested"] != null) state = ProposalState.WaitHandle;
                    updateStr = new JObject { { "$set", new JObject { { "proposalState", state } } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
                }
            }

            // WaitHandle -> HandleTimeOut
            timeLimit = lt - (votingPeriod + notingPeriod + handlingPeriod);
            findStr = new JObject {
               { "proposalState", ProposalState.WaitHandle }, { "blockTime", new JObject { { "$lt", timeLimit }, { "$gt", 0 } } } }.ToString();
            queryRes = mh.GetData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, findStr);
            res = filterProjWithEachConfig(queryRes, lt, StateFilterType.WaitHandle);
            updateStr = "";
            if (queryRes != null && queryRes.Count() > 0)
            {
                foreach (var item in queryRes)
                {
                    findStr = new JObject { { "projId", item["projId"] }, { "proposalIndex", item["proposalIndex"] } }.ToString();
                    var state = ProposalState.PassYes;
                    if (item["lootRequested"] != null) state = ProposalState.HandleTimeOut;
                    updateStr = new JObject { { "$set", new JObject { { "proposalState", state } } } }.ToString();
                    mh.UpdateData(lConn.connStr, lConn.connDB, moloProjProposalInfoCol, updateStr, findStr);
                }
            }
        }
        private JToken[] filterProjWithEachConfig(JArray JA, long lt, string type)
        {
            return JA.Where(p => filterProjWithEachConfig(p, lt, type)).ToArray();
        }
        private bool filterProjWithEachConfig(JToken jt, long lt, string type)
        {
            var findStr = new JObject { { "projId", jt["projId"] } }.ToString();
            var queryRes = mh.GetData(lConn.connStr, lConn.connDB, projInfoCol, findStr);
            if (queryRes.Count == 0) return false;

            var item = queryRes[0];
            var startTime = long.Parse(item["startTime"].ToString());
            if (item["projVersion"].ToString() == "2.0") startTime = long.Parse(jt["blockTime"].ToString());
            var timeLimit = getTimeLimit(item, type);
            return lt > startTime + timeLimit;
        }
        private long getTimeLimit(JToken jt, string type)
        {
            if (type == StateFilterType.VOTE)
            {
                return long.Parse(jt["votePeriod"].ToString());
            }
            if (type == StateFilterType.NOTE)
            {
                return long.Parse(jt["notePeriod"].ToString());
            }
            if (type == StateFilterType.CANCEL)
            {
                return long.Parse(jt["cancelPeriod"].ToString());
            }
            if (type == StateFilterType.WaitHandle)
            {
                return long.Parse(jt["emergencyExitWaitPeriod"].ToString());
            }
            return 0;
        }
        class StateFilterType
        {
            public const string VOTE = "0";
            public const string NOTE = "1";
            public const string CANCEL = "2";
            public const string WaitHandle = "3";
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
