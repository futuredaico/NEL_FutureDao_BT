
namespace NEL_FutureDao_BT.task
{
    /*
   "0x03995a801b13c36325306deef859ef977ce61c6e15a794281bf969d204825227",   // SummonComplete           0
   "0xde7b64a369e10562cc2e71f0f1f944eaf144b75fead6ecb51fac9c4dd6934885"，  // UpdateDelegateKey        1
   "0x2d105ebbc222c190059b3979356e13469f6a29a350add74ac3bf4f22f16301d6",   // SubmitProposal           2
   "0x29bf0061f2faa9daa482f061b116195432d435536d8af4ae6b3c5dd78223679b",   // SubmitVote               3
   "0x3f6fc303a82367bb4947244ba21c569a5ed2e870610f1a693366142309d7cbea",   // ProcessProposal          4
   "0xa749dd3df92cae4d106b3eadf60c0dffd3698de09c67ce58ce6f5d02eb821313",   // Abort                    5
   "0x667cb7a1818eacd2e3a421e734429ba9c4c7dec85e578e098b6d72cd2bfbf2f6",   // Ragequit
   "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65",   // Withdrawal
   "0x1facc413faf060d5cc5c8bc12d8fe29e5c530eb98e2475c76733ec749787a750",   // SubmitProposal_v2        6
   "0x2a383a979381335e3eb401ac01dd8083e024ff0256bf5338456ffc0063390bbd",   // SponsorProposal_v2       7
   "0x86f74240ecee9e4230d26ff92e17fee978460d9c0f78f5c88b2864c9e7a49427",   // ProcessProposal_v2       8
   "0xc215fed6680bb02d323dc3f8b8f85241572607538426059c9232601bd293c3be",   // CancelProposal_v2        9
   "0xcad1a1c68982832d9abc314de8a1e5d5e8c81b0588961e360766736d10c3be1a",   // Ragequit_v2
   "0x2717ead6b9200dd235aad468c9809ea400fe33ac69b5bfaa6d3e90fc922b6398",   // Withdrawal_v2
   "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",   // Transfer
   "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",   // Approval
   "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",   // OwnershipTransferred
            */
    class Topic
    {
        public static TopicKV Transfer = new TopicKV
        {
            hash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            eventName = "Transfer"
        };

        public static TopicKV Approval = new TopicKV
        {
            hash = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
            eventName = "Approval"
        };

        public static TopicKV OwnershipTransferred = new TopicKV
        {
            hash = "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0",
            eventName = "OwnershipTransferred"
        };

        //
        public static TopicKV SummonComplete = new TopicKV
        {
            hash = "0x03995a801b13c36325306deef859ef977ce61c6e15a794281bf969d204825227",
            eventName = "SummonComplete"
        };
        public static TopicKV UpdateDelegateKey = new TopicKV
        {
            hash = "0xde7b64a369e10562cc2e71f0f1f944eaf144b75fead6ecb51fac9c4dd6934885",
            eventName = "UpdateDelegateKey"
        };
        public static TopicKV SubmitProposal = new TopicKV
        {
            hash = "0x2d105ebbc222c190059b3979356e13469f6a29a350add74ac3bf4f22f16301d6",
            eventName = "SubmitProposal"
        };
        public static TopicKV SubmitVote = new TopicKV
        {
            hash = "0x29bf0061f2faa9daa482f061b116195432d435536d8af4ae6b3c5dd78223679b",
            eventName = "SubmitVote"
        };
        public static TopicKV ProcessProposal = new TopicKV
        {
            hash = "0x3f6fc303a82367bb4947244ba21c569a5ed2e870610f1a693366142309d7cbea",
            eventName = "ProcessProposal"
        };
        public static TopicKV Ragequit = new TopicKV
        {
            hash = "0x667cb7a1818eacd2e3a421e734429ba9c4c7dec85e578e098b6d72cd2bfbf2f6",
            eventName = "Ragequit"
        };
        public static TopicKV Abort = new TopicKV
        {
            hash = "0xa749dd3df92cae4d106b3eadf60c0dffd3698de09c67ce58ce6f5d02eb821313",
            eventName = "Abort"
        };
        public static TopicKV Withdrawal = new TopicKV
        {
            hash = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65",
            eventName = "Withdrawal"
        };
        //
        public static TopicKV SubmitProposal_v2 = new TopicKV
        {
            hash = "0x1facc413faf060d5cc5c8bc12d8fe29e5c530eb98e2475c76733ec749787a750",
            eventName = "SubmitProposal_v2"
        };
        public static TopicKV SponsorProposal_v2 = new TopicKV
        {
            hash = "0x2a383a979381335e3eb401ac01dd8083e024ff0256bf5338456ffc0063390bbd",
            eventName = "SponsorProposal_v2"
        };
        public static TopicKV ProcessProposal_v2 = new TopicKV
        {
            hash = "0x86f74240ecee9e4230d26ff92e17fee978460d9c0f78f5c88b2864c9e7a49427",
            eventName = "ProcessProposal_v2"
        };
        public static TopicKV CancelProposal_v2 = new TopicKV
        {
            hash = "0xc215fed6680bb02d323dc3f8b8f85241572607538426059c9232601bd293c3be",
            eventName = "CancelProposal_v2"
        };
        public static TopicKV Ragequit_v2 = new TopicKV
        {
            hash = "0xcad1a1c68982832d9abc314de8a1e5d5e8c81b0588961e360766736d10c3be1a",
            eventName = "Ragequit_v2"
        };
        public static TopicKV Withdrawal_v2 = new TopicKV
        {
            hash = "0x2717ead6b9200dd235aad468c9809ea400fe33ac69b5bfaa6d3e90fc922b6398",
            eventName = "Withdrawal_v2"
        };
    }
    class TopicKV
    {
        public string hash { get; set; }
        public string eventName { get; set; }
    }

}
