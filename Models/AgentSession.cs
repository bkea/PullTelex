namespace PullTelax.Models
{
    class AgentSession
    {
        public string SessionId { get; set; }
        public string StartDate { get; set; }
        public string EndDate { get; set; }
        public string ExitCode { get; set; }
        public string MediaTypeId { get; set; }
        public string TerminalId { get; set; }
        public string PhoneNumber { get; set; }
        public int SecondsNotset { get; set; }
        public int SecondsAvailable { get; set; }
        public int SecondsOnCall { get; set; }
        public int SecondsWrappingUp { get; set; }
        public int SecondsOnBreak { get; set; }
        public int SecondsDialingOut { get; set; }
        public int SecondsBusy { get; set; }
        public int SecondsOnACW { get; set; }
        public int AvailableCount { get; set; }
        public int CallCount { get; set; }
        public int WrapUpCount { get; set; }
        public int BreakCount { get; set; }
        public int DialOutCount { get; set; }
        public int BusyCount { get; set; }
        public int ACWCount { get; set; }
        public int MissedCallCount { get; set; }
        public int ThirdPartyTransferCount { get; set; }
        public int OnHoldCount { get; set; }
        public int SecondsOnHold { get; set; }
        public int IsForRetailOnly { get; set; }
        public int RingAllAttempts { get; set; }
        public int RingAllAttemptsNotAnswered { get; set; }
        public int RingAllAttemptsAnswered { get; set; }
    }
}
