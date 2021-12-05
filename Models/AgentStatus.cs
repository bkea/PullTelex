namespace PullTelax.Models
{
    class AgentStatus
    {
        public int AgentId { get; set; }
        public string SessionId { get; set; }
        public int StatusCode { get; set; }
        public string StatusDesc { get; set; }
        public string ChangeReason { get; set; }
        public bool Availability { get; set; }
        public string DateChanged { get; set; }
        public int Duration { get; set; }
        public string EmailAddress { get; set; }

    }
}
