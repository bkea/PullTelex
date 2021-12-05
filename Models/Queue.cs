namespace PullTelax.Models
{
    class Queue
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int StatusId { get; set; }
        public int Type { get; set; }
        public int MinimumWaitingSeconds { get; set; }
        public int AverageWaitingSeconds { get; set; }
        public int MaximumWaitingSeconds { get; set; }
    }
}
