using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Query;

namespace Akka.Persistence.DynamoDb.Query
{
    public class DynamoDbReadJournalProvider : IReadJournalProvider
    {
        private readonly Config _config;

        public DynamoDbReadJournalProvider(ExtendedActorSystem system, Config config)
        {
            _config = config;
        }

        public IReadJournal GetReadJournal()
        {
            return new DynamoDbReadJournal(_config);
        }
    }
}