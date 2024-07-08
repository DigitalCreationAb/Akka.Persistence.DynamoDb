using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.DynamoDb.Journal;
using Akka.Persistence.Query;
using Amazon.DynamoDBv2.DocumentModel;

namespace Akka.Persistence.DynamoDb.Query
{
    public class EventQueriesSource
    {
        private readonly Table _table;
        private readonly ActorSystem _actorSystem;

        public EventQueriesSource(Table table, ActorSystem actorSystem)
        {
            _table = table;
            _actorSystem = actorSystem;
        }

        public async IAsyncEnumerable<EventEnvelope> QueryAll(Offset from, bool finishWhenEndReached)
        {
            while (true)
            {
                var filter = new ScanFilter();

                filter.AddCondition(
                    EventDocument.Keys.Timestamp,
                    ScanOperator.GreaterThan,
                    from is Sequence seq ? seq.Value : 0);
                filter.AddCondition(
                    EventDocument.Keys.DocumentType,
                    ScanOperator.Equal,
                    EventDocument.DocumentTypes.Event);
            
                var scanSearch = _table.Scan(filter);

                while (!scanSearch.IsDone)
                {
                    var results = await scanSearch.GetNextSetAsync();

                    foreach (var evnt in results.Select(result => new EventDocument(result)).OrderBy(x => x.Timestamp))
                    {
                        var eventOffset = Offset.Sequence(evnt.Timestamp);

                        var eventData = evnt.ToPersistent(_actorSystem);
                        
                        yield return new EventEnvelope(
                            Offset.Sequence(evnt.Timestamp),
                            eventData.PersistenceId,
                            eventData.SequenceNr,
                            eventData.Payload,
                            evnt.Timestamp, 
                            Array.Empty<string>());

                        from = eventOffset;
                    }
                }
                
                if (finishWhenEndReached)
                    yield break;
                
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }
    }
}