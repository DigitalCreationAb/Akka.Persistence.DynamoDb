using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Persistence.Journal;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Akka.Persistence.DynamoDb.Journal
{
    public class EventDocument
    {
        private readonly IImmutableDictionary<string, AttributeValue> _attributes;

        public EventDocument(Document document)
        {
            _attributes = document.ToAttributeMap().ToImmutableDictionary();
        }

        public string? PersistenceId => GetStringValue(Keys.PersistenceId);

        public long SequenceNumber => GetLongValue(Keys.SequenceNumber);

        public string? Manifest => GetStringValue(Keys.Manifest);

        public string? WriterGuid => GetStringValue(Keys.WriterGuid);

        public long Timestamp => GetLongValue(Keys.Timestamp);

        public long HighestSequenceNumber => GetLongValue(Keys.HighestSequenceNumber);

        public Type? Type => Type.GetType(GetStringValue(Keys.Type) ?? "System.Object");
        
        public IPersistentRepresentation ToPersistent(ActorSystem system)
        {
            var serializer = system.Serialization.FindSerializerFor(Type);

            try
            {
                var payload = serializer.FromBinary(GetAttributeValue(Keys.Payload, item => item.B.ToArray()), Type);

                return new Persistent(
                    payload ?? new object(),
                    SequenceNumber,
                    PersistenceId,
                    Manifest,
                    sender: ActorRefs.NoSender,
                    writerGuid: WriterGuid,
                    timestamp: Timestamp);
            }
            catch (Exception e)
            {
                throw new Exception($"Failed deserializing event {SequenceNumber} from {PersistenceId}", e);
            }
        }

        public static (IImmutableList<Document> docs, IImmutableList<string> tags) ToDocument(
            IPersistentRepresentation persistentRepresentation, 
            ActorSystem system)
        {
            var item = persistentRepresentation;

            var tags = new List<string>();

            if (item.Payload is Tagged tagged)
            {
                item = item.WithPayload(tagged.Payload);
                            
                tags.AddRange(tagged.Tags);
            }
            
            var type = item.Payload.GetType();
            var serializer = system.Serialization.FindSerializerForType(type);
            var payload = serializer.ToBinary(item.Payload);
            var timestamp = item.Timestamp > 0 ? item.Timestamp : DateTime.UtcNow.Ticks;

            var docs = new List<Document>
            {
                new(new Dictionary<string, DynamoDBEntry>
                {
                    [Keys.GroupKey] = GetEventGroupKey(item.PersistenceId),
                    [Keys.SequenceNumber] = item.SequenceNr,
                    [Keys.PersistenceId] = item.PersistenceId,
                    [Keys.Manifest] = item.Manifest,
                    [Keys.WriterGuid] = item.WriterGuid,
                    [Keys.Timestamp] = timestamp,
                    [Keys.Type] = $"{type.FullName}, {type.Assembly.GetName().Name}",
                    [Keys.Payload] = payload,
                    [Keys.DocumentType] = DocumentTypes.Event
                })
            };

            docs.AddRange(tags.Select(tag => new Document(new Dictionary<string, DynamoDBEntry>
            {
                [Keys.GroupKey] = GetTagGroupKey(tag, item.PersistenceId),
                [Keys.SequenceNumber] = item.SequenceNr,
                [Keys.PersistenceId] = item.PersistenceId,
                [Keys.Timestamp] = timestamp,
                [Keys.DocumentType] = DocumentTypes.TagRef,
                [Keys.Tag] = tag
            })));
            
            return (docs.ToImmutableList(), tags.ToImmutableList());
        }

        public static Document ToHighestSequenceNumberDocument(string persistenceId, long highestSequenceNumber)
        {
            return new(new Dictionary<string, DynamoDBEntry>
            {
                [Keys.GroupKey] = GetHighestSequenceNumberGroupKey(persistenceId),
                [Keys.SequenceNumber] = 0L,
                [Keys.HighestSequenceNumber] = highestSequenceNumber,
                [Keys.DocumentType] = DocumentTypes.HighestSequenceNumber,
                [Keys.PersistenceId] = persistenceId
            });
        }

        public static string GetEventGroupKey(string persistenceId) => $"event-{persistenceId}";
        
        public static string GetTagGroupKey(string tag, string persistenceId) => $"tag-{tag}-{persistenceId}";
        
        public static string GetHighestSequenceNumberGroupKey(string persistenceId) => $"highestsequencenumber-{persistenceId}";

        private string? GetStringValue(string key)
        {
            return GetAttributeValue(key, item => item.S);
        }

        private long GetLongValue(string key)
        {
            return GetAttributeValue(key, item => long.TryParse(item.N, out var value) ? value : 0);
        }
        
        private T? GetAttributeValue<T>(string key, Func<AttributeValue, T> parse)
        {
            return _attributes.ContainsKey(key) ? parse(_attributes[key]) : default;
        }
        
        public static class Keys
        {
            public const string GroupKey = nameof(GroupKey);
            public const string PersistenceId = nameof(PersistenceId);
            public const string SequenceNumber = nameof(SequenceNumber);
            public const string DocumentType = nameof(DocumentType);
            public const string Manifest = nameof(Manifest);
            public const string WriterGuid = nameof(WriterGuid);
            public const string Timestamp = nameof(Timestamp);
            public const string Type = nameof(Type);
            public const string Payload = nameof(Payload);
            public const string Tag = nameof(Tag);
            public const string HighestSequenceNumber = nameof(HighestSequenceNumber);
        }
        
        public static class DocumentTypes
        {
            public const string Event = nameof(Event);
            public const string TagRef = nameof(TagRef);
            public const string HighestSequenceNumber = nameof(HighestSequenceNumber);
        }
    }
}