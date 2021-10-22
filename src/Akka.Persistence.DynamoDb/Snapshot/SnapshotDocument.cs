using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Akka.Actor;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Akka.Persistence.DynamoDb.Snapshot
{
    public class SnapshotDocument
    {
        private readonly IImmutableDictionary<string, AttributeValue> _attributes;

        public SnapshotDocument(Document document)
        {
            _attributes = document.ToAttributeMap().ToImmutableDictionary();
        }
        
        public string? PersistenceId => GetStringValue(Keys.PersistenceId);

        public long SequenceNumber => GetLongValue(Keys.SequenceNumber);

        public long Timestamp => GetLongValue(Keys.Timestamp);

        public Type? Type => Type.GetType(GetStringValue(Keys.Type) ?? "System.Object");

        public SelectedSnapshot ToSelectedSnapshot(ActorSystem system)
        {
            var serializer = system.Serialization.FindSerializerForType(Type);
            
            var payload = serializer.FromBinary(GetAttributeValue(Keys.Payload, item => item.B.ToArray()), Type);

            return new SelectedSnapshot(
                new SnapshotMetadata(PersistenceId, SequenceNumber, new DateTime(Timestamp)),
                payload);
        }
        
        public static Document ToDocument(SnapshotMetadata metadata, object snapshot, ActorSystem system)
        {
            var type = snapshot.GetType();
            var serializer = system.Serialization.FindSerializerForType(type);
            var payload = serializer.ToBinary(snapshot);
            
            return new Document(new Dictionary<string, DynamoDBEntry>
            {
                [Keys.PersistenceId] = metadata.PersistenceId,
                [Keys.SequenceNumber] = metadata.SequenceNr,
                [Keys.Timestamp] = metadata.Timestamp.Ticks,
                [Keys.Type] = $"{type.FullName}, {type.Assembly.GetName().Name}",
                [Keys.Payload] = payload
            });
        }
        
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
            public static string PersistenceId = nameof(PersistenceId);
            public static string SequenceNumber = nameof(SequenceNumber);
            public static string Timestamp = nameof(Timestamp);
            public static string Type = nameof(Type);
            public static string Payload = nameof(Payload);
        }
    }
}