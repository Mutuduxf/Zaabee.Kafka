using System;

namespace Zaabee.Kafka.Abstractions
{
    public interface IZaabeeKafkaClient : IPublisher, ISubscriber, IDisposable
    {

    }
}