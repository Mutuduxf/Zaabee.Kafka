namespace Zaabee.Confluent.Kafka;

public class KafkaClient : IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly IConsumer<Null, string> _consumer;
    private readonly ITextSerializer _serializer;

    public KafkaClient(string bootstrapServers, string groupId, ITextSerializer serializer)
    {
        _serializer = serializer;
        var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
    }

    public async Task PublishAsync<T>(string topic, T? value)
    {
        try
        {
            var result = await _producer.ProduceAsync(
                topic,
                new Message<Null, string> { Value = _serializer.ToText(value) }
            );
            Console.WriteLine($"Message delivered to {result.TopicPartitionOffset}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        }
    }

    public void Subscribe(
        string topic,
        Action<string> messageHandler,
        CancellationToken cancellationToken
    )
    {
        _consumer.Subscribe(topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(cancellationToken);
                if (consumeResult != null)
                {
                    messageHandler(consumeResult.Message.Value);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation, do nothing
        }
        finally
        {
            _consumer.Close();
        }
    }

    public void Dispose()
    {
        _producer.Dispose();
        _consumer.Dispose();
    }
}
