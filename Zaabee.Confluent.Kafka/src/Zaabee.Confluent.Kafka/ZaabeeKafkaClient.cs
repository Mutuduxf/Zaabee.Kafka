using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Zaabee.Confluent.Kafka
{
    public class ZaabeeKafkaClient
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public ZaabeeKafkaClient(ProducerConfig producerConfig, ConsumerConfig consumerConfig)
        {
            _producerConfig = producerConfig;
            _consumerConfig = consumerConfig;
        }

        public async Task PublishAsync<T>(string topic, T value, Action successful = null, Action fail = null)
        {
            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(_producerConfig).Build())
            {
                try
                {
                    var dr = await p.ProduceAsync(topic, new Message<Null, string> {Value = "test"});
                    successful?.Invoke();
                }
                catch (ProduceException<Null, string> e)
                {
                    fail?.Invoke();
                }
            }
        }

        public void Publish<T>(string topic, T value, Action successful = null, Action fail = null)
        {
            using (var p = new ProducerBuilder<Null, string>(_producerConfig).Build())
            {
                for (var i = 0; i < 100; i++)
                {
                    p.Produce(topic, new Message<Null, string> {Value = i.ToString()}, r =>
                    {
                        if (!r.Error.IsError) successful?.Invoke();
                        else fail?.Invoke();
                    });
                }

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public void Subscribe()
        {
            using (var c = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
            {
                c.Subscribe("my-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine(
                                $"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }
    }
}