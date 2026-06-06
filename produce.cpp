#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <string>
#include <random>
#include <thread>
#include <chrono>

int main() {
    std::string brokers = "localhost:9092";
    std::string topic = "telemetry.raw";
    std::string errstr;

    // --- set up the producer ---
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    // --- random number setup ---
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> normal_temp(60.0, 80.0);
    std::uniform_real_distribution<> hot_temp(110.0, 140.0);
    std::uniform_real_distribution<> chance(0.0, 1.0);
    std::uniform_int_distribution<> device_pick(1, 4);

    // --- produce 50 readings ---
    for (int i = 0; i < 50; i++) {
        double temp = normal_temp(gen);
        if (chance(gen) < 0.15) {          // ~15% anomalies
            temp = hot_temp(gen);
        }

        int device_num = device_pick(gen);
        long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        // build the JSON string by hand
        std::string payload = "{\"device\": \"sensor-0" + std::to_string(device_num) +
                              "\", \"temp_c\": " + std::to_string(temp) +
                              ", \"ts\": " + std::to_string(ts) + "}";

        RdKafka::ErrorCode err = producer->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,          // let Kafka pick the partition
            RdKafka::Producer::RK_MSG_COPY,        // copy the payload
            const_cast<char *>(payload.c_str()),
            payload.size(),
            nullptr, 0,                            // no key
            0, nullptr);

        if (err != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Produce failed: " << RdKafka::err2str(err) << std::endl;
        } else {
            std::cout << "sent: " << payload << std::endl;
        }

        producer->poll(0);                         // serve delivery callbacks
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    producer->flush(5000);                         // wait up to 5s for delivery
    std::cout << "done -- 50 events sent" << std::endl;

    delete producer;
    delete conf;
    return 0;
}
