from __future__ import annotations

import sys
from types import ModuleType

from market_pipeline.ops import provision


class Future:
    def result(self, timeout: float) -> None:
        assert timeout == 30


class Admin:
    created = []

    def __init__(self, settings):
        assert settings["bootstrap.servers"] == "kafka:9092"

    def create_topics(self, topics):
        self.created = topics
        Admin.created = topics
        return {topic.topic: Future() for topic in topics}


class Topic:
    def __init__(self, topic, *, num_partitions, replication_factor, config):
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config


def test_topic_configuration_uses_keyword_config(monkeypatch) -> None:
    admin_module = ModuleType("confluent_kafka.admin")
    admin_module.AdminClient = Admin
    admin_module.NewTopic = Topic
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", admin_module)
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("INPUT_TOPIC", "market.trades.v1")
    monkeypatch.setenv("DLQ_TOPIC", "market.trades.dlq.v1")
    monkeypatch.setenv("INPUT_PARTITIONS", "3")
    provision.create_topics()
    assert Admin.created[0].config["cleanup.policy"] == "delete"
    assert Admin.created[1].config["cleanup.policy"] == "compact,delete"
