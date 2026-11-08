"""Static validation of Task 7 package (no Docker needed)."""
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]

def test_load_balancer_config():
    s=(ROOT/'task7/nginx/loadbalancer.conf').read_text()
    assert 'gateway:8000' in s and 'gateway2:8000' in s
    assert 'least_conn;' in s

def test_queue_jobs_use_real_maintenance_endpoint():
    s=(ROOT/'task7/queue/worker.py').read_text()
    assert '/task4/maintenance/run' in s and 'basic_ack' in s
    assert 'basic_nack' in s

def test_kubernetes_6_replicas_with_pvcs_and_probes():
    s=(ROOT/'k8s/04-storage-statefulset.yaml').read_text()
    assert 'replicas: 6' in s and 'volumeClaimTemplates:' in s
    assert 'readinessProbe:' in s and 'livenessProbe:' in s

def test_metrics_exporter():
    s=(ROOT/'task7/monitoring/metrics.py').read_text()
    assert 'storage_node_healthy' in s and 'storage_repair_shards_latest' in s

def test_ha_is_isolated():
    s=(ROOT/'compose.task7-ha.yaml').read_text()
    assert 'patroni1' in s and 'patroni2' in s and 'patroni3' in s
    assert 'etcd1' in s and 'etcd2' in s and 'etcd3' in s
