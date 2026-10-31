"""Standard-library integration tests: real JVMs, temporary durable data, isolated ports."""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def request(port_number, method='GET', path='/employees', data=None, headers=None):
    conn = http.client.HTTPConnection('127.0.0.1', port_number, timeout=8)
    hdr = dict(headers or {})
    if data is not None:
        hdr.setdefault('Content-Type', 'application/x-www-form-urlencoded')
    try:
        conn.request(method, path, data, hdr)
        res = conn.getresponse()
        return res.status, {k.lower(): v for k, v in res.getheaders()}, res.read().decode()
    finally:
        conn.close()


def eventually(check, timeout=12):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(.08)
    raise AssertionError('Condition did not become true before deadline')


class Processes:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.items = []
        self.files = []

    def start(self, label, *args, **env):
        log = open(Path(self.tmp.name) / (label + '.log'), 'ab', buffering=0)
        proc = subprocess.Popen(['java', '-cp', str(ROOT / 'target/classes'), 'md.utm.pad.Main', *map(str, args)],
                                stdout=log, stderr=subprocess.STDOUT, env={**os.environ, **env})
        self.items.append(proc)
        self.files.append(log)
        return proc

    def close(self):
        for proc in self.items:
            if proc.poll() is None:
                proc.terminate()
        for proc in self.items:
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in self.files:
            log.close()
        self.tmp.cleanup()


class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.group = Processes()
        self.addCleanup(self.group.close)
        self.storage_port, self.a, self.b, self.proxy = [port() for _ in range(4)]
        self.file = str(Path(self.group.tmp.name) / 'store.bin')
        self.storage = self.group.start('storage', 'storage', self.storage_port, self.file)
        eventually(lambda: request(self.storage_port, path='/health')[0] == 200)
        self.store_env = {'STORE_URL': f'http://127.0.0.1:{self.storage_port}'}
        self.dw1 = self.group.start('dw1', 'dw', self.a, 'dw-1', **self.store_env)
        self.dw2 = self.group.start('dw2', 'dw', self.b, 'dw-2', **self.store_env)
        for p in (self.a, self.b):
            eventually(lambda p=p: request(p, path='/health')[0] == 200)
        self.px = self.group.start('proxy', 'proxy', self.proxy,
            DW_ENDPOINTS=f'http://127.0.0.1:{self.a},http://127.0.0.1:{self.b}',
            HEALTH_INTERVAL_MS='150', CACHE_TTL_MS='1000')
        eventually(lambda: request(self.proxy, path='/health')[0] == 200)

    def test_cache_miss_hit_expiry_and_put_invalidation(self):
        first = request(self.proxy)
        second = request(self.proxy)
        self.assertEqual((first[0], first[1]['x-cache'], second[1]['x-cache']), (200, 'MISS', 'HIT'))
        time.sleep(1.15)
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'MISS')
        created = request(self.proxy, 'PUT', data='id=4&name=Alexandra')
        self.assertEqual(created[0], 201)
        self.assertEqual(created[1]['location'], '/employees?id=4')
        fresh = request(self.proxy)
        self.assertEqual(fresh[1]['x-cache'], 'MISS')
        self.assertIn({'id': 4, 'name': 'Alexandra'}, json.loads(fresh[2]))
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'HIT')
        self.assertEqual(request(self.proxy, 'PUT', data='id=4&name=New')[0], 200)
        self.assertIn('New', request(self.proxy)[2])

    def test_round_robin_and_independent_processes(self):
        self.assertEqual(len({p.pid for p in [self.storage, self.dw1, self.dw2, self.px]}), 4)
        rows = [request(self.proxy, headers={'Cache-Control': 'no-cache'}) for _ in range(4)]
        self.assertTrue(all(r[0] == 200 and r[1]['x-cache'] == 'BYPASS' for r in rows))
        nodes = [r[1]['x-warehouse'] for r in rows]
        self.assertNotEqual(nodes[0], nodes[1])
        self.assertEqual(nodes[:2], nodes[2:])

    def test_failover_reads_writes_and_node_recovery(self):
        # Establish the next RR target, then kill it (no graceful shutdown).
        first = request(self.proxy, headers={'Cache-Control': 'no-cache'})[1]['x-warehouse']
        victim = self.dw2 if first == 'dw-1' else self.dw1
        survivor = 'dw-1' if first == 'dw-1' else 'dw-2'
        victim.kill()
        victim.wait(timeout=5)
        self.assertEqual(request(self.proxy, headers={'Cache-Control': 'no-cache'})[0], 200)
        time.sleep(.5)
        for _ in range(4):
            r = request(self.proxy, headers={'Cache-Control': 'no-cache'})
            self.assertEqual((r[0], r[1]['x-warehouse']), (200, survivor))
        self.assertEqual(request(self.proxy, 'PUT', data='id=8&name=Failover')[0], 201)
        restored_port = self.b if victim is self.dw2 else self.a
        restored_name = 'dw-2' if victim is self.dw2 else 'dw-1'
        self.group.start('restored', 'dw', restored_port, restored_name, **self.store_env)
        eventually(lambda: request(self.proxy, headers={'Cache-Control': 'no-cache'})[1].get('x-warehouse') == restored_name)
        self.assertIn('Failover', request(restored_port, path='/employees?id=8')[2])

    def test_all_nodes_down(self):
        for proc in [self.dw1, self.dw2]:
            proc.kill()
            proc.wait()
        eventually(lambda: request(self.proxy, path='/health')[0] == 503)
        self.assertEqual(request(self.proxy, headers={'Cache-Control': 'no-cache'})[0], 503)

    def test_shared_data_and_persistence_after_storage_restart(self):
        self.assertEqual(request(self.proxy, 'PUT', data='id=9&name=Durable')[0], 201)
        for p in (self.a, self.b):
            self.assertEqual(json.loads(request(p, path='/employees?id=9')[2])['name'], 'Durable')
        self.storage.kill()
        self.storage.wait()
        self.storage = self.group.start('storage-restarted', 'storage', self.storage_port, self.file)
        eventually(lambda: request(self.storage_port, path='/health')[0] == 200)
        self.assertEqual(json.loads(request(self.b, path='/employees?id=9')[2])['name'], 'Durable')

    def test_correlation_sessions_and_keepalive(self):
        r = request(self.proxy, headers={'X-Correlation-Id': 'teacher-demo-123', 'Cache-Control': 'no-cache'})
        self.assertEqual(r[1]['x-correlation-id'], 'teacher-demo-123')
        cookie = r[1]['set-cookie'].split(';')[0]
        again = request(self.proxy, headers={'Cookie': cookie})
        self.assertEqual(again[1]['x-session-id'], r[1]['x-session-id'])
        self.assertEqual(int(again[1]['x-session-requests']), int(r[1]['x-session-requests']) + 1)
        logs = ''.join(p.read_text() for p in Path(self.group.tmp.name).glob('dw*.log'))
        self.assertIn('correlation=teacher-demo-123', logs)
        self.assertNotEqual(r[1]['x-correlation-id'], again[1]['x-correlation-id'])
        conn = http.client.HTTPConnection('127.0.0.1', self.proxy, timeout=5)
        try:
            conn.request('GET', '/employees')
            first = conn.getresponse(); first.read()
            original = conn.sock
            conn.request('GET', '/employees')
            second = conn.getresponse(); second.read()
            self.assertIsNotNone(original)
            self.assertIs(conn.sock, original)
        finally:
            conn.close()

    def test_formats_and_validation(self):
        for p in (self.proxy, self.a):
            for accept in ('text/csv', 'application/json;q=0,application/xml;q=0', 'application/*;q=0,*/*;q=1'):
                self.assertEqual(request(p, headers={'Accept': accept})[0], 406)
            r = request(p, headers={'Accept': 'application/json;q=0.3,application/xml;q=0.9'})
            self.assertIn('application/xml', r[1]['content-type'])
            self.assertIn('<employees>', r[2])
            self.assertEqual(request(p, 'PUT', data='id=50&name=A', headers={'Accept': 'text/csv'})[0], 406)
        self.assertEqual(request(self.proxy, 'PUT', data='id=-1&name=A')[0], 400)
        self.assertEqual(request(self.proxy, 'PUT', data='{}', headers={'Content-Type': 'application/json'})[0], 415)

    def test_post_pull_confirmation_and_persistent_proposal(self):
        self.assertEqual(request(self.proxy, 'POST', data='id=1&name=Proposed')[0], 202)
        self.assertEqual(json.loads(request(self.a, path='/employees?id=1')[2])['name'], 'Ana')
        changes = json.loads(request(self.b, path='/updates/employees?after=0')[2])
        self.assertEqual(changes[0]['employee']['name'], 'Proposed')
        self.storage.kill(); self.storage.wait()
        self.storage = self.group.start('storage-restart', 'storage', self.storage_port, self.file)
        eventually(lambda: request(self.storage_port, path='/health')[0] == 200)
        self.assertEqual(json.loads(request(self.a, path='/updates/employees?after=0')[2]), changes)
        self.assertEqual(request(self.b, 'PUT', data='id=1&name=Proposed')[0], 200)
        self.assertEqual(json.loads(request(self.a, path='/employees?id=1')[2])['name'], 'Proposed')
        self.assertEqual(json.loads(request(self.a, path='/updates/employees?after=1')[2]), [])

    def test_concurrent_requests(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            rows = list(pool.map(lambda _: request(self.proxy, headers={'Cache-Control': 'no-cache'}), range(24)))
        self.assertTrue(all(r[0] == 200 for r in rows))
        self.assertEqual({r[1]['x-warehouse'] for r in rows}, {'dw-1', 'dw-2'})


class CacheRaceTests(unittest.TestCase):
    """Controlled backend pauses requests to exercise interleavings deterministically."""
    def setUp(self):
        self.group = Processes(); self.addCleanup(self.group.close)
        self.write_entered = threading.Event(); self.release_write = threading.Event()
        self.read_entered = threading.Event(); self.release_read = threading.Event()
        self.value = 'old'; self.slow_write = False; self.slow_read = False
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                value = outer.value
                if self.path != '/health' and outer.slow_read:
                    outer.slow_read = False
                    outer.read_entered.set(); outer.release_read.wait(6)
                self.reply(value)

            def do_PUT(self):
                self.rfile.read(int(self.headers.get('Content-Length', 0)))
                if outer.slow_write:
                    outer.write_entered.set(); outer.release_write.wait(6)
                outer.value = 'new'
                self.reply('new')

            def reply(self, value):
                data = value.encode()
                self.send_response(200); self.send_header('Content-Type', 'text/plain')
                self.send_header('X-Warehouse', 'controlled'); self.send_header('Content-Length', str(len(data)))
                self.end_headers(); self.wfile.write(data)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.addCleanup(self.stop_server)
        self.proxy = port()
        self.group.start('proxy', 'proxy', self.proxy, DW_ENDPOINTS=f'http://127.0.0.1:{self.server.server_port}', HEALTH_INTERVAL_MS='200')
        eventually(lambda: request(self.proxy, path='/health')[0] == 200)

    def stop_server(self):
        self.release_write.set(); self.release_read.set()
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def test_slow_write_does_not_block_reads_or_cache_stale_result(self):
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'MISS')
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'HIT')
        self.slow_write = True
        with concurrent.futures.ThreadPoolExecutor() as pool:
            write = pool.submit(request, self.proxy, 'PUT', data='id=1&name=new')
            self.assertTrue(self.write_entered.wait(3))
            try:
                # It must finish while the write is still blocked at the backend.
                read = pool.submit(request, self.proxy).result(timeout=2)
                self.assertFalse(write.done())
                self.assertEqual((read[1]['x-cache'], read[2]), ('MISS', 'old'))
            finally:
                self.release_write.set()
            self.assertEqual(write.result()[0], 200)
        fresh = request(self.proxy)
        self.assertEqual((fresh[1]['x-cache'], fresh[2]), ('MISS', 'new'))
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'HIT')

    def test_old_read_finishing_after_write_cannot_repopulate_cache(self):
        self.slow_read = True
        with concurrent.futures.ThreadPoolExecutor() as pool:
            old = pool.submit(request, self.proxy)
            self.assertTrue(self.read_entered.wait(3))
            try:
                self.assertEqual(request(self.proxy, 'PUT', data='id=1&name=new')[0], 200)
            finally:
                self.release_read.set()
            self.assertEqual(old.result()[2], 'old')
        fresh = request(self.proxy)
        self.assertEqual((fresh[1]['x-cache'], fresh[2]), ('MISS', 'new'))
        self.assertEqual(request(self.proxy)[1]['x-cache'], 'HIT')


if __name__ == '__main__':
    unittest.main(verbosity=2)
