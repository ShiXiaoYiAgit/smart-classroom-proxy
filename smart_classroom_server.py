#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
智慧教室数据代理 - Render 部署版
功能：MQTT 实时订阅 + 官方 API 历史同步 + A_/M_ 配对 + HTTP API
============================================================
"""

import socket
import struct
import time
import random
import json
import threading
import os
import ssl
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from datetime import datetime, timedelta

# ========== 你的项目配置 ==========
# Render 会自动分配 PORT 环境变量，如果没有则默认用 5000
CFG = {
    "mqtt_host": "iot.dfrobot.com.cn",
    "mqtt_port": 1883,
    "iot_id": "zUyzwNQDR",
    "iot_pwd": "z8szwHwDgz",
    "topic": "rqMHQNQvR",
    "api_url": "https://api.dfrobot.work/easyiot/apiv3/messages/search",
    "api_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3ODg2MTczMDcsImlvdF9uYW1lIjoielV5endOUURSIiwibmJmIjoxNzg4NDQ0NTA3LCJ1c2VyX2lkIjoiNWNiMTE2MGQ2YmI3NGE0ZDgzMTc0MDI4YjAyNDM4OWIifQ.9X-w17LS0lI7nButk_0phPfUX64gPvmRFzhg6P4GoMg",
    "http_port": int(os.environ.get("PORT", 5000)),  # ← 关键修改：从环境变量读取端口
    "data_dir": "./iot_data",
    "max_messages": 5000,
    "retain_days": 0,
    "auto_save_interval": 60,
    "history_count": 1000,
}

# ========== 全局状态 ==========
messages = []
msg_lock = threading.Lock()
mqtt_connected = False
mqtt_sock = None
stop_flag = False
last_save_time = 0
known_msg_ids = set()
history_loaded = False


# ========== 官方 HTTP API 获取历史数据 ==========
def fetch_history_from_api():
    print("[历史] 🔄 正在从官方 API 获取历史数据...")
    payload = json.dumps({"topic": CFG["topic"], "count": CFG["history_count"]}).encode('utf-8')
    req = Request(CFG["api_url"], data=payload, method='POST', headers={
        'Content-Type': 'application/json;charset=UTF-8',
        'Authorization': CFG["api_token"],
        'Origin': 'https://iot.dfrobot.com.cn',
        'Referer': 'https://iot.dfrobot.com.cn/',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0.36',
    })
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urlopen(req, context=ctx, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            raw_messages = []
            if isinstance(data, list):
                raw_messages = data
            elif isinstance(data, dict):
                for key in ['messages', 'data', 'list', 'items', 'result', 'records', 'rows']:
                    if key in data:
                        raw_messages = data[key]
                        break
                if not raw_messages and 'message' in data:
                    raw_messages = [data]

            if raw_messages:
                sample = {k: v for k, v in raw_messages[0].items() if k not in ('message', 'payload', 'content')}
                print(f"[历史] API 返回样例: {json.dumps(sample, ensure_ascii=False)}")

            result = []
            for m in raw_messages:
                if not isinstance(m, dict):
                    continue
                raw_time = None
                time_field = None
                for tkey in ['time', 'timestamp', 'created_at', 'date', 'datetime', 'ts', 't',
                             'create_time', 'update_time', 'send_time', 'pub_time', 'publish_time', 'msg_time']:
                    if tkey in m and m[tkey] is not None:
                        raw_time = m[tkey]
                        time_field = tkey
                        break
                if raw_time is None:
                    for k, v in m.items():
                        if isinstance(v, str) and ('-' in v or ':' in v or 'T' in v) and len(v) >= 10:
                            raw_time = v
                            time_field = k
                            break
                        elif isinstance(v, (int, float)) and v > 1e9:
                            raw_time = v
                            time_field = k
                            break

                fmt_time = None
                if raw_time is not None:
                    try:
                        if isinstance(raw_time, (int, float)):
                            ts = raw_time / 1000 if raw_time > 1e10 else raw_time
                            fmt_time = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                        elif isinstance(raw_time, str):
                            raw_time = raw_time.strip()
                            formats = [
                                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ",
                                "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%dT%H:%M:%S.%fZ",
                                "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
                            ]
                            for fmt in formats:
                                try:
                                    dt = datetime.strptime(raw_time.replace('Z', '+00:00').split('+')[0], fmt)
                                    fmt_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                                    break
                                except:
                                    continue
                            if fmt_time is None:
                                fmt_time = raw_time
                    except:
                        fmt_time = str(raw_time)
                if fmt_time is None:
                    fmt_time = str(raw_time) if raw_time is not None else "未知时间"

                raw_msg = None
                for mkey in ['message', 'msg', 'content', 'payload', 'data', 'value', 'body', 'text', 'info', 'raw']:
                    if mkey in m and m[mkey] is not None:
                        raw_msg = m[mkey]
                        break
                if raw_msg is None:
                    raw_msg = json.dumps({k: v for k, v in m.items() if k != time_field})

                raw_topic = m.get('topic', CFG["topic"])
                result.append({"topic": raw_topic, "message": str(raw_msg), "time": fmt_time, "source": "api"})

            print(f"[历史] ✅ 成功获取 {len(result)} 条历史数据")
            return result
    except HTTPError as e:
        print(f"[历史] ❌ HTTP 错误 {e.code}: {e.reason}")
        try:
            print(f"[历史] 响应: {e.read().decode('utf-8')[:500]}")
        except:
            pass
        return []
    except URLError as e:
        print(f"[历史] ❌ 网络错误: {e.reason}")
        return []
    except Exception as e:
        print(f"[历史] ❌ 异常: {e}")
        return []


# ========== A_ / M_ 配对逻辑（Python 版） ==========
def pair_messages(msgs):
    """将 A_（光线）和 M_（音量）消息按时间差 <= 2秒配对"""
    a_list = []
    m_list = []
    for m in msgs:
        msg = str(m.get("message", "")).strip()
        t = m.get("time", "")
        if not msg or not t:
            continue
        if msg.startswith("A_"):
            try:
                val = float(msg[2:])
                a_list.append({"time": t, "value": val})
            except (ValueError, TypeError):
                pass
        elif msg.startswith("M_"):
            try:
                val = float(msg[2:])
                m_list.append({"time": t, "value": val})
            except (ValueError, TypeError):
                pass

    def parse_time(t):
        try:
            return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.min

    a_list.sort(key=lambda x: parse_time(x["time"]))
    m_list.sort(key=lambda x: parse_time(x["time"]))

    used = set()
    pairs = []
    for a in a_list:
        a_time = parse_time(a["time"])
        for i, m in enumerate(m_list):
            if i in used:
                continue
            m_time = parse_time(m["time"])
            diff = abs((a_time - m_time).total_seconds())
            if diff <= 2:
                used.add(i)
                later_time = a["time"] if a_time > m_time else m["time"]
                pairs.append({
                    "time": later_time,
                    "light": round(a["value"], 2),
                    "mic": round(m["value"], 2)
                })
                break

    pairs.sort(key=lambda x: parse_time(x["time"]), reverse=True)
    return pairs


# ========== 持久化存储 ==========
def ensure_data_dir():
    if not os.path.exists(CFG["data_dir"]):
        os.makedirs(CFG["data_dir"])

def get_data_file(topic):
    safe_topic = topic.replace("/", "_").replace("\\", "_")
    return os.path.join(CFG["data_dir"], f"{safe_topic}_history.json")

def save_messages():
    global last_save_time
    with msg_lock:
        data = {
            "topic": CFG["topic"],
            "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(messages),
            "messages": list(reversed(messages))
        }
    ensure_data_dir()
    filepath = get_data_file(CFG["topic"])
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        last_save_time = time.time()
        print(f"[存储] ✅ 已保存 {data['count']} 条消息")
        return True
    except Exception as e:
        print(f"[存储] ❌ 保存失败: {e}")
        return False

def load_messages():
    global messages, known_msg_ids, history_loaded
    filepath = get_data_file(CFG["topic"])
    if not os.path.exists(filepath):
        print("[存储] ℹ️ 无本地缓存")
        return 0
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        loaded = data.get("messages", [])
        if CFG["retain_days"] > 0:
            cutoff = datetime.now() - timedelta(days=CFG["retain_days"])
            loaded = [m for m in loaded if datetime.strptime(m["time"], "%Y-%m-%d %H:%M:%S") > cutoff]
        with msg_lock:
            messages = []
            known_msg_ids.clear()
            for m in loaded:
                msg_id = m["time"] + "|" + m["message"]
                if msg_id not in known_msg_ids:
                    known_msg_ids.add(msg_id)
                    messages.insert(0, m)
        print(f"[存储] ✅ 已加载 {len(messages)} 条本地缓存")
        history_loaded = True
        return len(messages)
    except Exception as e:
        print(f"[存储] ❌ 加载失败: {e}")
        return 0

def merge_history(api_messages):
    global history_loaded, known_msg_ids
    added = len(api_messages)
    with msg_lock:
        messages[:] = [m for m in messages if m.get("source") != "api"]
        known_msg_ids.clear()
        for m in messages:
            known_msg_ids.add(m["time"] + "|" + m["message"])
        for m in api_messages:
            messages.append(m)
            known_msg_ids.add(m["time"] + "|" + m["message"])
        def parse_time(m):
            try:
                return datetime.strptime(m["time"], "%Y-%m-%d %H:%M:%S")
            except:
                return datetime.min
        messages.sort(key=parse_time, reverse=True)
        while len(messages) > CFG["max_messages"]:
            old = messages.pop()
            known_msg_ids.discard(old["time"] + "|" + old["message"])
    history_loaded = True
    print(f"[历史] ✅ 合并完成，API 数据 {added} 条，当前共 {len(messages)} 条")
    return added

def auto_save_worker():
    while not stop_flag:
        time.sleep(CFG["auto_save_interval"])
        if not stop_flag:
            save_messages()


# ========== MQTT 协议 ==========
def encode_string(s):
    b = s.encode('utf-8') if isinstance(s, str) else s
    return struct.pack('!H', len(b)) + b

def encode_remaining_length(length):
    result = bytearray()
    while True:
        byte = length % 128
        length = length // 128
        if length > 0:
            byte |= 0x80
        result.append(byte)
        if length == 0:
            break
    return bytes(result)

def decode_remaining_length(sock):
    multiplier = 1
    value = 0
    while True:
        byte = ord(sock.recv(1))
        value += (byte & 127) * multiplier
        multiplier *= 128
        if (byte & 128) == 0:
            break
    return value

def read_mqtt_packet(sock):
    header = sock.recv(1)
    if not header:
        return None, None
    packet_type = header[0]
    remaining_len = decode_remaining_length(sock)
    payload = b''
    while len(payload) < remaining_len:
        chunk = sock.recv(remaining_len - len(payload))
        if not chunk:
            break
        payload += chunk
    return packet_type, payload

def build_connect_packet(client_id, username, password):
    payload = encode_string("MQTT") + b"\x04" + b"\xc2" + b"\x00\x3c"
    payload += encode_string(client_id) + encode_string(username) + encode_string(password)
    return b"\x10" + encode_remaining_length(len(payload)) + payload

def build_subscribe_packet(topic, packet_id=1):
    payload = struct.pack('!H', packet_id) + encode_string(topic) + b"\x00"
    return b"\x82" + encode_remaining_length(len(payload)) + payload

def build_pingreq_packet():
    return b"\xc0\x00"

def parse_publish_payload(payload, qos=0):
    topic_len = struct.unpack('!H', payload[0:2])[0]
    topic = payload[2:2+topic_len].decode('utf-8', errors='replace')
    offset = 2 + topic_len
    if qos > 0:
        offset += 2
    message = payload[offset:].decode('utf-8', errors='replace')
    return topic, message


# ========== MQTT 客户端线程 ==========
def mqtt_worker():
    global mqtt_connected, mqtt_sock, stop_flag
    client_id = f"pyproxy_{random.randint(1000,9999)}_{int(time.time())%10000}"
    packet_id = 1
    while not stop_flag:
        try:
            print(f"[MQTT] 正在连接 {CFG['mqtt_host']}:{CFG['mqtt_port']} ...")
            mqtt_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            mqtt_sock.settimeout(10)
            mqtt_sock.connect((CFG['mqtt_host'], CFG['mqtt_port']))
            mqtt_sock.settimeout(None)
            mqtt_sock.sendall(build_connect_packet(client_id, CFG['iot_id'], CFG['iot_pwd']))
            ptype, payload = read_mqtt_packet(mqtt_sock)
            if ptype != 0x20 or len(payload) < 2 or payload[1] != 0:
                mqtt_sock.close()
                time.sleep(5)
                continue
            print("[MQTT] ✅ CONNECT 成功")
            mqtt_connected = True
            mqtt_sock.sendall(build_subscribe_packet(CFG['topic'], packet_id))
            packet_id = (packet_id + 1) % 65536
            ptype, payload = read_mqtt_packet(mqtt_sock)
            if ptype == 0x90:
                print(f"[MQTT] ✅ SUBSCRIBE 成功")
            last_ping = time.time()
            while not stop_flag:
                mqtt_sock.settimeout(2.0)
                try:
                    ptype, payload = read_mqtt_packet(mqtt_sock)
                except socket.timeout:
                    ptype = None
                if ptype is None:
                    if time.time() - last_ping > 30:
                        mqtt_sock.settimeout(None)
                        mqtt_sock.sendall(build_pingreq_packet())
                        last_ping = time.time()
                    continue
                mqtt_sock.settimeout(None)
                if ptype & 0xF0 == 0x30:
                    qos = (ptype >> 1) & 0x03
                    topic, message = parse_publish_payload(payload, qos)
                    print(f"[MQTT] 📥 收到: [{topic}] {message}")
                    msg_id = time.strftime("%Y-%m-%d %H:%M:%S") + "|" + message
                    with msg_lock:
                        if msg_id not in known_msg_ids:
                            known_msg_ids.add(msg_id)
                            messages.insert(0, {
                                "topic": topic, "message": message,
                                "time": time.strftime("%Y-%m-%d %H:%M:%S"), "source": "mqtt"
                            })
                            while len(messages) > CFG["max_messages"]:
                                old = messages.pop()
                                known_msg_ids.discard(old["time"] + "|" + old["message"])
                elif ptype == 0xD0:
                    pass
                elif ptype == 0x20:
                    pass
                last_ping = time.time()
        except Exception as e:
            print(f"[MQTT] ❌ 异常: {e}")
            mqtt_connected = False
            try:
                mqtt_sock.close()
            except:
                pass
            if not stop_flag:
                time.sleep(5)
    print("[MQTT] 线程已停止")


# ========== HTTP 服务器 ==========
class RequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def _send_html(self, html, code=200):
        self.send_response(code)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))

    def do_GET(self):
        if self.path == '/':
            html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'project9.html')
            if os.path.exists(html_path):
                with open(html_path, 'r', encoding='utf-8') as f:
                    self._send_html(f.read())
            else:
                self._send_html(f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>智慧教室</title></head>
<body style="font-family:sans-serif;max-width:600px;margin:40px auto;padding:20px">
    <h1>⚠️ 前端文件未找到</h1>
    <p>请确保 <code>project9.html</code> 与本服务器文件位于同一目录。</p>
    <p>当前查找路径：<code>{html_path}</code></p>
    <hr>
    <p>API 状态：<a href="/api/status">/api/status</a></p>
    <p>配对数据：<a href="/api/paired">/api/paired</a></p>
</body>
</html>""")
        elif self.path == '/api/status':
            with msg_lock:
                count = len(messages)
                hist_count = sum(1 for m in messages if m.get("source") == "api")
                rt_count = sum(1 for m in messages if m.get("source") == "mqtt")
            self._send_json({
                "mqtt_connected": mqtt_connected,
                "topic": CFG["topic"],
                "message_count": count,
                "history_count": hist_count,
                "realtime_count": rt_count,
                "history_loaded": history_loaded,
                "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
            })
        elif self.path == '/api/messages':
            with msg_lock:
                self._send_json({"messages": messages[:500]})
        elif self.path == '/api/paired':
            with msg_lock:
                msgs = list(messages)
            pairs = pair_messages(msgs)
            latest = pairs[0] if pairs else None
            self._send_json({
                "success": True,
                "latest": latest,
                "history": pairs[:1000],
                "count": len(pairs),
                "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        if self.path == '/api/fetch_history':
            print("[API] 收到刷新历史数据请求")
            api_messages = fetch_history_from_api()
            if api_messages:
                added = merge_history(api_messages)
                save_messages()
                self._send_json({
                    "success": True,
                    "added": added,
                    "total": len(messages),
                    "message": f"成功获取并合并 {len(api_messages)} 条历史数据"
                })
            else:
                self._send_json({
                    "success": False,
                    "message": "从官方API获取历史数据失败，请检查Token是否过期"
                })
        elif self.path == '/api/save':
            success = save_messages()
            self._send_json({
                "success": success,
                "message": "保存成功" if success else "保存失败"
            })
        else:
            self._send_json({"error": "not found"}, 404)

    def log_message(self, format, *args):
        pass


# ========== 主程序 ==========
if __name__ == '__main__':
    print("=" * 60)
    print("  智慧教室数据代理 - Render 部署版")
    print("=" * 60)
    print(f"  MQTT 服务器 : {CFG['mqtt_host']}:{CFG['mqtt_port']}")
    print(f"  HTTP API    : {CFG['api_url']}")
    print(f"  Topic       : {CFG['topic']}")
    print(f"  数据目录    : {os.path.abspath(CFG['data_dir'])}")
    print(f"  HTTP 端口   : {CFG['http_port']}")
    print("=" * 60)

    load_messages()

    if not messages:
        print("[启动] 本地无缓存，正在从官方 API 获取历史数据...")
        api_messages = fetch_history_from_api()
        if api_messages:
            merge_history(api_messages)
            save_messages()
    else:
        print("[启动] 本地有缓存，正在同步最新历史数据...")
        api_messages = fetch_history_from_api()
        if api_messages:
            merge_history(api_messages)
            save_messages()

    mqtt_thread = threading.Thread(target=mqtt_worker, daemon=True)
    mqtt_thread.start()

    save_thread = threading.Thread(target=auto_save_worker, daemon=True)
    save_thread.start()

    server = HTTPServer(('0.0.0.0', CFG['http_port']), RequestHandler)
    print(f"[HTTP] 服务启动: http://0.0.0.0:{CFG['http_port']}")
    print("[系统] 按 Ctrl+C 停止\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[系统] 正在停止...")
        stop_flag = True
        save_messages()
        try:
            mqtt_sock.close()
        except:
            pass
        server.shutdown()
        print("[系统] 已停止")