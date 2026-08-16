import asyncio
import httpx
import json
import struct
import time
import zlib
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import threading

app = FastAPI(title="Babel Gateway")

# Global Counter untuk integer Request ID
INTERNAL_ID_COUNTER = 0
COUNTER_LOCK = threading.Lock()

def get_next_internal_id():
    global INTERNAL_ID_COUNTER
    with COUNTER_LOCK:
        INTERNAL_ID_COUNTER += 1
        return INTERNAL_ID_COUNTER

# --- 1. KONFIGURASI & REGISTRY ---
SERVICES = {
    "service-a": {
        "protocol": "http-json",
        "host": "http://service-a:8101",
        "capabilities": {"echo": "echo", "uppercase": "uppercase", "metadata": "metadata"}
    },
    "service-b": {
        "protocol": "tcp-frame-json",
        "host": "service-b",
        "port": 8201,
        "capabilities": {"echo": "ECHO", "uppercase": "UPPERCASE", "sum": "SUM", "reverse": "REVERSE", "metadata": "METADATA"}
    },
    "service-c": {
        "protocol": "udp-crc-json",
        "host": "service-c",
        "port": 8301,
        "capabilities": {"echo": 1, "sum": 2, "metadata": 3}
    }
}

START_TIME = time.time()

# --- 2. MODEL DATA (Sesuai Kontrak) ---
class ExecuteOptions(BaseModel):
    preferred_service: Optional[str] = None
    timeout_ms: Optional[int] = 2000

class ExecuteRequest(BaseModel):
    request_id: str
    operation: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    options: ExecuteOptions = Field(default_factory=ExecuteOptions)

def build_error(req_id: str, op: str, srv_id: Optional[str], code: str, msg: str, retryable: bool = False):
    return JSONResponse(status_code=503 if code == "BACKEND_TIMEOUT" or code == "UNAVAILABLE" else 500, content={
        "request_id": req_id,
        "status": "error",
        "service_id": srv_id,
        "operation": op,
        "result": None,
        "error": {
            "code": code,
            "message": msg,
            "retryable": retryable
        }
    })

def build_success(req_id: str, op: str, srv_id: str, result_val: Any):
    return {
        "request_id": req_id,
        "status": "success",
        "service_id": srv_id,
        "operation": op,
        "result": result_val if isinstance(result_val, dict) and "value" in result_val else {"value": result_val},
        "error": None
    }

# --- 3. ADAPTER PROTOKOL ---

async def handle_service_a(req_id: str, mapped_op: str, args: dict):
    url = f"{SERVICES['service-a']['host']}/execute"
    payload = {"request_id": req_id, "operation": mapped_op, "arguments": args}
    
    async with httpx.AsyncClient() as client:
        # Service A menggunakan header X-Request-ID untuk korelasi
        headers = {"X-Request-ID": req_id, "Content-Type": "application/json"}
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        # Normalisasi Response Service A (operation_result -> result)
        if "error" in data and data["error"]:
            raise Exception(data["error"].get("message", "Service A Error"))
        return data.get("operation_result", data.get("result", {}))

async def handle_service_b(req_id: str, mapped_op: str, args: dict):
    config = SERVICES['service-b']
    internal_id = get_next_internal_id()
    
    # Penyesuaian argumen untuk Service B (sum butuh numberList)
    if mapped_op == "SUM" and "values" in args:
        args = {"numberList": args["values"]}

    # Bikin JSON Payload
    payload_json = {"requestId": internal_id, "operation": mapped_op, "arguments": args}
    payload_bytes = json.dumps(payload_json).encode('utf-8')
    
    # Pack Header TCP (Magic, Version, Type, Length, RequestID) -> >H B B I Q (16 bytes)
    header = struct.pack('>H B B I Q', 0xBABE, 1, 1, len(payload_bytes), internal_id)
    frame = header + payload_bytes

    reader, writer = await asyncio.open_connection(config['host'], config['port'])
    try:
        writer.write(frame)
        await writer.drain()

        # Baca 16-byte header balasan
        resp_header = await reader.readexactly(16)
        magic, version, status, length, resp_req_id = struct.unpack('>H B B I Q', resp_header)
        
        # Baca sisa payload sesuai length
        resp_payload = await reader.readexactly(length)
        data = json.loads(resp_payload.decode('utf-8'))
        
        if data.get("errorData"):
            raise Exception(data["errorData"].get("message", "Service B Error"))
        
        # Normalisasi Response Service B
        result_data = data.get("resultData", {})
        val = result_data.get("numericResult", result_data.get("value"))
        return {"value": val} if val is not None else result_data

    finally:
        writer.close()
        await writer.wait_closed()


async def handle_service_c(req_id: str, mapped_op: int, args: dict):
    config = SERVICES['service-c']
    internal_id = get_next_internal_id()
    seq_num = 1 # Dummy sequence
    
    # Bikin JSON Payload
    payload_json = {"requestId": internal_id, "operation": mapped_op, "arguments": args}
    payload_bytes = json.dumps(payload_json).encode('utf-8')
    
    # Pack Header UDP (Magic, Ver, MsgType, ReqID, SeqNum, Opcode, Status, Length) -> >H B B I Q B B H (20 bytes)
    # MsgType 1 = Request, Opcode = mapped_op, Status = 0
    header = struct.pack('>H B B I Q B B H', 0xC0DE, 1, 1, internal_id, seq_num, mapped_op, 0, len(payload_bytes))
    packet = header + payload_bytes
    
    # Hitung CRC-32 dan pack sebagai unsigned int 4-byte di akhir
    crc_calc = zlib.crc32(packet) & 0xFFFFFFFF
    packet += struct.pack('>I', crc_calc)

    loop = asyncio.get_running_loop()
    
    class UDPClientProtocol(asyncio.DatagramProtocol):
        def __init__(self, on_con_lost, response_future):
            self.on_con_lost = on_con_lost
            self.response_future = response_future

        def connection_made(self, transport):
            self.transport = transport
            transport.sendto(packet)

        def datagram_received(self, data, addr):
            # Validasi CRC paket yang diterima
            if len(data) < 24:
                return # Paket rusak, biarkan timeout yang bekerja
            
            received_crc = struct.unpack('>I', data[-4:])[0]
            expected_crc = zlib.crc32(data[:-4]) & 0xFFFFFFFF
            
            if received_crc != expected_crc:
                # CRC Mismatch, buang paket
                return
                
            resp_header = data[:20]
            resp_payload = data[20:-4]
            
            data_json = json.loads(resp_payload.decode('utf-8'))
            self.response_future.set_result(data_json)
            self.transport.close()

        def error_received(self, exc):
            pass

        def connection_lost(self, exc):
            if not self.response_future.done():
                self.on_con_lost.set_result(True)

    on_con_lost = loop.create_future()
    response_future = loop.create_future()
    
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(on_con_lost, response_future),
        remote_addr=(config['host'], config['port'])
    )

    try:
        # Menunggu balasan maksimal 2 detik
        data_json = await asyncio.wait_for(response_future, timeout=2.0)
        
        if data_json.get("error"):
            raise Exception(data_json["error"].get("message", "Service C Error"))
        
        # Normalisasi
        result_data = data_json.get("result", {})
        return {"value": result_data.get("value")}
        
    finally:
        transport.close()
        
# --- 4. ENDPOINT GATEWAY ---

@app.post("/execute")
async def execute(req: ExecuteRequest):
    op = req.operation
    req_id = req.request_id
    pref_service = req.options.preferred_service
    timeout = req.options.timeout_ms / 1000.0 if req.options.timeout_ms else 2.0

    # Routing Logic: Cari service yang mendukung operasi ini
    selected_service = None
    mapped_op = None

    # Jika ada preferred service dan mendukung, gunakan itu
    if pref_service and pref_service in SERVICES and op in SERVICES[pref_service]["capabilities"]:
        selected_service = pref_service
        mapped_op = SERVICES[pref_service]["capabilities"][op]
    else:
        # Fallback: Cari service pertama yang mendukung
        for srv, config in SERVICES.items():
            if op in config["capabilities"]:
                selected_service = srv
                mapped_op = config["capabilities"][op]
                break
    
    if not selected_service:
        return build_error(req_id, op, None, "UNSUPPORTED_OPERATION", f"Operation {op} not supported")

    # Execution with Failure Isolation
    try:
        if selected_service == "service-a":
            task = handle_service_a(req_id, mapped_op, req.arguments)
        elif selected_service == "service-b":
            task = handle_service_b(req_id, mapped_op, req.arguments)
        elif selected_service == "service-c":
            task = handle_service_c(req_id, mapped_op, req.arguments)
        
        result_normalized = await asyncio.wait_for(task, timeout=timeout)
        return build_success(req_id, op, selected_service, result_normalized)

    except asyncio.TimeoutError:
        return build_error(req_id, op, selected_service, "BACKEND_TIMEOUT", "Backend did not respond within timeout", True)
    except Exception as e:
        return build_error(req_id, op, selected_service, "BACKEND_ERROR", str(e), False)

@app.get("/services")
async def get_services():
    services_list = []
    for srv_id, config in SERVICES.items():
        services_list.append({
            "service_id": srv_id,
            "protocol": config["protocol"],
            "status": "available",
            "capabilities": list(config["capabilities"].keys())
        })
    return {"services": services_list}

@app.get("/status")
async def get_status():
    uptime = int((time.time() - START_TIME) * 1000)
    return {
        "status": "ok",
        "gateway_id": "candidate-gateway-18224123", # Ganti dengan NIM kamu
        "uptime_ms": uptime,
        "backends": {srv: "available" for srv in SERVICES.keys()}
    }