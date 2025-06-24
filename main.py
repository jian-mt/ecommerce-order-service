#!/usr/bin/env python3
"""
电商订单处理服务 - 可观测性测试应用
包含日志、指标和链路追踪的完整示例
"""

import asyncio
import json
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace, baggage
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
import requests


# 配置结构化日志
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        # 获取当前trace信息
        span = trace.get_current_span()
        trace_id = ""
        span_id = ""
        
        if span.get_span_context().is_valid:
            trace_id = format(span.get_span_context().trace_id, '032x')
            span_id = format(span.get_span_context().span_id, '016x')
        
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": "ecommerce-order-service",
            "version": "1.0.0",
            "trace_id": trace_id,
            "span_id": span_id,
        }
        
        # 添加额外的字段
        if hasattr(record, 'user_id'):
            log_obj['user_id'] = record.user_id
        if hasattr(record, 'order_id'):
            log_obj['order_id'] = record.order_id
        if hasattr(record, 'duration_ms'):
            log_obj['duration_ms'] = record.duration_ms
        if hasattr(record, 'error_code'):
            log_obj['error_code'] = record.error_code
            
        return json.dumps(log_obj, ensure_ascii=False)


# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())
logger.handlers = [handler]
logger.propagate = False

# 配置OpenTelemetry
resource = Resource.create({
    ResourceAttributes.SERVICE_NAME: "ecommerce-order-service",
    ResourceAttributes.SERVICE_VERSION: "1.0.0",
    ResourceAttributes.DEPLOYMENT_ENVIRONMENT: "production",
})

# 配置Trace Provider
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# 配置OTLP导出器 - 连接到Tempo
otlp_exporter = OTLPSpanExporter(
    endpoint="http://tempo-distributor.tempo:4317",  # Tempo的OTLP gRPC端点
    insecure=True
)

span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# 配置Prometheus指标
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

ORDER_PROCESSING_DURATION = Histogram(
    'order_processing_duration_seconds',
    'Order processing duration in seconds',
    ['status']
)

ACTIVE_ORDERS = Gauge(
    'active_orders_total',
    'Number of currently active orders'
)

INVENTORY_LEVEL = Gauge(
    'inventory_level',
    'Current inventory level',
    ['product_id']
)

# 数据模型
class OrderItem(BaseModel):
    product_id: str
    quantity: int
    price: float

class Order(BaseModel):
    user_id: str
    items: List[OrderItem]

class OrderResponse(BaseModel):
    order_id: str
    status: str
    total_amount: float
    estimated_delivery: str

# 模拟数据库和外部服务
class MockDatabase:
    def __init__(self):
        self.orders = {}
        self.inventory = {
            "prod_001": 100,
            "prod_002": 50,
            "prod_003": 200,
            "prod_004": 30,
            "prod_005": 75
        }
    
    async def save_order(self, order_id: str, order_data: dict):
        # 模拟数据库延迟
        await asyncio.sleep(random.uniform(0.01, 0.05))
        self.orders[order_id] = order_data
        logger.info("Order saved to database", extra={"order_id": order_id})
    
    async def check_inventory(self, product_id: str, quantity: int) -> bool:
        # 模拟数据库查询延迟
        await asyncio.sleep(random.uniform(0.005, 0.02))
        current_stock = self.inventory.get(product_id, 0)
        INVENTORY_LEVEL.labels(product_id=product_id).set(current_stock)
        return current_stock >= quantity
    
    async def update_inventory(self, product_id: str, quantity: int):
        await asyncio.sleep(random.uniform(0.01, 0.03))
        if product_id in self.inventory:
            self.inventory[product_id] -= quantity
            INVENTORY_LEVEL.labels(product_id=product_id).set(self.inventory[product_id])
            logger.info(f"Inventory updated for {product_id}", 
                       extra={"product_id": product_id, "remaining_stock": self.inventory[product_id]})

class PaymentService:
    @staticmethod
    async def process_payment(order_id: str, amount: float) -> bool:
        with tracer.start_as_current_span("payment_processing") as span:
            span.set_attribute("order_id", order_id)
            span.set_attribute("amount", amount)
            
            # 模拟支付处理延迟
            processing_time = random.uniform(0.1, 0.5)
            await asyncio.sleep(processing_time)
            
            # 10% 概率支付失败
            success = random.random() > 0.1
            
            if success:
                logger.info("Payment processed successfully", 
                           extra={"order_id": order_id, "amount": amount, "duration_ms": processing_time * 1000})
                span.set_attribute("payment.status", "success")
            else:
                logger.error("Payment processing failed", 
                            extra={"order_id": order_id, "amount": amount, "error_code": "PAYMENT_DECLINED"})
                span.set_attribute("payment.status", "failed")
                span.record_exception(Exception("Payment declined by bank"))
            
            return success

class NotificationService:
    @staticmethod
    async def send_notification(user_id: str, order_id: str, message: str):
        with tracer.start_as_current_span("send_notification") as span:
            span.set_attribute("user_id", user_id)
            span.set_attribute("order_id", order_id)
            span.set_attribute("notification.type", "order_update")
            
            # 模拟通知发送延迟
            await asyncio.sleep(random.uniform(0.02, 0.1))
            
            logger.info("Notification sent", 
                       extra={"user_id": user_id, "order_id": order_id, "message": message})

# 初始化服务
app = FastAPI(title="电商订单处理服务", version="1.0.0")
db = MockDatabase()

# 配置FastAPI的OpenTelemetry集成
FastAPIInstrumentor.instrument_app(app)
RequestsInstrumentor().instrument()
LoggingInstrumentor().instrument()

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    # 记录请求指标
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status_code=response.status_code
    ).inc()
    
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(process_time)
    
    response.headers["X-Process-Time"] = str(process_time)
    return response

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/orders", response_model=OrderResponse)
async def create_order(order: Order):
    order_id = str(uuid.uuid4())
    
    with tracer.start_as_current_span("create_order") as span:
        span.set_attribute("order_id", order_id)
        span.set_attribute("user_id", order.user_id)
        span.set_attribute("items_count", len(order.items))
        
        # 设置baggage用于跨服务传播
        baggage.set_baggage("user_id", order.user_id)
        baggage.set_baggage("order_id", order_id)
        
        start_time = time.time()
        ACTIVE_ORDERS.inc()
        
        try:
            logger.info("Processing new order", 
                       extra={"order_id": order_id, "user_id": order.user_id})
            
            # 1. 验证库存
            total_amount = 0.0
            with tracer.start_as_current_span("inventory_check") as inventory_span:
                for item in order.items:
                    inventory_span.add_event(f"Checking inventory for {item.product_id}")
                    
                    is_available = await db.check_inventory(item.product_id, item.quantity)
                    if not is_available:
                        logger.warning("Insufficient inventory", 
                                     extra={"order_id": order_id, "product_id": item.product_id, 
                                           "requested_quantity": item.quantity})
                        inventory_span.set_attribute("inventory.status", "insufficient")
                        raise HTTPException(status_code=400, detail=f"Insufficient inventory for {item.product_id}")
                    
                    total_amount += item.price * item.quantity
                
                inventory_span.set_attribute("inventory.status", "available")
            
            # 2. 处理支付
            payment_success = await PaymentService.process_payment(order_id, total_amount)
            if not payment_success:
                logger.error("Payment failed", 
                           extra={"order_id": order_id, "user_id": order.user_id, "error_code": "PAYMENT_FAILED"})
                span.set_attribute("order.status", "payment_failed")
                raise HTTPException(status_code=402, detail="Payment processing failed")
            
            # 3. 更新库存
            with tracer.start_as_current_span("update_inventory"):
                for item in order.items:
                    await db.update_inventory(item.product_id, item.quantity)
            
            # 4. 保存订单
            order_data = {
                "user_id": order.user_id,
                "items": [item.dict() for item in order.items],
                "total_amount": total_amount,
                "status": "confirmed",
                "created_at": datetime.utcnow().isoformat()
            }
            await db.save_order(order_id, order_data)
            
            # 5. 发送通知
            await NotificationService.send_notification(
                order.user_id, 
                order_id, 
                f"Your order {order_id} has been confirmed"
            )
            
            processing_time = time.time() - start_time
            ORDER_PROCESSING_DURATION.labels(status="success").observe(processing_time)
            
            logger.info("Order processed successfully", 
                       extra={"order_id": order_id, "user_id": order.user_id, 
                             "total_amount": total_amount, "duration_ms": processing_time * 1000})
            
            span.set_attribute("order.status", "success")
            span.set_attribute("order.total_amount", total_amount)
            
            return OrderResponse(
                order_id=order_id,
                status="confirmed",
                total_amount=total_amount,
                estimated_delivery="2024-01-15"
            )
            
        except HTTPException:
            processing_time = time.time() - start_time
            ORDER_PROCESSING_DURATION.labels(status="failed").observe(processing_time)
            span.set_attribute("order.status", "failed")
            raise
        except Exception as e:
            processing_time = time.time() - start_time
            ORDER_PROCESSING_DURATION.labels(status="error").observe(processing_time)
            logger.error("Unexpected error processing order", 
                        extra={"order_id": order_id, "error": str(e), "error_code": "INTERNAL_ERROR"})
            span.set_attribute("order.status", "error")
            span.record_exception(e)
            raise HTTPException(status_code=500, detail="Internal server error")
        finally:
            ACTIVE_ORDERS.dec()

@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    with tracer.start_as_current_span("get_order") as span:
        span.set_attribute("order_id", order_id)
        
        if order_id not in db.orders:
            logger.warning("Order not found", extra={"order_id": order_id})
            span.set_attribute("order.found", False)
            raise HTTPException(status_code=404, detail="Order not found")
        
        order_data = db.orders[order_id]
        logger.info("Order retrieved", extra={"order_id": order_id})
        span.set_attribute("order.found", True)
        
        return {
            "order_id": order_id,
            **order_data
        }

@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    with tracer.start_as_current_span("cancel_order") as span:
        span.set_attribute("order_id", order_id)
        
        if order_id not in db.orders:
            logger.warning("Attempted to cancel non-existent order", extra={"order_id": order_id})
            raise HTTPException(status_code=404, detail="Order not found")
        
        # 模拟取消处理
        await asyncio.sleep(random.uniform(0.05, 0.2))
        
        db.orders[order_id]["status"] = "cancelled"
        logger.info("Order cancelled", extra={"order_id": order_id})
        
        # 发送取消通知
        await NotificationService.send_notification(
            db.orders[order_id]["user_id"],
            order_id,
            f"Your order {order_id} has been cancelled"
        )
        
        span.set_attribute("order.status", "cancelled")
        return {"message": "Order cancelled successfully"}

# 生成测试流量的端点
@app.post("/generate-test-traffic")
async def generate_test_traffic(num_orders: int = 10):
    """生成测试订单流量"""
    results = []
    
    for i in range(num_orders):
        try:
            # 随机生成订单数据
            user_id = f"user_{random.randint(1000, 9999)}"
            items = []
            
            for _ in range(random.randint(1, 3)):
                items.append(OrderItem(
                    product_id=f"prod_{random.randint(1, 5):03d}",
                    quantity=random.randint(1, 5),
                    price=round(random.uniform(10.0, 100.0), 2)
                ))
            
            order = Order(user_id=user_id, items=items)
            
            # 创建订单
            response = await create_order(order)
            results.append({"status": "success", "order_id": response.order_id})
            
            # 随机延迟
            await asyncio.sleep(random.uniform(0.1, 0.5))
            
        except Exception as e:
            results.append({"status": "error", "error": str(e)})
    
    return {"generated_orders": len(results), "results": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=None)
    
