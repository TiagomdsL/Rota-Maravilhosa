# tracing.py - Implementação mínima com os 2 extras
import logging
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from fastapi import FastAPI

class TraceIdFilter(logging.Filter):
    def filter(self, record):
        try:
            span = trace.get_current_span()
            if span and span.get_span_context().is_valid:
                record.trace_id = format(span.get_span_context().trace_id, '032x')
            else:
                record.trace_id = 'no-trace'
        except Exception:
            record.trace_id = 'error'
        return True

def setup_tracing(app: FastAPI, service_name: str):
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = TracerProvider(resource=resource)
    
    exporter = OTLPSpanExporter(
        endpoint="http://otel-collector.observability.svc.cluster.local:4318/v1/traces",
        #insecure=True
    )
    
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()
    
    for handler in logging.root.handlers:
        handler.addFilter(TraceIdFilter())
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [trace_id=%(trace_id)s] - %(message)s'
    )
    
    print(f"✅ OpenTelemetry configurado para: {service_name}")

def get_current_span():
    return trace.get_current_span()