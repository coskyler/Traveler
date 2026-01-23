from crawler.pipeline import orchestrator
from crawler.pipeline.types import OperatorInfo, ClassifyResult

operator = OperatorInfo(
    name="Tuscany Taste Tour",
    country="Italy",
    city="Cecina",
    url="https://tuscanytastetour.com/"
)

r: ClassifyResult = orchestrator.run(operator)

print(r.model_dump_json(indent=2))