from crawler.pipeline import orchestrator
from crawler.pipeline.types import OperatorInfo, ClassifyResult

operator = OperatorInfo(
    name="Amigo Tours Rome",
    country="Italy",
    city="Rome",
    url="https://amigotours.com/",
)

r: ClassifyResult = orchestrator.run(operator)

print(r.model_dump_json(indent=2))