from pydantic import BaseModel, Field
from uuid import UUID
from typing import List, Optional, Any

class TechnicalAdjustmentResponse(BaseModel):
    technicalAdjustmentId: UUID = Field(..., alias="technical_adjustment_id")
    model_name: str
    insurableInterestSetId: UUID
    policyTermOptionId: UUID
    quoteOptionId: UUID
    assetTypes: List[str]
    appliesTo: Optional[Any]
    perils: List[str]
    insuredValueTypes: List[str]
    adjustmentTypeIdentifierCode: str
    adjustmentValue: float
    adjustmentReason: str
    reasonCategory: str

    class Config:
        populate_by_name = True
        json_schema_extra = {
            "example": {
                "technicalAdjustmentId": "bd6c8a44-3621-4d93-bd12-80c451d82d8e",
                "model_name": "v0022025001",
                "insurableInterestSetId": "9f22a18b-fc52-4b74-82a9-f09c5d64f693",
                "policyTermOptionId": "5f4b9c9e-4372-4b65-8b32-5f7f3de0ce0e",
                "quoteOptionId": "f62a43f8-2fda-48aa-9b8d-f1ffbcf2027a",
                "assetTypes": ["onshore_property"],
                "appliesTo": None,
                "perils": ["Fire"],
                "insuredValueTypes": [],
                "adjustmentTypeIdentifierCode": "ModelToTechnical",
                "adjustmentValue": 2.0,
                "adjustmentReason": "Fire to tech added",
                "reasonCategory": "Policy Coverage",
            }
        }

class TechnicalAdjustmentListResponse(BaseModel):
    technical_adjustments: List[TechnicalAdjustmentResponse]


from typing import List

class TechnicalAdjustmentListResponse(PaginatedResponse[TechnicalAdjustmentResponse]):
    # The API should return "technicalAdjustments" instead of "records"
    technical_adjustments: List[TechnicalAdjustmentResponse]

    class Config:
        populate_by_name = True  # allows using both 'records' and alias fields

    def model_dump(self, **kwargs):
        """Override dump so 'records' → 'technicalAdjustments'."""
        data = super().model_dump(**kwargs)
        data["technicalAdjustments"] = data.pop("records", [])
        return data
    

@app.get("/technical-adjustments", response_model=TechnicalAdjustmentListResponse)
def list_technical_adjustments():
    meta = PaginatedMeta(
        page_number=1,
        page_size=1000,
        total_items=1153,
        total_pages=2,
    )

    records = []  # list of TechnicalAdjustmentResponse

    return TechnicalAdjustmentListResponse(meta=meta, records=records)    