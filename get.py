from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlalchemy.orm import Session, joinedload
from uuid import UUID
from app.db.session import get_db
from app.models import (
    TechnicalAdjustment,
    TechnicalAdjustmentModelField,
    TechnicalAdjustmentField,
    TechnicalAdjustmentModelConfiguration,
)
from app.schemas.technical_adjustment import TechnicalAdjustmentListResponse

router = APIRouter(prefix="/technical-adjustments", tags=["Technical Adjustments"])

@router.get(
    "",
    response_model=TechnicalAdjustmentListResponse,
    summary="List technical adjustments by insurable interest and policy term option",
    response_description="List of matching technical adjustments",
)
def get_technical_adjustments(
    insurable_interest_set_id: UUID = Query(..., description="Insurable interest set UUID"),
    policy_term_option_id: UUID = Query(..., description="Policy term option UUID"),
    db: Session = Depends(get_db),
):
    """
    Get all **Technical Adjustments** for a given
    `insurable_interest_set_id` and `policy_term_option_id`.

    Joins related tables to include model name and adjustment code.
    """

    adjustments = (
        db.query(TechnicalAdjustment)
        .options(
            joinedload(TechnicalAdjustment.model_field)
            .joinedload(TechnicalAdjustmentModelField.field),
            joinedload(TechnicalAdjustment.model_field)
            .joinedload(TechnicalAdjustmentModelField.model_configuration),
        )
        .filter(
            TechnicalAdjustment.insurable_interest_set_id == insurable_interest_set_id,
            TechnicalAdjustment.policy_term_option_id == policy_term_option_id,
        )
        .all()
    )

    if not adjustments:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No technical adjustments found for given parameters",
        )

    results = []
    for adj in adjustments:
        results.append({
            "technicalAdjustmentId": adj.id,
            "model_name": adj.model_field.model_configuration.model_name,
            "insurableInterestSetId": adj.insurable_interest_set_id,
            "policyTermOptionId": adj.policy_term_option_id,
            "quoteOptionId": adj.quote_option_id,
            "assetTypes": adj.asset_types or [],
            "appliesTo": adj.applies_to,
            "perils": adj.perils or [],
            "insuredValueTypes": adj.insured_value_types or [],
            "adjustmentTypeIdentifierCode": adj.model_field.field.adjustment_type_identifier_code,
            "adjustmentValue": adj.adjustment_value,
            "adjustmentReason": adj.adjustment_reason,
            "reasonCategory": adj.reason_category,
        })

    return {"technical_adjustments": results}