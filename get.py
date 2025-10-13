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



from sqlalchemy import select
from sqlalchemy.orm import joinedload

class TechnicalAdjustmentRepository(BaseRepository[TechnicalAdjustment, int, CreateTechnicalAdjustment]):
    model = TechnicalAdjustment

    def list_with_related_fields(
        self,
        insurable_interest_set_id: int,
        policy_term_option_id: int,
    ) -> list[TechnicalAdjustment]:
        """List TechnicalAdjustments with joined model field and related data."""

        stmt = (
            select(self.model)
            .options(
                joinedload(self.model.adjustment_model_field)
                .joinedload(TechnicalAdjustmentModelField.technical_adjustment_field),
                joinedload(self.model.adjustment_model_field)
                .joinedload(TechnicalAdjustmentModelField.technical_adjustment_model_configuration),
            )
            .where(
                self.model.insurable_interest_set_id == insurable_interest_set_id,
                self.model.policy_term_option_id == policy_term_option_id,
            )
        )

        return self._session.execute(stmt).scalars().all()
    

# ----------------------------Paginated 
class TechnicalAdjustmentRepository(
    BaseRepository[TechnicalAdjustment, int, CreateTechnicalAdjustment]
):
    model = TechnicalAdjustment

    def list_with_related_fields_paged(
        self,
        insurable_interest_set_id: int,
        policy_term_option_id: int,
        page: int = 1,
        page_size: int = 50,
    ) -> PaginatedResponse[TechnicalAdjustment]:
        """
        List TechnicalAdjustments with joined model field and related data, paginated.
        """

        # Base filter conditions
        filters = [
            self.model.insurable_interest_set_id == insurable_interest_set_id,
            self.model.policy_term_option_id == policy_term_option_id,
        ]

        # Convert page number for offset calculation
        page = max(page, 1)
        offset = (page - 1) * page_size

        # ---- Count total items ----
        total_items = (
            self._session.execute(
                select(func.count()).select_from(self.model).where(*filters)
            )
            .scalars()
            .one()
        )

        # ---- Fetch paginated records with joined loads ----
        stmt = (
            select(self.model)
            .options(
                joinedload(self.model.adjustment_model_field)
                .joinedload(TechnicalAdjustmentModelField.technical_adjustment_field),
                joinedload(self.model.adjustment_model_field)
                .joinedload(TechnicalAdjustmentModelField.technical_adjustment_model_configuration),
            )
            .where(*filters)
            .limit(page_size)
            .offset(offset)
        )

        records = self._session.execute(stmt).scalars().all()

        # ---- Pagination metadata ----
        meta = PaginatedMeta(
            total_items=total_items,
            total_pages=math.ceil(total_items / page_size) if page_size else 1,
            page_number=page,
            page_size=page_size,
        )

        return PaginatedResponse(
            meta=meta,
            records=records,
        )


@router.get("/technical-adjustments", response_model=PaginatedResponse[TechnicalAdjustmentSchema])
def list_technical_adjustments(
    insurable_interest_set_id: int,
    policy_term_option_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, le=200),
    repo: TechnicalAdjustmentRepository = Depends(get_repository(TechnicalAdjustmentRepository)),
):
    return repo.list_with_related_fields_paged(
        insurable_interest_set_id=insurable_interest_set_id,
        policy_term_option_id=policy_term_option_id,
        page=page,
        page_size=page_size,
    )    

@router.get(
    "/technical-adjustments",
    response_model=PaginatedResponse[TechnicalAdjustmentRead],  # Optional, if you have schema
)
def list_technical_adjustments(
    insurable_interest_set_id: int,
    policy_term_option_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, le=200),
    repo: TechnicalAdjustmentRepository = Depends(get_repository(TechnicalAdjustmentRepository)),
):
    # ORM-level data
    paged = repo.list_with_related_fields_paged(
        insurable_interest_set_id=insurable_interest_set_id,
        policy_term_option_id=policy_term_option_id,
        page=page,
        page_size=page_size,
    )

    results = []
    for adj in paged.records:
        results.append({
            "technicalAdjustmentId": adj.id,
            "model_name": adj.adjustment_model_field.technical_adjustment_model_configuration.model_name,
            "insurableInterestSetId": adj.insurable_interest_set_id,
            "policyTermOptionId": adj.policy_term_option_id,
            "quoteOptionId": adj.quote_option_id,
            "assetTypes": adj.asset_types or [],
            "appliesTo": adj.applies_to,
            "perils": adj.perils or [],
            "insuredValueTypes": adj.insured_value_types or [],
            "adjustmentTypeIdentifierCode": adj.adjustment_model_field.technical_adjustment_field.adjustment_type_identifier_code,
            "adjustmentValue": adj.adjustment_value,
            "adjustmentReason": adj.adjustment_reason,
            "reasonCategory": adj.reason_category,
        })

    return PaginatedResponse(
        meta=paged.meta,
        records=results,
    )