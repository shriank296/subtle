"""Repositories for interacting with vault domain."""

import json
import logging
from collections.abc import Sequence
from copy import deepcopy
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import Table, and_, desc, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import DeclarativeBase, contains_eager

from app.database.repository import BaseRepository, BaseRepositoryCore
from app.vault import models as M
from app.vault import schema as S
from app.vault import types
from app.vault.mappings import ASSET_TYPE_TO_MAPPING

logger = logging.getLogger(__name__)

APPLIES_TO = "applies_to"
ASSET_TYPE = "asset_type"
BREAKDOWNS = "breakdowns"
COLUMNS = "columns"
CURRENCY_CODE = "currency_code"
EXCLUSIONS = "exclusions"
ID = "id"
INCLUSIONS = "inclusions"
INSURABLE_INTEREST_HASH = "insurable_interest_hash"
INSURABLE_INTEREST_ID = "insurable_interest_id"
INSURED_VALUES = "insured_values"
NON_ASSET = "non_asset"
POLICY_TERMS = "policy_terms"
PRICE = "price"
PRICING_INPUT_ID = "pricing_input_id"
QUOTE_OPTION_IDS = "quote_option_ids"
QUOTE_OPTIONS = "quote_options"
TECHNICAL_ADJUSTMENT_ID = "technical_adjustment_id"
TECHNICAL_ADJUSTMENTS = "technical_adjustments"
VALUE = "value"


def generate_content_based_id(exposure_list: list[dict]) -> UUID:
    """Generates a deterministic UUID based on the content of an exposure list of dicts.

    This function creates a deep copy of the input list, sorts the 'insured_values' list
    in each exposure dict by 'value' and 'currency' to ensure consistent ordering, serializes the list
    to a JSON string, and then generates a UUID5 using the NAMESPACE_URL and the serialized JSON.
    """

    exposure_dicts = deepcopy(exposure_list)

    for exposure in exposure_dicts:
        if (
            isinstance(exposure, dict)
            and INSURED_VALUES in exposure
            and exposure[INSURED_VALUES]
            and isinstance(exposure[INSURED_VALUES], list)
        ):
            # Sort insured_values by value and currencyCode.
            if isinstance(exposure[INSURED_VALUES][0], dict):
                exposure[INSURED_VALUES].sort(
                    key=lambda x: (x[VALUE], x[CURRENCY_CODE])
                )
            else:
                # Assume they are objects with .value and .currencyCode attributes
                exposure[INSURED_VALUES].sort(key=lambda x: (x.value, x.currency_code))

    exposure_json = json.dumps(exposure_dicts, default=str, indent=4)
    insurable_interest_hash = uuid5(NAMESPACE_URL, exposure_json)
    return insurable_interest_hash


class FXRatesRepository(BaseRepositoryCore[M.FXRate, types.FXRateId, S.FXRate]):
    """Repository for managing FX rates in the vault domain."""

    model = M.FXRate

    def add(
        self,
        input_model: list[S.FXRate],
        pricing_input_id: types.PricingInputId,
        flush: bool = False,
    ) -> list[M.FXRate] | None:
        """Add FX rates to the database."""
        return self.insert_fx_rates(
            input_model=input_model,
            pricing_input_id=pricing_input_id,
            flush=flush,
        )

    def insert_fx_rates(
        self,
        input_model: list[S.FXRate],
        flush: bool = False,
        pricing_input_id: types.PricingInputId | None = None,
    ) -> list[M.FXRate] | None:
        fx_rates: list[M.FXRate] = []
        for fx_rate in input_model:
            fx_rate_db_obj = M.FXRate(
                id=uuid4(),
                currency_code_from=fx_rate.currency_code_from,
                currency_code_to=fx_rate.currency_code_to,
                rate_of_exchange=fx_rate.rate_of_exchange,
                rate_type=fx_rate.rate_type.value,
                pricing_input_id=pricing_input_id,
            )
            fx_rates.append(fx_rate_db_obj)

            self._session.add(fx_rate_db_obj)

        if flush:
            self._session.flush()

        return fx_rates if fx_rates else None


class QuoteOptionRepository(
    BaseRepositoryCore[
        M.QuoteOptionInput, types.QuoteOptionsInputId, S.QuoteOptionInput
    ]
):
    """Repository for managing quote options in the vault domain."""

    model = M.QuoteOptionInput

    def add(
        self,
        input_model: S.QuoteOptionInput,
        pricing_input_id: types.PricingInputId,
        flush: bool = False,
    ) -> M.QuoteOptionInput:
        model_dict = input_model.model_dump(
            exclude={"catastrophe_model_results", "pricing_input_id"}, by_alias=False
        )

        # Map total_deductions_percentage to total_deductions_pct
        # input_model uses total_deductions_percentage
        if "total_deductions_percentage" in model_dict:
            model_dict["total_deductions_pct"] = model_dict.pop(
                "total_deductions_percentage"
            )

        _model = self.model(id=uuid4(), **model_dict, pricing_input_id=pricing_input_id)

        self._session.add(instance=_model, _warn=True)
        if flush:
            self._session.flush()

        return _model


class InsurableInterestsRepository(
    BaseRepositoryCore[
        M.InsurableInterest, types.PrivateExposureId, S.InsurableInterest
    ]
):
    model = M.InsurableInterest

    def add(
        self,
        input_model: list[S.InsurableInterest | S.OnshoreProperty] | S.NonAsset,
        insurable_interest_id: types.InsurableInterestId | None = None,
        pricing_input_id: types.PricingInputId | None = None,
        flush: bool = False,
    ) -> types.PrivateExposureId:
        """
        Add an object to the session.

        Returns:
            if not input_model:
                raise ValueError("input_model list is empty; cannot determine asset_type.")
            asset_type = input_model[0].asset_type
        """

        # Get asset_type from the appropriate source
        if isinstance(input_model, list):
            # For list inputs, convert each item to dict and pass the list
            model_data = [item.model_dump(by_alias=False) for item in input_model]
            if input_model and hasattr(input_model[0], "insured_values"):
                insured_values = input_model[0].insured_values
            else:
                insured_values = None
            asset_type = input_model[0].asset_type if input_model else "non_asset"
        else:
            asset_type = input_model.asset_type
            model_data = [input_model.model_dump(by_alias=False)]
            insured_values = getattr(input_model, "insured_values", None)

        _model_class_raw = ASSET_TYPE_TO_MAPPING.get(
            asset_type or NON_ASSET,
            ASSET_TYPE_TO_MAPPING[NON_ASSET],
        )["db_class"]
        _model_class = cast(type[DeclarativeBase], _model_class_raw)

        _insurable_interest_hash = generate_content_based_id(model_data)

        exposure_stmt = pg_insert(self.model).values(
            insurable_interest_id=insurable_interest_id,
            insurable_interest_hash=_insurable_interest_hash,
            asset_type=asset_type,
            insured_values=insured_values,
        )
        exposure_upsert_stmt = exposure_stmt.on_conflict_do_nothing(
            index_elements=[INSURABLE_INTEREST_HASH]
        )
        results = self._session.execute(exposure_upsert_stmt)

        exposure_subclass_insert_stmt = None

        if results.rowcount == 1:  # type: ignore
            # Get the first item for model_dump if input_model is a list
            model_for_dump = (
                input_model[0] if isinstance(input_model, list) else input_model
            )

            # Check if _model_class has __table__ attribute before using it
            if hasattr(_model_class, "__table__"):
                table = cast(Table, _model_class.__table__)
                exposure_subclass_insert_stmt = pg_insert(table).values(
                    insurable_interest_hash=_insurable_interest_hash,
                    **model_for_dump.model_dump(
                        exclude={
                            ID,
                            INSURED_VALUES,
                            ASSET_TYPE,
                            INSURABLE_INTEREST_ID,
                            INSURABLE_INTEREST_HASH,
                        },
                        by_alias=False,
                    ),
                )

                if asset_type != "non_asset":
                    sub_class_upsert_stmt = (
                        # TODO: added this as part of insurable interest fix - check if required and fails lint
                        exposure_subclass_insert_stmt.on_conflict_do_nothing(
                            index_elements=[INSURABLE_INTEREST_HASH]
                        )
                    )
                    self._session.execute(sub_class_upsert_stmt)

        join_statement = pg_insert(M.PricingInsurableInterest).values(
            insurable_interest_hash=_insurable_interest_hash,
            pricing_input_id=pricing_input_id,
        )

        self._session.execute(join_statement)

        if flush:
            self._session.flush()
        return cast(types.PrivateExposureId, _insurable_interest_hash)


class PricingInputRepository(
    BaseRepositoryCore[M.PricingInput, types.PricingRequestId, S.PricingInput]
):
    model = M.PricingInput

    def add(
        self,
        input_model: S.PricingInput,
        flush: bool = False,
        pricing_result_id: types.PricingResultId | None = None,
    ) -> M.PricingInput:
        """Add an object to the session"""

        _model = self.model(
            id=uuid4(),
            placement_id=input_model.placement_id,
            placement_inception_date=input_model.placement_inception_date,
            placement_expiry_date=input_model.placement_expiry_date,
            pricing_start_date=input_model.pricing_start_date,
            risk_id=input_model.risk_id,
            insurable_interest_set_id=input_model.insurable_interest_set.insurable_interest_set_id,
        )

        self._session.add(_model)
        if flush:
            self._session.flush()

        return _model


class PolicyTermOptionRepository(
    BaseRepositoryCore[M.PolicyTermOption, types.PolicyTermOptionId, S.PolicyTermOption]
):
    model = M.PolicyTermOption

    def add(
        self,
        input_model: S.PolicyTermOption,
        pricing_input_id: types.PricingInputId | None = None,
        flush: bool = True,
    ) -> M.PolicyTermOption:
        """Add an object to a session"""
        _model = self.model(
            **input_model.model_dump(
                by_alias=False,
                exclude={
                    ID,
                    POLICY_TERMS,
                    INCLUSIONS,
                    EXCLUSIONS,
                    "policy_term_option_id",
                },  # Exclude external identifier to avoid duplication
            ),
            id=uuid4(),
            policy_term_option_id=input_model.policy_term_option_id,
            pricing_input_id=pricing_input_id,
        )

        if input_model.policy_terms:
            for policy_term in input_model.policy_terms:
                pt_obj = M.PolicyTerm(
                    id=uuid4(), **policy_term.model_dump(by_alias=False)
                )  # Generate internal ID since schema no longer provides one
                self._session.add(instance=pt_obj, _warn=True)
                pt_obj.policy_term_option = _model

        if input_model.inclusions:
            for inclusion in input_model.inclusions:
                inclusion_db_obj = M.PolicyTermOptionInclusion(
                    id=uuid4(), **inclusion.model_dump(by_alias=False)
                )
                self._session.add(instance=inclusion_db_obj, _warn=True)
                inclusion_db_obj.policy_term_option = _model

        if input_model.exclusions:
            for exclusion in input_model.exclusions:
                exclusion_db_obj = M.PolicyTermOptionExclusion(
                    id=uuid4(), **exclusion.model_dump(by_alias=False)
                )
                self._session.add(instance=exclusion_db_obj, _warn=True)
                exclusion_db_obj.policy_term_option = _model

        self._session.add(instance=_model, _warn=True)

        if flush:
            self._session.flush()

        return _model


class TechnicalAdjustmentModelRepository(
    BaseRepositoryCore[
        M.TechnicalAdjustment, types.TechnicalAdjustmentId, S.TechnicalAdjustment
    ]
):
    """Repository for managing technical adjustments in the vault domain."""

    model = M.TechnicalAdjustment

    def add(
        self,
        input_model: S.TechnicalAdjustment,
        pricing_input_id: types.PricingInputId,
        flush: bool = False,
    ) -> M.TechnicalAdjustment:
        """Add an object to the session"""

        adjustment_model_dict = input_model.model_dump(
            exclude={APPLIES_TO, PRICING_INPUT_ID}, by_alias=False
        )

        tech_adjustment_id = uuid4()

        _model = self.model(
            id=tech_adjustment_id,
            pricing_input_id=pricing_input_id,
            **adjustment_model_dict,
        )

        for applies_to in input_model.applies_to:
            applies_to_db_obj = M.AppliesTo(
                **applies_to.model_dump(by_alias=False),
                id=uuid4(),
                technical_adjustment_id=tech_adjustment_id,
            )
            self._session.add(instance=applies_to_db_obj, _warn=True)
            applies_to_db_obj.technical_adjustment = _model

        self._session.add(instance=_model, _warn=True)

        if flush:
            self._session.flush()

        return _model


class LayerOutputRepository(
    BaseRepositoryCore[M.QuoteOptionOutput, types.LayerId, S.QuoteOptionOutput]
):
    model = M.QuoteOptionOutput

    def add(
        self,
        input_model: S.QuoteOptionOutput,
        flush: bool = False,
        model_result_id: types.ModelResultId | None = None,
    ) -> M.QuoteOptionOutput:
        """Add an object to the session"""
        layer_dict = input_model.model_dump(exclude={PRICE}, by_alias=False)
        _model = self.model(**layer_dict, model_result_id=model_result_id)
        self._session.add(instance=_model, _warn=True)
        if flush:
            self._session.flush()

        return _model


class ModelResultRepository(
    BaseRepository[M.ModelResult, types.ModelResultId, S.Result]
):
    model = M.ModelResult

    def add_modelresult(
        self,
        *,
        input_model: S.Result,
        pricing_result_id: types.PricingResultId,
        flush: bool = False,
    ) -> M.ModelResult:
        """Add an object to the session"""

        # Modified selection
        _model = self.model(
            **input_model.model_dump(
                exclude={
                    BREAKDOWNS,
                    QUOTE_OPTIONS,
                    TECHNICAL_ADJUSTMENTS,
                },
                by_alias=False,
            ),
            id=uuid4(),
            pricing_result_id=pricing_result_id,
        )

        # handle breakdowns
        for breakdown_detail in input_model.breakdowns:
            # Dimensions
            dimensions_dict = breakdown_detail.dimensions.model_dump(
                exclude={"applies_to"}, by_alias=False
            )
            breakdown_dimension_obj = M.BreakdownDimension(
                **dimensions_dict, id=uuid4()
            )

            # Applies To (was columns)
            for applies_to in breakdown_detail.dimensions.applies_to:
                applies_to_db_obj = M.BreakdownDimensionAppliesTo(
                    **applies_to.model_dump(by_alias=False), id=uuid4()
                )
                self._session.add(instance=applies_to_db_obj, _warn=True)
                applies_to_db_obj.breakdown_dimension = breakdown_dimension_obj

            # Facts
            for fact in breakdown_detail.facts:
                fact_db_obj = M.BreakdownFact(
                    **fact.model_dump(by_alias=False), id=uuid4()
                )
                self._session.add(instance=fact_db_obj, _warn=True)
                fact_db_obj.breakdown_dimension = breakdown_dimension_obj

            self._session.add(instance=breakdown_dimension_obj, _warn=True)
            breakdown_dimension_obj.model_results = _model

        # Quote Options Output
        _model.quote_options_output = [
            M.QuoteOptionOutput(id=uuid4(), **q_opt_ouput.model_dump(by_alias=False))
            for q_opt_ouput in input_model.quote_options
        ]

        self._session.add(instance=_model, _warn=True)
        if flush:
            self._session.flush()

        return _model


class PricingRequestRepository(
    BaseRepository[M.PricingRequest, types.PricingRequestId, S.PricingRequest]
):
    model = M.PricingRequest

    def add_pricingrequest(
        self,
        pricing_request_id: types.PricingRequestId,
        pricing_input_id: types.PricingInputId,
        expiring_pricing_input_id: types.PricingInputId | None,
        flush: bool = False,
    ) -> M.PricingRequest:
        """Add an object to the session"""

        _model = self.model(
            pricing_request_id=pricing_request_id,
            pricing_input_id=pricing_input_id,
            expiring_pricing_input_id=expiring_pricing_input_id,
        )

        self._session.add(instance=_model, _warn=True)
        if flush:
            self._session.flush()

        return _model

    def get_pricingrequest(
        self,
        placement_id: types.PlacementId,
        insurable_interest_set_id: types.InsurableInterestId,
        policy_term_option_id: types.PolicyTermOptionId,
    ) -> Sequence:
        """Get pricing request data with combined quote options information.

        Returns all unique quote options for the given combination with their most
        recent pricing request record.
        """
        statement = (
            select(
                M.PricingRequest.pricing_request_id,
                M.QuoteOptionInput.quote_option_id,
                M.QuoteOptionInput.expiring_quote_option_id,
                M.QuoteOptionInput.layer_id,
                M.QuoteOptionInput.premium_currency_code,
                M.QuoteOptionInput.limit_value,
                M.QuoteOptionInput.limit_currency_code,
                M.QuoteOptionInput.excess_value,
                M.QuoteOptionInput.excess_currency_code,
                M.QuoteOptionInput.total_deductions_pct.label(
                    "total_deductions_percentage"
                ),
                M.QuoteOptionInput.yoa,
                M.PricingRequest.created_at,
            )
            .join(
                M.PricingInput, M.QuoteOptionInput.pricing_input_id == M.PricingInput.id
            )
            .join(
                M.PricingRequest, M.PricingInput.id == M.PricingRequest.pricing_input_id
            )
            .join(
                M.PolicyTermOption,
                M.PricingInput.id == M.PolicyTermOption.pricing_input_id,
            )
            .where(
                M.PricingInput.placement_id == placement_id,
                M.PricingInput.insurable_interest_set_id == insurable_interest_set_id,
                M.PolicyTermOption.policy_term_option_id == policy_term_option_id,
            )
            .order_by(
                M.QuoteOptionInput.quote_option_id,
                desc(M.PricingRequest.created_at),
            )
            .distinct(M.QuoteOptionInput.quote_option_id)
        )

        return self._session.execute(statement).fetchall()

    def get_pricingresults(
        self,
        placement_id: types.PlacementId,
        insurable_interest_set_id: types.InsurableInterestId,
        policy_term_option_id: types.PolicyTermOptionId,
    ) -> Sequence[M.ModelResult]:
        """Get pricing response data with combined quote options information.

        Returns all unique quote options for the given combination with their most
        recent pricing request record. Filters breakdown_dimensions so only those
        without entries in breakdown_dimensions_applies_to are loaded.
        """

        latest_quote_results = (
            select(
                M.ModelResult.id.label("model_result_id"),
                M.QuoteOptionOutput.quote_option_id.label("quote_option_id"),
                M.PricingResult.created_at.label("pricing_result_created_at"),
            )
            .join(
                M.PricingResult, M.ModelResult.pricing_result_id == M.PricingResult.id
            )
            .join(
                M.PricingRequest,
                M.PricingRequest.pricing_request_id
                == M.PricingResult.pricing_request_id,
            )
            .join(
                M.PricingInput, M.PricingInput.id == M.PricingRequest.pricing_input_id
            )
            .join(
                M.PolicyTermOption,
                M.PricingInput.id == M.PolicyTermOption.pricing_input_id,
            )
            .join(
                M.QuoteOptionOutput,
                M.QuoteOptionOutput.model_result_id == M.ModelResult.id,
            )
            .where(
                M.PricingInput.placement_id == placement_id,
                M.PricingInput.insurable_interest_set_id == insurable_interest_set_id,
                M.PolicyTermOption.policy_term_option_id == policy_term_option_id,
            )
            .order_by(
                M.QuoteOptionOutput.quote_option_id,
                desc(M.PricingResult.created_at),
            )
            .distinct(M.QuoteOptionOutput.quote_option_id)
            .subquery()
        )

        statement = (
            select(M.ModelResult)
            .join(
                latest_quote_results,
                latest_quote_results.c.model_result_id == M.ModelResult.id,
            )
            .join(
                M.BreakdownDimension,
                M.BreakdownDimension.model_result_id == M.ModelResult.id,
            )
            .outerjoin(
                M.BreakdownDimensionAppliesTo,
                M.BreakdownDimensionAppliesTo.breakdown_dimension_id
                == M.BreakdownDimension.id,
            )
            .where(M.BreakdownDimensionAppliesTo.id.is_(None))
            .order_by(
                latest_quote_results.c.quote_option_id,
                desc(latest_quote_results.c.pricing_result_created_at),
            )
            .options(contains_eager(M.ModelResult.breakdown_dimensions))
        )

        return self._session.execute(statement).unique().scalars().all()


class PricingResultRepository(
    BaseRepository[M.PricingResult, types.PricingResultId, S.PricingResult]
):
    model = M.PricingResult

    def add_pricingresult(
        self,
        *,
        input_model: S.PricingResult,
        pricing_request_id: types.PricingRequestId,
        pricing_engine_version: str,
        is_primary: bool,
        source: str,
        created_by: str,
        flush: bool = False,
    ) -> M.PricingResult:
        """Add an object to the session"""

        _model = self.model(
            id=uuid4(),
            pricing_engine_version=pricing_engine_version,
            pricing_request_id=pricing_request_id,
            is_primary=is_primary,
            source=source,
            created_by=created_by,
        )

        self._session.add(instance=_model, _warn=True)
        if flush:
            self._session.flush()

        return _model

    def get_by_pricing_request_id(
        self, pricing_request_id: types.PricingRequestId
    ) -> M.PricingResult | None:
        statement = select(M.PricingResult).where(
            M.PricingResult.pricing_request_id == pricing_request_id,
            M.PricingResult.is_primary,
        )
        return self._session.execute(statement).scalars().one_or_none()

    def get_by_quote_option_id(
        self, quote_option_id: types.QuoteOptionId
    ) -> M.PricingResult | None:
        """GET PricingRequest by quote_option_id"""

        statement = (
            select(M.PricingResult)
            .join(
                M.PricingRequest,
                M.PricingRequest.pricing_request_id
                == M.PricingResult.pricing_request_id,
            )
            .join(
                M.PricingInput, M.PricingRequest.pricing_input_id == M.PricingInput.id
            )
            .join(
                M.QuoteOptionInput,
                M.PricingInput.id == M.QuoteOptionInput.pricing_input_id,
            )
            .where(
                and_(
                    M.QuoteOptionInput.quote_option_id == quote_option_id,
                    M.PricingResult.is_primary,
                )
            )
            .order_by(desc(M.PricingRequest.created_at))
            .limit(1)
        )

        return self._session.execute(statement).scalars().one_or_none()


def get_by_quote_option_id(
        self, quote_option_id: types.QuoteOptionId
    ) -> M.PricingResult | None:
        """GET PricingRequest by quote_option_id with eager loading of relationships."""

        statement = (
            select(M.PricingResult)
            .join(
                M.PricingRequest,
                M.PricingRequest.pricing_request_id
                == M.PricingResult.pricing_request_id,
            )
            .join(
                M.PricingInput, M.PricingRequest.pricing_input_id == M.PricingInput.id
            )
            .join(
                M.QuoteOptionInput,
                M.PricingInput.id == M.QuoteOptionInput.pricing_input_id,
            )
            .options(
                selectinload(M.PricingResult.pricing_requests)
                .selectinload(M.PricingRequest.pricing_input)
                .selectinload(M.PricingInput.quote_option_inputs),
                selectinload(M.PricingResult.pricing_requests)
                .selectinload(M.PricingRequest.pricing_input)
                .selectinload(M.PricingInput.technical_adjustments),
                selectinload(M.PricingResult.pricing_requests)
                .selectinload(M.PricingRequest.pricing_input)
                .selectinload(M.PricingInput.insurable_interests),
                selectinload(M.PricingResult.pricing_requests)
                .selectinload(M.PricingRequest.pricing_input)
                .selectinload(M.PricingInput.policy_term_option),
            )
            .where(
                and_(
                    M.QuoteOptionInput.quote_option_id == quote_option_id,
                    M.PricingResult.is_primary,
                )
            )
            .order_by(desc(M.PricingRequest.created_at))
            .limit(1)
        )

        return self._session.execute(statement).scalars().one_or_none()
