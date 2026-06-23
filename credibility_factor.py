@dataclass(frozen=True)
class CredibilityInputs:
    gn_rol: float
    return_period: float
    full_credibility: float
    max_weighting: float


class CredibilityCalculator:

    def calculate_return_period(
        self,
        gn_rol: float,
    ) -> float:
        ...

    def calculate_full_credibility(
        self,
        return_period: float,
    ) -> float:
        ...

    def calculate_weighting(
        self,
        years: int,
        location_count: int,
        full_credibility: float,
        max_weighting: float,
    ) -> float:
        ...

  for band in location_bands:
    for year in range(4, 21):
        weighting = calculator.calculate_weighting(
            years=year,
            location_count=band_representative_value,
            full_credibility=full_credibility,
            max_weighting=max_weighting,
        )

# output:
{
    "10-49": {
        4: 0.25,
        5: 0.31,
        ...
    },
    "50-99": {
        ...
    }
}
