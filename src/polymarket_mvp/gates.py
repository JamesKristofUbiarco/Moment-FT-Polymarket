"""Explicit feasibility gates; estimates are never completion evidence."""
from dataclasses import dataclass, asdict

@dataclass(frozen=True)
class Projection:
    processing_seconds: float
    census_seconds: float
    training_seconds: float
    evaluation_seconds: float
    elapsed_seconds: float
    minimum_windows: int
    budget_windows: int
    full_file_benchmarks: int

    def report(self):
        result = asdict(self)
        total = sum((self.processing_seconds, self.census_seconds,
                     self.training_seconds, self.evaluation_seconds, self.elapsed_seconds))
        result.update(projected_hours=total / 3600,
                      performance_gate=self.full_file_benchmarks >= 3 and total <= 48 * 3600,
                      cardinality_gate=self.minimum_windows <= self.budget_windows)
        return result

    def require_full(self):
        r = self.report()
        if not r['performance_gate']:
            raise RuntimeError(f'FULL blocked pending measured alternatives: {r}')
        return r
