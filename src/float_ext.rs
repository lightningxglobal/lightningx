/// Epsilon for all floating-point comparisons in the matching engine.
/// 1e-12 is smaller than any tradeable unit (minimum meaningful qty is 1e-8 on most exchanges).
pub const FLOAT_EPS: f64 = 1e-12;

/// Epsilon-aware comparison methods, extending f64 as a trait.
pub trait FloatExt {
    /// self ≈ other  (within epsilon)
    fn eq_eps(self, other: f64) -> bool;
    /// self < other  (strictly less, not epsilon-close)
    fn lt_eps(self, other: f64) -> bool;
    /// self > other  (strictly greater, not epsilon-close)
    fn gt_eps(self, other: f64) -> bool;
    /// self ≤ other  (not meaningfully greater than other)
    fn le_eps(self, other: f64) -> bool;
    /// self ≥ other  (not meaningfully less than other)
    fn ge_eps(self, other: f64) -> bool;
    /// |self| < epsilon  (near zero)
    fn near_zero(self) -> bool;
    /// self > epsilon  (meaningfully positive, not just floating-point noise)
    fn positive(self) -> bool;
    /// self > -epsilon  (non-negative within epsilon)
    fn nonneg(self) -> bool;
}

impl FloatExt for f64 {
    #[inline(always)]
    fn eq_eps(self, other: f64) -> bool { (self - other).abs() < FLOAT_EPS }
    #[inline(always)]
    fn lt_eps(self, other: f64) -> bool { other - self > FLOAT_EPS }
    #[inline(always)]
    fn gt_eps(self, other: f64) -> bool { self - other > FLOAT_EPS }
    #[inline(always)]
    fn le_eps(self, other: f64) -> bool { self - other < FLOAT_EPS }
    #[inline(always)]
    fn ge_eps(self, other: f64) -> bool { self - other > -FLOAT_EPS }
    #[inline(always)]
    fn near_zero(self) -> bool { self.abs() < FLOAT_EPS }
    #[inline(always)]
    fn positive(self) -> bool { self > FLOAT_EPS }
    #[inline(always)]
    fn nonneg(self) -> bool { self > -FLOAT_EPS }
}
