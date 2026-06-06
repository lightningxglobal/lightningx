use anyhow::{Result, anyhow};

pub const AMOUNT_SCALE: i64 = 100_000_000;
const AMOUNT_SCALE_I128: i128 = AMOUNT_SCALE as i128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct AmountAtoms(i64);

impl AmountAtoms {
    pub const ZERO: Self = Self(0);

    pub const fn from_atoms(atoms: i64) -> Self {
        Self(atoms)
    }

    pub const fn atoms(self) -> i64 {
        self.0
    }

    /// Compatibility conversion for legacy f64 DB/API boundaries. Do not use
    /// inside matching, settlement, or risk arithmetic.
    pub fn from_f64_round(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(anyhow!("amount is not finite"));
        }
        let scaled = (value * AMOUNT_SCALE as f64).round();
        if scaled > i64::MAX as f64 || scaled < i64::MIN as f64 {
            return Err(anyhow!("amount overflows i64 atoms"));
        }
        Ok(Self(scaled as i64))
    }

    pub fn from_decimal_str(input: &str) -> Result<Self> {
        let s = input.trim();
        if s.is_empty() {
            return Err(anyhow!("empty amount"));
        }

        let (negative, body) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        if body.is_empty() {
            return Err(anyhow!("missing amount digits"));
        }

        let mut parts = body.split('.');
        let whole = parts.next().unwrap_or_default();
        let frac = parts.next();
        if parts.next().is_some() {
            return Err(anyhow!("amount has multiple decimal points"));
        }
        if whole.is_empty() && frac.unwrap_or_default().is_empty() {
            return Err(anyhow!("missing amount digits"));
        }
        if !whole.bytes().all(|b| b.is_ascii_digit()) {
            return Err(anyhow!("invalid whole amount digits"));
        }

        let mut atoms: i128 = 0;
        for b in whole.bytes() {
            atoms = atoms
                .checked_mul(10)
                .and_then(|v| v.checked_add((b - b'0') as i128))
                .ok_or_else(|| anyhow!("amount overflows i64 atoms"))?;
        }
        atoms = atoms
            .checked_mul(AMOUNT_SCALE_I128)
            .ok_or_else(|| anyhow!("amount overflows i64 atoms"))?;

        if let Some(frac) = frac {
            if frac.len() > 8 {
                return Err(anyhow!("amount has more than 8 decimal places"));
            }
            if !frac.bytes().all(|b| b.is_ascii_digit()) {
                return Err(anyhow!("invalid fractional amount digits"));
            }
            let mut frac_atoms: i128 = 0;
            for b in frac.bytes() {
                frac_atoms = frac_atoms * 10 + (b - b'0') as i128;
            }
            for _ in frac.len()..8 {
                frac_atoms *= 10;
            }
            atoms = atoms
                .checked_add(frac_atoms)
                .ok_or_else(|| anyhow!("amount overflows i64 atoms"))?;
        }

        if negative {
            atoms = -atoms;
        }
        if atoms > i64::MAX as i128 || atoms < i64::MIN as i128 {
            return Err(anyhow!("amount overflows i64 atoms"));
        }
        Ok(Self(atoms as i64))
    }

    pub fn to_decimal_string(self) -> String {
        let atoms = self.0 as i128;
        let negative = atoms < 0;
        let abs = if negative { -atoms } else { atoms };
        let whole = abs / AMOUNT_SCALE_I128;
        let frac = abs % AMOUNT_SCALE_I128;
        let mut out = if frac == 0 {
            whole.to_string()
        } else {
            let mut frac_s = format!("{frac:08}");
            while frac_s.ends_with('0') {
                frac_s.pop();
            }
            format!("{whole}.{frac_s}")
        };
        if negative {
            out.insert(0, '-');
        }
        out
    }

    /// Compatibility projection for legacy f64 API/SBE fields.
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / AMOUNT_SCALE as f64
    }

    /// Integer-domain product of two atom-scaled values (e.g. price × quantity
    /// → quote cost). Computed in i128 and divided back by the scale with
    /// round-half-up, so no float arithmetic is involved. Rejects negative
    /// inputs: money amounts on the settle path are always non-negative.
    pub fn checked_mul_scaled(self, rhs: AmountAtoms) -> Result<AmountAtoms> {
        if self.0 < 0 || rhs.0 < 0 {
            return Err(anyhow!("negative amount in scaled multiply"));
        }
        let wide = self.0 as i128 * rhs.0 as i128;
        let scaled = (wide + AMOUNT_SCALE_I128 / 2) / AMOUNT_SCALE_I128;
        if scaled > i64::MAX as i128 {
            return Err(anyhow!("amount overflows i64 atoms"));
        }
        Ok(AmountAtoms(scaled as i64))
    }

    pub fn checked_add(self, rhs: AmountAtoms) -> Result<AmountAtoms> {
        self.0
            .checked_add(rhs.0)
            .map(AmountAtoms)
            .ok_or_else(|| anyhow!("amount overflows i64 atoms"))
    }

    pub fn checked_sub(self, rhs: AmountAtoms) -> Result<AmountAtoms> {
        self.0
            .checked_sub(rhs.0)
            .map(AmountAtoms)
            .ok_or_else(|| anyhow!("amount overflows i64 atoms"))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AccountBalance {
    pub balance_atoms: i64,
    pub frozen_atoms: i64,
}

impl AccountBalance {
    pub const fn from_atoms(balance_atoms: i64, frozen_atoms: i64) -> Self {
        Self {
            balance_atoms,
            frozen_atoms,
        }
    }

    pub fn from_f64_round(balance: f64, frozen: f64) -> Result<Self> {
        Ok(Self {
            balance_atoms: AmountAtoms::from_f64_round(balance)?.atoms(),
            frozen_atoms: AmountAtoms::from_f64_round(frozen)?.atoms(),
        })
    }

    pub fn balance(self) -> f64 {
        AmountAtoms::from_atoms(self.balance_atoms).to_f64()
    }

    pub fn frozen(self) -> f64 {
        AmountAtoms::from_atoms(self.frozen_atoms).to_f64()
    }

    pub fn available(self) -> f64 {
        AmountAtoms::from_atoms(self.available_atoms()).to_f64()
    }

    pub const fn available_atoms(self) -> i64 {
        self.balance_atoms - self.frozen_atoms
    }

    pub fn try_freeze_atoms(&mut self, amount_atoms: i64) -> bool {
        if amount_atoms <= 0 {
            return true;
        }
        if self.available_atoms() >= amount_atoms {
            self.frozen_atoms += amount_atoms;
            true
        } else {
            false
        }
    }

    pub fn release_atoms(&mut self, amount_atoms: i64) {
        if amount_atoms <= 0 {
            return;
        }
        self.frozen_atoms = self.frozen_atoms.saturating_sub(amount_atoms);
    }

    pub fn apply_atoms_delta(&mut self, balance_delta_atoms: i64, frozen_release_atoms: i64) {
        self.balance_atoms = self
            .balance_atoms
            .saturating_add(balance_delta_atoms)
            .max(0);
        self.release_atoms(frozen_release_atoms);
    }
}

#[cfg(test)]
mod tests {
    use super::{AMOUNT_SCALE, AmountAtoms};

    #[test]
    fn parses_and_formats_decimal_amounts() {
        let cases = [
            ("0", 0, "0"),
            ("1", AMOUNT_SCALE, "1"),
            ("1.23", 123_000_000, "1.23"),
            ("0.00000001", 1, "0.00000001"),
            ("-12.34000000", -1_234_000_000, "-12.34"),
            ("+7.5", 750_000_000, "7.5"),
        ];

        for (raw, atoms, rendered) in cases {
            let amount = AmountAtoms::from_decimal_str(raw).unwrap();
            assert_eq!(amount.atoms(), atoms);
            assert_eq!(amount.to_decimal_string(), rendered);
        }
    }

    #[test]
    fn rejects_invalid_decimal_amounts() {
        for raw in ["", ".", "1.000000001", "1.2.3", "abc", "1x", "1.x"] {
            assert!(AmountAtoms::from_decimal_str(raw).is_err(), "raw={raw}");
        }
    }

    #[test]
    fn rounds_legacy_f64_at_boundary_only() {
        let amount = AmountAtoms::from_f64_round(0.1 + 0.2).unwrap();
        assert_eq!(amount.atoms(), 30_000_000);
        assert_eq!(amount.to_decimal_string(), "0.3");
    }

    #[test]
    fn scaled_multiply_stays_in_integer_domain() {
        let price = AmountAtoms::from_decimal_str("75893.61").unwrap();
        let qty = AmountAtoms::from_decimal_str("0.00123456").unwrap();
        // 75893.61 × 0.00123456 = 93.6952151616 → round-half-up at 1e-8
        let cost = price.checked_mul_scaled(qty).unwrap();
        assert_eq!(cost.atoms(), 9_369_521_516);

        // Values that lose precision in f64 must stay exact here.
        let big_price = AmountAtoms::from_decimal_str("90071992.54740993").unwrap();
        let one = AmountAtoms::from_decimal_str("1").unwrap();
        assert_eq!(
            big_price.checked_mul_scaled(one).unwrap().atoms(),
            big_price.atoms()
        );

        assert!(
            AmountAtoms::from_atoms(-1)
                .checked_mul_scaled(one)
                .is_err()
        );
        assert!(
            AmountAtoms::from_atoms(i64::MAX)
                .checked_mul_scaled(AmountAtoms::from_atoms(i64::MAX))
                .is_err()
        );
    }

    #[test]
    fn settle_math_conserves_funds() {
        // Conservation invariant: buyer pays cost + buy_fee, seller receives
        // cost - sell_fee, exchange keeps buy_fee + sell_fee. Net quote delta
        // across all three parties must be exactly zero in atoms.
        let cases = [
            ("75893.61", "0.00123456", "0.09369505", "0.04684753"),
            ("0.00000001", "0.00000001", "0", "0"),
            ("123456.789", "987.654321", "121.93", "60.96"),
            ("1", "1", "0.001", "0.001"),
        ];
        for (price, qty, buy_fee, sell_fee) in cases {
            let price = AmountAtoms::from_decimal_str(price).unwrap();
            let qty = AmountAtoms::from_decimal_str(qty).unwrap();
            let buy_fee = AmountAtoms::from_decimal_str(buy_fee).unwrap();
            let sell_fee = AmountAtoms::from_decimal_str(sell_fee).unwrap();

            let cost = price.checked_mul_scaled(qty).unwrap();
            let buyer_debit = cost.checked_add(buy_fee).unwrap();
            let seller_credit = cost.checked_sub(sell_fee).unwrap();
            let fee_revenue = buy_fee.checked_add(sell_fee).unwrap();

            assert_eq!(
                buyer_debit.atoms(),
                seller_credit.atoms() + fee_revenue.atoms(),
                "quote conservation violated for price={price:?} qty={qty:?}"
            );
        }
    }

    #[test]
    fn account_balance_freezes_and_releases_by_atoms() {
        let mut bal = super::AccountBalance::from_atoms(1_000_000_000, 100_000_000);
        assert!(bal.try_freeze_atoms(200_000_000));
        assert_eq!(bal.frozen_atoms, 300_000_000);
        assert!(!bal.try_freeze_atoms(800_000_001));
        bal.release_atoms(50_000_000);
        assert_eq!(bal.frozen_atoms, 250_000_000);
        bal.apply_atoms_delta(-100_000_000, 200_000_000);
        assert_eq!(bal.balance_atoms, 900_000_000);
        assert_eq!(bal.frozen_atoms, 50_000_000);
    }
}
