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
}
