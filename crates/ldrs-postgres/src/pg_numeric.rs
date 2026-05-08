//! Zero-allocation PG NUMERIC binary serializer.
//!
//! Takes an `i128` value + decimal scale and writes the PG NUMERIC wire format
//! directly to a `BytesMut`, using stack-allocated digit arrays.

use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

const SIGN_POS: u16 = 0x0000;
const SIGN_NEG: u16 = 0x4000;

#[derive(Debug, Clone, Copy)]
pub struct PgFixedNumeric {
    pub value: i128,
    pub scale: i16,
}

impl ToSql for PgFixedNumeric {
    #[inline]
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        write_numeric(self.value, self.scale, out);
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

#[inline]
fn write_numeric(value: i128, scale: i16, out: &mut BytesMut) {
    let dscale = scale.max(0) as u16;

    // Zero fast path
    if value == 0 {
        out.put_u16(0);
        out.put_i16(0);
        out.put_u16(SIGN_POS);
        out.put_u16(dscale);
        return;
    }

    let (sign, abs) = if value < 0 {
        (SIGN_NEG, value.unsigned_abs())
    } else {
        (SIGN_POS, value as u128)
    };

    let s = scale.max(0) as u32;
    let div_scale = 10u128.pow(s);
    let x_int = abs / div_scale;
    let x_frac = abs % div_scale;

    // Integer part — base-10000 groups, LSB-first
    let mut int_groups = [0i16; 10];
    let mut n = x_int;
    let mut int_count = 0usize;
    while n > 0 {
        int_groups[int_count] = (n % 10000) as i16;
        n /= 10000;
        int_count += 1;
    }

    // Fractional part — base-10000 groups, MSB-first
    // (group 0 is closest to the decimal point)
    let frac_count = ((s + 3) / 4) as usize;
    let mut frac_groups = [0i16; 10];
    for i in 0..frac_count {
        let group_end = ((i + 1) * 4).min(s as usize);
        let group_start = i * 4;
        let digit_count = group_end - group_start;
        let padding = 4 - digit_count;
        let remaining = s as usize - group_end;
        let shifted = x_frac / 10u128.pow(remaining as u32);
        let masked = shifted % 10u128.pow(digit_count as u32);
        frac_groups[i] = (masked * 10u128.pow(padding as u32)) as i16;
    }

    // Combined digit array in MSB→LSB order
    let mut combined = [0i16; 20];
    let mut total = 0usize;
    for i in (0..int_count).rev() {
        combined[total] = int_groups[i];
        total += 1;
    }
    for i in 0..frac_count {
        combined[total] = frac_groups[i];
        total += 1;
    }

    // Strip leading zeros (advance MSB pointer)
    let mut first = 0;
    while first < total && combined[first] == 0 {
        first += 1;
    }

    // Strip trailing zeros (retract LSB pointer)
    let mut last = total;
    while last > first && combined[last - 1] == 0 {
        last -= 1;
    }

    let ndigits = (last - first) as u16;
    // Weight = base-10000 exponent of the MSB digit.
    // In `combined`, index `int_count - 1` is 10000^0; each earlier index is one
    // higher exponent, each later index one lower.
    let weight = (int_count as i32) - 1 - (first as i32);

    out.put_u16(ndigits);
    out.put_i16(weight as i16);
    out.put_u16(sign);
    out.put_u16(dscale);
    for i in first..last {
        out.put_i16(combined[i]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected_bytes(
        ndigits: u16,
        weight: i16,
        sign: u16,
        scale: u16,
        digits: &[i16],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&scale.to_be_bytes());
        for d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    #[test]
    fn test_pg_numeric_serialization() {
        // (value, scale, expected_ndigits, expected_weight, expected_sign, expected_dscale, expected_digits)
        let cases: &[(i128, i16, u16, i16, u16, u16, &[i16])] = &[
            // Zero — empty digits array
            (0, 15, 0, 0, SIGN_POS, 15, &[]),
            // Clean integer at full scale: 3.000000000000000
            (3_000_000_000_000_000, 15, 1, 0, SIGN_POS, 15, &[3]),
            // Mid-range fractional: 468797.177024568000000
            (
                468_797_177_024_568_000_000,
                15,
                5,
                1,
                SIGN_POS,
                15,
                &[46, 8797, 1770, 2456, 8000],
            ),
            // Max-precision edge: 507531.111989867000000
            (
                507_531_111_989_867_000_000,
                15,
                5,
                1,
                SIGN_POS,
                15,
                &[50, 7531, 1119, 8986, 7000],
            ),
            // Negative sign path: -123.45
            (-12345, 2, 2, 0, SIGN_NEG, 2, &[123, 4500]),
            // Value < 1, negative weight: 0.005
            (5, 3, 1, -1, SIGN_POS, 3, &[50]),
        ];

        for (value, scale, ndigits, weight, sign, dscale, digits) in cases {
            let mut buf = BytesMut::new();
            PgFixedNumeric {
                value: *value,
                scale: *scale,
            }
            .to_sql(&Type::NUMERIC, &mut buf)
            .unwrap();
            let expected = expected_bytes(*ndigits, *weight, *sign, *dscale, digits);
            assert_eq!(
                &buf[..],
                &expected[..],
                "value={}, scale={}",
                value,
                scale
            );
        }
    }
}
