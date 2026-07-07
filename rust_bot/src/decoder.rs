use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Result};

pub const ORDER_FILLED_V2_TOPIC: &str =
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee";

pub const CTF_EXCHANGE_V2: &str = "0xe111180000d2663c0091e4f400237545b87b996b";
pub const USDC_DECIMALS: u64 = 6;

#[derive(Debug, Clone)]
pub struct DecodedTrade {
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub maker: String,
    pub taker: String,
    pub side: u8,
    pub token_id: String,
    pub maker_amount: f64,
    pub taker_amount: f64,
    pub share_size: f64,
    pub price: f64,
    pub usd_amount: f64,
}

/// Decode a V2 OrderFilled log from its raw topics and data.
/// Topics layout: [topic0, orderHash, maker, taker]
/// Data layout: side(uint8) | tokenId(uint256) | makerAmountFilled(uint256) | takerAmountFilled(uint256) | fee | builder | metadata
pub fn decode_v2_order_filled(
    tx_hash: B256,
    log_index: u64,
    block_number: u64,
    topics: &[alloy_primitives::FixedBytes<32>],
    data: &[u8],
) -> Result<DecodedTrade> {
    if topics.len() < 4 {
        bail!("OrderFilled needs 4 topics, got {}", topics.len());
    }

    let maker = Address::from_word(topics[2]).to_string().to_lowercase();
    let taker = Address::from_word(topics[3]).to_string().to_lowercase();

    if data.len() < 32 * 7 {
        bail!("OrderFilled data too short: {} bytes", data.len());
    }

    let side = parse_u8_at(data, 0)?;
    let token_id = U256::from_be_slice(&data[12..32]).to_string();
    let maker_amount_raw = U256::from_be_slice(&data[32..64]);
    let taker_amount_raw = U256::from_be_slice(&data[64..96]);

    let maker_amount = raw_to_usdc(maker_amount_raw);
    let taker_amount = raw_to_usdc(taker_amount_raw);

    let (share_size, usdc_amount) = if side == 0 {
        // maker BUY: maker pays USDC, receives shares
        (taker_amount, maker_amount)
    } else {
        // maker SELL: maker pays shares, receives USDC
        (maker_amount, taker_amount)
    };

    let price = if share_size > 0.0 {
        usdc_amount / share_size
    } else {
        0.0
    };

    Ok(DecodedTrade {
        tx_hash: tx_hash.to_string(),
        log_index,
        block_number,
        maker,
        taker,
        side,
        token_id,
        maker_amount,
        taker_amount,
        share_size,
        price,
        usd_amount: usdc_amount,
    })
}

fn parse_u8_at(data: &[u8], offset: usize) -> Result<u8> {
    let slice = data
        .get(offset + 31)
        .ok_or_else(|| anyhow::anyhow!("out of bounds reading u8"))?;
    Ok(*slice)
}

fn raw_to_usdc(raw: U256) -> f64 {
    let divisor = U256::from(10u64).pow(U256::from(USDC_DECIMALS));
    let whole = raw / divisor;
    let rem = raw % divisor;
    let whole_f: f64 = whole.to_string().parse().unwrap_or(0.0);
    let rem_f: f64 = rem.to_string().parse().unwrap_or(0.0);
    let divisor_f: f64 = 10f64.powi(USDC_DECIMALS as i32);
    whole_f + rem_f / divisor_f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_to_usdc() {
        let raw = U256::from(1_500_000u64);
        assert!((raw_to_usdc(raw) - 1.5).abs() < 1e-9);
    }
}
