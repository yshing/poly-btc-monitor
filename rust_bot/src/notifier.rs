use crate::signal::Signal;
use anyhow::Result;
use reqwest::Client;
use serde_json::json;

pub struct Notifier {
    telegram_bot_token: Option<String>,
    telegram_chat_id: Option<String>,
    webhook_url: Option<String>,
    http: Client,
}

impl Notifier {
    pub fn new(
        telegram_bot_token: Option<String>,
        telegram_chat_id: Option<String>,
        webhook_url: Option<String>,
    ) -> Self {
        Self {
            telegram_bot_token,
            telegram_chat_id,
            webhook_url,
            http: Client::new(),
        }
    }

    pub async fn notify(&self, signal: &Signal) -> Result<()> {
        let message = format_signal(signal);
        tracing::info!("SIGNAL: {}", serde_json::to_string(signal)?);
        tracing::info!("\n{}", message);

        if let (Some(token), Some(chat_id)) = (&self.telegram_bot_token,&self.telegram_chat_id) {
            if let Err(e) = self.send_telegram(token, chat_id, &message).await {
                tracing::error!("telegram send failed: {}", e);
            }
        }

        if let Some(url) = &self.webhook_url {
            if let Err(e) = self.send_webhook(url, signal).await {
                tracing::error!("webhook send failed: {}", e);
            }
        }

        Ok(())
    }

    async fn send_telegram(
        &self,
        token: &str,
        chat_id: &str,
        text: &str,
    ) -> Result<()> {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
        let payload = json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
        });

        let resp = self.http.post(&url).json(&payload).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("telegram API error: {}", body);
        }
        Ok(())
    }

    async fn send_webhook(&self,
        url: &str,
        signal: &Signal,
    ) -> Result<()> {
        let resp = self.http.post(url).json(signal).send().await?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("webhook error: {}", body);
        }
        Ok(())
    }
}

fn format_signal(signal: &Signal) -> String {
    format!(
        "🎯 *COPY SIGNAL* #{rank} ({conf})\n\
        👤 `{wallet}`\n\
        📊 {market}\n\
        ➡️ {direction} `{outcome}` @ ${price}\n\
        💰 Target: ${target} | Suggested copy: ${copy}\n\
        🔗 [tx](https://polygonscan.com/tx/{tx})",
        rank = signal.wallet_rank,
        conf = signal.wallet_confidence,
        wallet = signal.wallet,
        market = signal.market_slug,
        direction = signal.direction,
        outcome = signal.outcome,
        price = format!("{:.4}", signal.price),
        target = format!("{:.2}", signal.recommended_follow_size),
        copy = format!("{:.2}", signal.suggested_copy_size),
        tx = signal.tx_hash,
    )
}
