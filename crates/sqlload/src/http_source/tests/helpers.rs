/// The base URL the URL tests join paths under.
pub(crate) fn base() -> reqwest::Url {
    reqwest::Url::parse("https://api.shop.example/v1/").unwrap_or_else(|_| unreachable!())
}
