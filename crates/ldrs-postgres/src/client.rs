use anyhow::Context;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use url::Url;

pub async fn create_connection(conn_url: &str) -> Result<tokio_postgres::Client, anyhow::Error> {
    let connector = TlsConnector::new().with_context(|| "Could not create TLS connector")?;
    let connector = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(conn_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

pub fn check_for_role(conn_str: &str) -> Result<(String, Option<String>), anyhow::Error> {
    let mut pg_url = Url::parse(conn_str)?;
    let role = pg_url
        .query_pairs()
        .find(|(k, _)| k == "role")
        .map(|(_, v)| v.into_owned());

    {
        let new_pairs: Vec<_> = pg_url
            .query_pairs()
            .filter(|(k, _)| k != "role")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        pg_url.query_pairs_mut().clear().extend_pairs(new_pairs);
        Ok((pg_url.to_string(), role))
    }
}
