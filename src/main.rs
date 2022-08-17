use anyhow::{Context, Result};
use reqwest::{header, Client};
use serde_json::Value;
use std::vec::Vec;
use url::Url;

fn gh_client() -> Result<Client> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        header::HeaderValue::from_static("application/vnd.github.v3+json"),
    );
    reqwest::Client::builder()
        .user_agent("rust reqwest")
        .default_headers(headers)
        .build()
        .context("Failed to make gh client")
}

fn fmt_url(repo: &String) -> Result<String> {
    let mut base = Url::parse("https://api.github.com")?;
    base.set_path(format!("repos/{}", repo).as_str());
    Ok(String::from(base.as_str()))
}

fn topics(
    client: Client,
    repos: Vec<String>,
) -> impl futures::Future<Output = Result<Vec<String>>> {
    use futures::{StreamExt as _, TryStreamExt as _};

    futures::stream::iter(repos.into_iter())
        .map(move |repo| {
            // ----
            // リクエスト処理
            // ----
            let client = client.clone();
            tokio::spawn(async move {
                println!(
                    "-start fetch json: {}: {:?}",
                    repo,
                    std::thread::current().id()
                );
                let res = client
                    .get(fmt_url(&repo)?)
                    .send()
                    .await
                    .with_context(|| format!("Failed to get info of repo: {}", repo))?;
                let res = match res.error_for_status() {
                    Ok(res) => res,
                    Err(err) => anyhow::bail!("Failed to get: {}: {}", repo, err),
                };
                let json: Value = res
                    .json()
                    .await
                    .with_context(|| format!("Failed to parse info of repo: {}", repo))?;
                println!(
                    "-end fetch json: {}: {:?}",
                    repo,
                    std::thread::current().id()
                );

                Ok((repo, json))
            })
        })
        .buffer_unordered(3) // リクエストの実行を並行化
        .map(|x| x?)
        .map(|v| {
            // ----
            // JSON 操作
            // ----
            tokio::task::spawn_blocking(move || {
                let (repo, json) = v?;
                println!(
                    "-start get \"topics\" from json: {}: {:?}",
                    repo,
                    std::thread::current().id()
                );
                let topics = json
                    .get("topics")
                    .with_context(|| format!("Failed to get topics: {}", repo))?
                    .as_array()
                    .with_context(|| format!("Failed to convert topics to array: {}", repo))?;
                let mut res = Vec::<String>::new();
                for v in topics.iter() {
                    let t =
                        String::from(v.as_str().with_context(|| {
                            format!("Failed to convert topic to str: {}", repo)
                        })?);
                    println!("-res: {}: {}", repo, t);
                    res.push(t);
                }
                println!(
                    "-end get \"topics\" from json: {}: {:?}",
                    repo,
                    std::thread::current().id()
                );
                Ok(res)
            })
        })
        .buffer_unordered(3) // JSON 操作の実行を並行(並列)化
        .map(|x| x?)
        .try_fold(std::vec::Vec::<String>::new(), |mut acc, x| async move {
            // ---
            // Vec へ保存
            // ---
            for t in x {
                acc.push(t);
            }
            Result::<Vec<String>>::Ok(acc)
        })
}

#[tokio::main]
async fn main() {
    let client = gh_client().unwrap();
    let repos = vec![
        "rust-lang-nursery/failure",
        "rust-lang-nursery/lazy-static.rs",
        "rust-lang/libc",
        "bitflags/bitflags",
        "rust-lang/log",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    match topics(client, repos).await {
        Ok(res) => println!("done: {:#?}", res),
        Err(err) => eprintln!("err {:?}", err),
    };
}
