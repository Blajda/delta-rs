use super::TestContext;
use std::collections::HashMap;

pub async fn setup_s3_context() -> TestContext {
    let mut config = HashMap::new();
    config.insert("URI".to_owned(), "s3://deltars/".to_owned());
    config.insert("AWS_ENDPOINT_URL".to_owned(), "http://localhost:4566".to_owned());
    config.insert("AWS_REGION".to_owned(), "us-east-2".to_owned());
    config.insert("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned());
    config.insert("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned());

    TestContext {
        storage_context: None,
        config,
        ..TestContext::default()
    }
}