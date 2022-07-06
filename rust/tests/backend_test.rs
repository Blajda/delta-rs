mod common;

use common::TestContext;
use futures::Stream;
use futures::stream::StreamExt;


#[tokio::test]
// Validate vacuum works on a non-partitioned table
async fn backend_list_obj() {
    let mut context = TestContext::from_env().await;
    let backend = context.new_storage();

    let uri = context.config.get("URI").unwrap();

    backend.put_obj(&(uri.to_string() + "/first_level"), "data".as_ref()).await.unwrap();
    backend.put_obj(&(uri.to_string() + "/dir/second_level"), "data2".as_ref()).await.unwrap();
    backend.put_obj(&(uri.to_string() + "/dir/second_level2"), "data3".as_ref()).await.unwrap();

    let mut objects = Vec::new();    
    let uri_proper = uri.to_string() + "/";
    let mut objs = backend.list_objs(&uri_proper).await.unwrap();
    while let Some(Ok(obj)) = objs.next().await {
        objects.push(obj)
    }

    println!("{:?}", objects);
    assert_eq!(objects.len(), 2);
}