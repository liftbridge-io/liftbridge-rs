use liftbridge_rs::client::{Client, SubscriptionOptions};
use std::collections::HashMap;
use std::sync::RwLock;

#[actix_rt::main]
async fn main() -> () {
    let mut client = Client::new(vec!["localhost:9292"])
        .await
        .expect("Unable to connect");
    match client.create_stream("sub", "name").await {
        Err(LiftbridgeError::StreamExists) => println!("stream already exists"),
        Err(e) => panic!("error creating stream: {}", e),
        _ => println!("created stream"),
    }
    let mut stream = client
        .subscribe("name", SubscriptionOptions::default())
        .await
        .unwrap();
    client
        .publish("name", "test".as_bytes().to_vec())
        .await
        .unwrap();

    let msg = stream.next().await.unwrap().unwrap();
    println!("{}", String::from_utf8(msg.value).unwrap());
}
