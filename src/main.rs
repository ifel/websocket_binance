use serde::Deserialize;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{collections::HashMap, sync::RwLock};

use tokio;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::StreamExt;
use std::time::Instant;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use serde::de;
use anyhow::Result;

mod consts;

type PricePair = (f32, f32);

fn deserialize_string_tuple_vec<'de, D>(deserializer: D) -> Result<Vec<PricePair>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct StringTupleVecVisitor(PhantomData<Vec<PricePair>>);

    impl <'de> de::Visitor<'de> for StringTupleVecVisitor {
        type Value = Vec<PricePair>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a vector of string tuples that can be parsed as f32 and f32")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error> where A: de::SeqAccess<'de>, {
            let mut result = vec![];
            while let Some(tuple) = seq.next_element::<(&str, &str)>()? {
                let price = tuple.0.parse::<f32>().map_err(de::Error::custom)?;
                let quantity = tuple.1.parse::<f32>().map_err(de::Error::custom)?;
                result.push((price, quantity));
            }
            result.sort_by(|x, y| x.partial_cmp(y).expect("Failed to sort price pairs"));
            Ok(result)
        }
    }
    deserializer.deserialize_seq(StringTupleVecVisitor(PhantomData))
}

#[derive(Deserialize, Debug, Default)]
struct MessageData {
    // e: String,
    // E: u64,
    // T: u64,
    // s: String,
    // U: u64,
    // u: u64,
    // pu: u64,
    #[serde(deserialize_with = "deserialize_string_tuple_vec")]
    b: Vec<PricePair>,
    #[serde(deserialize_with = "deserialize_string_tuple_vec")]
    a: Vec<PricePair>,
}

#[derive(Deserialize, Debug)]
struct ReceivedMessage {
    stream: String,
    data: MessageData,
}

#[tokio::main]
async fn main() {
    let streams = consts::get_stream_names();
    let first_stream_name = streams[0];
    let map: Arc<HashMap<String, RwLock<MessageData>>> = Arc::new(
        streams
        .iter()
        .map(|stream| (
            stream.to_string(), RwLock::new(MessageData::default())
        )).collect()
    );
    let url = format!("wss://fstream.binance.com/stream?streams={}", streams.join("/"));

    let dump_counter = Arc::new(AtomicUsize::new(0));
    
    loop {
        let (ws_stream, _) = connect_async(&url).await.expect("Failed to connect");
        ws_stream.for_each(|message| {
            let map = map.clone();
            let dump_counter = dump_counter.clone();
            async move {
                let start = Instant::now();
                if let Ok(message) = message {
                    if let Err(err) = process_message(message, &map).await
                    {
                        println!("Error processing message: {:?}", err);
                    }
                };
                let duration = start.elapsed();
                println!("Time elapsed in process_message() is: {:?}", duration);
                let dump_counter_previous_value = dump_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if dump_counter_previous_value >= 100 {
                    if let Ok(val) = map[first_stream_name].read() {
                        println!("{first_stream_name} data {:#?}", val);
                    }
                    
                    dump_counter.store(0, std::sync::atomic::Ordering::Relaxed);
                }
            }}).await;
        println!("Stream is finished");
    }
}

async fn process_message(message: Message, map: &Arc<HashMap<String, RwLock<MessageData>>>) -> Result<(), Box<dyn Error + Send + Sync>>{
    // let data2_ = message.clone().into_text()?;
    // println!("{:?}", data2_);
    let data_ = message.into_data();
    let msg: ReceivedMessage = serde_json::from_slice(&data_)?;
    // println!("Received message: {:#?}", msg);
    if let Ok(mut val) = map[&msg.stream].write() {
        *val = msg.data;
    };
    Ok(())
}