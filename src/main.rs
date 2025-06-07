use serde::Deserialize;

use tokio;
use std::time::Instant;
use std::fmt;
use std::marker::PhantomData;
use std::thread;
use serde::de;
use anyhow::Result;
use tungstenite::{connect};

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

#[derive(Deserialize, Debug, Default)]
struct ReceivedMessage {
    stream: String,
    data: MessageData,
}

#[tokio::main]
async fn main() {
    let streams = consts::get_stream_names();
    let handles = streams.into_iter().map(|stream_name| thread::spawn(move || process_stream(&stream_name)))
        .collect::<Vec<_>>();
    let _ = handles.into_iter().map(|handle| handle.join().expect("Failed to join thread")).collect::<Vec<_>>();
}

fn process_stream(stream_name: &str) {
    let url = format!("wss://fstream.binance.com/stream?streams={}", stream_name);
    let mut current_data = ReceivedMessage::default();
    loop {
        let mut dump_counter = 0;
        let (mut ws_stream, _) = connect(&url).expect("Failed to connect");
        loop {
            let Ok(message) = ws_stream.read() else {
                println!("Error reading from stream");
                break;
            };
            let start = Instant::now();

            let data_ = message.into_data();
            match serde_json::from_slice::<ReceivedMessage>(&data_) {
                Ok(parsed) => current_data = parsed,
                Err(err) => println!("Error processing message: {:?}", err)
            }            
            let duration = start.elapsed();
            println!("Time elapsed in process_message() is: {:?}", duration);
            dump_counter += 1;
            if dump_counter >= 100 {
                println!("{stream_name} data {:#?}", current_data);
                dump_counter = 0;
            }
        }
        println!("Stream is finished");
    }
}
