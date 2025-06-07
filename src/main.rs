use serde::Deserialize;

use tokio;
use std::time::Duration;
use std::{thread::sleep, time::Instant};
use std::fmt;
use std::marker::PhantomData;
use std::thread;
use serde::de;
use anyhow::Result;
use tungstenite::{connect};
use floating_duration::TimeAsFloat;

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

const PROCESS_MESSAGES: usize = 1000;

fn process_stream(stream_name: &str) {
    let url = format!("wss://fstream.binance.com/stream?streams={}", stream_name);
    let mut current_data = ReceivedMessage::default();
    let mut message_counter = 0;
    let mut stop_execution = false;
    let mut processing_times: [f64; PROCESS_MESSAGES] = [0.0; PROCESS_MESSAGES];
    let mut latencies: [f64; PROCESS_MESSAGES] = [0.0; PROCESS_MESSAGES];
    loop {
        let connect_result = connect(&url);
        if let Err(err) = connect_result {
            println!("Failed to connect to stream for {url}: {err}");
            sleep(Duration::from_millis(1000));
            continue;
        }
        let (mut ws_stream, _) = connect_result.unwrap();
        let mut last_message_received_timestamp = Instant::now();
        loop {
            let Ok(message) = ws_stream.read() else {
                println!("Error reading from stream");
                break;
            };
            let start = Instant::now();

            if message.is_ping() {
                continue;
            }

            let data_ = message.clone().into_data();
            match serde_json::from_slice::<ReceivedMessage>(&data_) {
                Ok(parsed) => current_data = parsed,
                Err(err) => {println!("Error processing message: {:?}, message: {:#?}", err, message); continue;}
            }
            let duration = start.elapsed();
            // println!("Time elapsed in process_message() is: {:?}", duration);
            processing_times[message_counter] = duration.as_fractional_micros();
            latencies[message_counter] = last_message_received_timestamp.elapsed().as_fractional_millis();
            last_message_received_timestamp = start;
            message_counter += 1;
            if message_counter == PROCESS_MESSAGES {
                stop_execution = true;
                let processing_times_pct: inc_stats::Percentiles<f64> = processing_times.iter().collect();
                let latencies_pct: inc_stats::Percentiles<f64> = latencies.iter().collect();
                
                let processing_times_p99 = processing_times_pct.percentile(0.99).unwrap().unwrap();
                let processing_times_p95 = processing_times_pct.percentile(0.95).unwrap().unwrap();
                let processing_times_p90 = processing_times_pct.percentile(0.90).unwrap().unwrap();
                let processing_times_p75 = processing_times_pct.percentile(0.75).unwrap().unwrap();
                let processing_times_med = processing_times_pct.median().unwrap();

                let latencies_p99 = latencies_pct.percentile(0.99).unwrap().unwrap();
                let latencies_p95 = latencies_pct.percentile(0.95).unwrap().unwrap();
                let latencies_p90 = latencies_pct.percentile(0.90).unwrap().unwrap();
                let latencies_p75 = latencies_pct.percentile(0.75).unwrap().unwrap();
                let latencies_med = latencies_pct.median().unwrap();
                println!(r##"
{stream_name}
Processing Time - P99: {:.3?}µs, P95: {:.3?}µs, P90: {:.3?}µs, P75: {:.3?}µs, Median: {:.3?}µs
Time between requests - P99: {:.3?}ms, P95: {:.3?}ms, P90: {:.3?}ms, P75: {:.3?}ms, Median: {:.3?}ms 
"##, processing_times_p99, processing_times_p95, processing_times_p90, processing_times_p75, processing_times_med,
latencies_p99, latencies_p95, latencies_p90, latencies_p75, latencies_med);
                break;
            }
        }
        if stop_execution {
            break;
        }
        println!("Stream is finished");
    }
}
