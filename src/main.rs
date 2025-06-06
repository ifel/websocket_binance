use serde::Deserialize;
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
            Ok(result)
        }
    }
    deserializer.deserialize_seq(StringTupleVecVisitor(PhantomData))
}

#[derive(Deserialize, Debug)]
struct MessageData {
    e: String,
    E: u64,
    T: u64,
    s: String,
    U: u64,
    u: u64,
    pu: u64,
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
    let map: Arc<RwLock<HashMap<String, MessageData>>> = Arc::new(RwLock::new(HashMap::new()));
    let url = "wss://fstream.binance.com/stream?streams=1000000mogusdt@depth20@100ms/1000bonkusdt@depth20@100ms/1000catusdt@depth20@100ms/1000flokiusdt@depth20@100ms/1000luncusdt@depth20@100ms/1000pepeusdt@depth20@100ms/1000ratsusdt@depth20@100ms/1000xecusdt@depth20@100ms/1000xusdt@depth20@100ms/1inchusdt@depth20@100ms/aaveusdt@depth20@100ms/aceusdt@depth20@100ms/achusdt@depth20@100ms/actusdt@depth20@100ms/acxusdt@depth20@100ms/adausdt@depth20@100ms/aergousdt@depth20@100ms/aerousdt@depth20@100ms/aevousdt@depth20@100ms/agldusdt@depth20@100ms/agtusdt@depth20@100ms/ai16zusdt@depth20@100ms/aiusdt@depth20@100ms/aixbtusdt@depth20@100ms/aktusdt@depth20@100ms/alchusdt@depth20@100ms/algousdt@depth20@100ms/aliceusdt@depth20@100ms/alphausdt@depth20@100ms/altusdt@depth20@100ms/animeusdt@depth20@100ms/ankrusdt@depth20@100ms/apeusdt@depth20@100ms/api3usdt@depth20@100ms/aptusdt@depth20@100ms/arbusdt@depth20@100ms/arcusdt@depth20@100ms/arkmusdt@depth20@100ms/arkusdt@depth20@100ms/arpausdt@depth20@100ms/arusdt@depth20@100ms/astrusdt@depth20@100ms/atausdt@depth20@100ms/athusdt@depth20@100ms/atomusdt@depth20@100ms/auctionusdt@depth20@100ms/ausdt@depth20@100ms/avaaiusdt@depth20@100ms/avausdt@depth20@100ms/avaxusdt@depth20@100ms/aweusdt@depth20@100ms/axlusdt@depth20@100ms/axsusdt@depth20@100ms/b3usdt@depth20@100ms/babyusdt@depth20@100ms/bakeusdt@depth20@100ms/bananas31usdt@depth20@100ms/bananausdt@depth20@100ms/bandusdt@depth20@100ms/bankusdt@depth20@100ms/banusdt@depth20@100ms/batusdt@depth20@100ms/bbusdt@depth20@100ms/bchusdt@depth20@100ms/bdxnusdt@depth20@100ms/belusdt@depth20@100ms/berausdt@depth20@100ms/bicousdt@depth20@100ms/bigtimeusdt@depth20@100ms/biousdt@depth20@100ms/blurusdt@depth20@100ms/bmtusdt@depth20@100ms/bnbusdt@depth20@100ms/bntusdt@depth20@100ms/bomeusdt@depth20@100ms/brettusdt@depth20@100ms/brusdt@depth20@100ms/bsvusdt@depth20@100ms/bswusdt@depth20@100ms/btcusdt@depth20@100ms/busdt@depth20@100ms/c98usdt@depth20@100ms/cakeusdt@depth20@100ms/catiusdt@depth20@100ms/celousdt@depth20@100ms/celrusdt@depth20@100ms/cetususdt@depth20@100ms/cfxusdt@depth20@100ms/cgptusdt@depth20@100ms/chessusdt@depth20@100ms/chillguyusdt@depth20@100ms/chrusdt@depth20@100ms/chzusdt@depth20@100ms/ckbusdt@depth20@100ms/compusdt@depth20@100ms/cookieusdt@depth20@100ms/cosusdt@depth20@100ms/cotiusdt@depth20@100ms/cowusdt@depth20@100ms/crvusdt@depth20@100ms/ctkusdt@depth20@100ms/ctsiusdt@depth20@100ms/cvcusdt@depth20@100ms/cyberusdt@depth20@100ms/dashusdt@depth20@100ms/deepusdt@depth20@100ms/degenusdt@depth20@100ms/dentusdt@depth20@100ms/dexeusdt@depth20@100ms/dogeusdt@depth20@100ms/dogsusdt@depth20@100ms/doodusdt@depth20@100ms/dotusdt@depth20@100ms/driftusdt@depth20@100ms/duskusdt@depth20@100ms/dydxusdt@depth20@100ms/dymusdt@depth20@100ms/eduusdt@depth20@100ms/egldusdt@depth20@100ms/eigenusdt@depth20@100ms/enausdt@depth20@100ms/enjusdt@depth20@100ms/ensusdt@depth20@100ms/epicusdt@depth20@100ms/eptusdt@depth20@100ms/etcusdt@depth20@100ms/ethfiusdt@depth20@100ms/ethusdt@depth20@100ms/ethwusdt@depth20@100ms/fartcoinusdt@depth20@100ms/fheusdt@depth20@100ms/fidausdt@depth20@100ms/filusdt@depth20@100ms/fiousdt@depth20@100ms/flmusdt@depth20@100ms/flowusdt@depth20@100ms/fluxusdt@depth20@100ms/formusdt@depth20@100ms/forthusdt@depth20@100ms/fxsusdt@depth20@100ms/galausdt@depth20@100ms/gasusdt@depth20@100ms/glmusdt@depth20@100ms/gmtusdt@depth20@100ms/gmxusdt@depth20@100ms/goatusdt@depth20@100ms/gpsusdt@depth20@100ms/grassusdt@depth20@100ms/griffainusdt@depth20@100ms/grtusdt@depth20@100ms/gtcusdt@depth20@100ms/gunusdt@depth20@100ms/gusdt@depth20@100ms/haedalusdt@depth20@100ms/hbarusdt@depth20@100ms/heiusdt@depth20@100ms/hftusdt@depth20@100ms/hifiusdt@depth20@100ms/highusdt@depth20@100ms/hippousdt@depth20@100ms/hiveusdt@depth20@100ms/hmstrusdt@depth20@100ms/hookusdt@depth20@100ms/hotusdt@depth20@100ms/humausdt@depth20@100ms/hyperusdt@depth20@100ms/hypeusdt@depth20@100ms/icpusdt@depth20@100ms/icxusdt@depth20@100ms/idusdt@depth20@100ms/ilvusdt@depth20@100ms/imxusdt@depth20@100ms/initusdt@depth20@100ms/injusdt@depth20@100ms/iostusdt@depth20@100ms/iotausdt@depth20@100ms/iotxusdt@depth20@100ms/iousdt@depth20@100ms/ipusdt@depth20@100ms/jasmyusdt@depth20@100ms/jellyjellyusdt@depth20@100ms/joeusdt@depth20@100ms/jstusdt@depth20@100ms/jtousdt@depth20@100ms/jupusdt@depth20@100ms/kaiausdt@depth20@100ms/kaitousdt@depth20@100ms/kasusdt@depth20@100ms/kavausdt@depth20@100ms/kdausdt@depth20@100ms/kernelusdt@depth20@100ms/kmnousdt@depth20@100ms/kncusdt@depth20@100ms/komausdt@depth20@100ms/ksmusdt@depth20@100ms/lausdt@depth20@100ms/ldousdt@depth20@100ms/leverusdt@depth20@100ms/linkusdt@depth20@100ms/listausdt@depth20@100ms/lptusdt@depth20@100ms/lqtyusdt@depth20@100ms/lrcusdt@depth20@100ms/lskusdt@depth20@100ms/ltcusdt@depth20@100ms/lumiausdt@depth20@100ms/luna2usdt@depth20@100ms/magicusdt@depth20@100ms/manausdt@depth20@100ms/mantausdt@depth20@100ms/maskusdt@depth20@100ms/maviausdt@depth20@100ms/mavusdt@depth20@100ms/mboxusdt@depth20@100ms/melaniausdt@depth20@100ms/memeusdt@depth20@100ms/merlusdt@depth20@100ms/metisusdt@depth20@100ms/meusdt@depth20@100ms/mewusdt@depth20@100ms/milkusdt@depth20@100ms/minausdt@depth20@100ms/mkrusdt@depth20@100ms/mlnusdt@depth20@100ms/mocausdt@depth20@100ms/moodengusdt@depth20@100ms/morphousdt@depth20@100ms/moveusdt@depth20@100ms/movrusdt@depth20@100ms/mtlusdt@depth20@100ms/mubarakusdt@depth20@100ms/myrousdt@depth20@100ms/nearusdt@depth20@100ms/neiroethusdt@depth20@100ms/neousdt@depth20@100ms/nfpusdt@depth20@100ms/nilusdt@depth20@100ms/nknusdt@depth20@100ms/nmrusdt@depth20@100ms/notusdt@depth20@100ms/ntrnusdt@depth20@100ms/nxpcusdt@depth20@100ms/obolusdt@depth20@100ms/ognusdt@depth20@100ms/ogusdt@depth20@100ms/omniusdt@depth20@100ms/omusdt@depth20@100ms/ondousdt@depth20@100ms/oneusdt@depth20@100ms/ongusdt@depth20@100ms";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    ws_stream.for_each(|message| {
        let map = map.clone();
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
            if let Ok(mp) = map.read() {
                println!("{:?}", mp.len());
            }
        }}).await;
}

async fn process_message(message: Message, map: &Arc<RwLock<HashMap<String, MessageData>>>) -> Result<(), Box<dyn Error + Send + Sync>>{
    // let data2_ = message.clone().into_text()?;
    // println!("{:?}", data2_);
    let data_ = message.into_data();
    let msg: ReceivedMessage = serde_json::from_slice(&data_)?;
    // println!("Received message: {:#?}", msg);
    if let Ok(mut mp) = map.write() {
        mp.insert(msg.stream, msg.data);
    };
    Ok(())
}