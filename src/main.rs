extern crate serde_json;
use serde_json::json;

use std::fs;
use std::convert::TryInto;

use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Map, Value};
use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CallStats {
    rx: u32,
    tx: u32,
    duration: u64,
    time_to_first_byte: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct TraceData {
    host: String,
    time: DateTime<Utc>,
    client: String,
    call_stats: CallStats,
    api: String,
    path: String,
    query: String,
    status_code: u32,
    status_msg: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct State {
    time: String, /* nanoseconds since epoch */
    entity: String, /* hostname (e.g. minio0:9000) */
    state: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct StateMetadata {
    value: u32,
    color: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct StateHeader {
    start: Vec<u64>,
    title: String, /* optional */
    host: String, /* optional */
    //entity_kind: String,
    states: Map<String, Value>,
}

fn main() -> std::io::Result<()> {

    let raw_data = fs::read_to_string("trace_minio")?;

    /* find start time when we unwrap the x value */
    let t: Vec<TraceData> = Deserializer::from_str(&raw_data).into_iter::<TraceData>().map(|x| x.unwrap()).collect::<Vec<TraceData>>();

    let mut s: String = String::from("");
    let begin: DateTime<Utc> = Utc::now();
    let begin_ns: u64 = begin.timestamp_subsec_nanos().into();
    let mut start: u64 = (begin.timestamp() * 1_000_000_000)
        .try_into()
        .expect("failed to make unix timestamp into ns timestamp");
    start += begin_ns;

    let mut metamap: Map<String, Value> = Map::new();
    let mut num_states = 0;
    /*
     * First find the first event. All of the statemap states times are offsets
     * from the first event.
     */
    for value in &t {
        let parsed_data = value;

        let end_ns: u64 = parsed_data.time.timestamp_subsec_nanos().into();
        let unix_end_time: u64 = (parsed_data.time.timestamp() * 1_000_000_000)
            .try_into()
            .expect("failed to make unix timestamp into ns timestamp");

        let end_time_ns = unix_end_time + end_ns;
        let begin_time_ns = end_time_ns - parsed_data.call_stats.duration;

        if begin_time_ns < start {
            start = begin_time_ns;
        }

        if !metamap.contains_key(&parsed_data.api.clone()) {
            metamap.insert(String::from(parsed_data.api.clone()),
                json!({ "value": num_states }));
            num_states += 1;
        }
    }
    let waiting_state: u64 = num_states;
    metamap.insert(String::from("waiting"), json!({ "value": &waiting_state, "color": "#FFFFFF" }));

    /*
     * Create all of the states now that we have the beginning timestamp.
     */
    for value in t {

        let parsed_data = value;

        let end_ns: u64 = parsed_data.time.timestamp_subsec_nanos().into();
        let unix_end_time: u64 = (parsed_data.time.timestamp() * 1_000_000_000)
            .try_into()
            .expect("failed to make unix timestamp into ns timestamp");

        let end_time_ns = unix_end_time + end_ns;
        let begin_time_ns = end_time_ns - parsed_data.call_stats.duration;

        let offset = begin_time_ns - start;
        let statenum = metamap.get(&parsed_data.api).unwrap();
        let statenum = statenum["value"].as_u64().unwrap();

        let state = State {
            time: offset.to_string(),
            entity: parsed_data.host.clone(),
            state: statenum,
        };

        s = format!("{}{}\n", s, serde_json::to_string(&state)?);

        let offset = end_time_ns - start;
        let state = State {
            time: offset.to_string(),
            entity: parsed_data.host,
            state: waiting_state,
        };

        s = format!("{}{}\n", s, serde_json::to_string(&state)?);
    }

    let header = StateHeader {
        start: vec![start / 1_000_000_000, start % 1_000_000_000],
        title: String::from("minio trace"),
        host: String::from("myhost"),
        //entity_kind: String::from("Host"),
        states: metamap,
    };

    println!("{}", serde_json::to_string(&header)?);
    println!("{}", s);

    Ok(())
}
