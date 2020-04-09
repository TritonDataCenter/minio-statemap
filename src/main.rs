extern crate serde_json;
extern crate getopts;

use std::env;
use std::fs;
use std::convert::TryInto;

use getopts::Options;

use serde::{Deserialize, Serialize};
use serde_json::json;
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

/*
 * Parse the MinIO trace data file and print statemap-formatted records to
 * stdout.
 */
fn print_states(filename: &str) -> std::io::Result<()> {
    let raw_data = fs::read_to_string(filename)?;

    let mut start: Option<u64> = None;

    let mut metamap: Map<String, Value> = Map::new();
    let mut num_states = 0;

    /*
     * parse_data receives a callback for every json block that minio reported
     * and does two things:
     *   1) Finds the timestamp of the earliest reported event. Statemaps
     *      require each state timestamp is an offset from the first event.
     *   2) Finds some state metadata. In MinIO terms this is
     *      a list of state names (like 'PutObject' or 'ListDir') assigned a
     *      unique number.
     *
     * You may think "why not just look at the first entry in the minio trace
     * output to find the beginning timestamp instead of interating over the
     * entire data set?" - A good question!
     *
     * MinIO's trace data is sorted by _end_ time of operation, not _start_
     * time. Further, MinIO doesn't report the start time of each operation.
     * MinIO only reports the end time of each operation and the duration of
     * the operation, so we must infer the start time based on this information.
     */
    let parse_data= |x: Result<TraceData, serde_json::Error>| {
        /* Just panic if we see invalid json */
        let td = x.expect("unexpected json format");

        let end_ns: u64 = td.time.timestamp_subsec_nanos().into();
        let unix_end_time: u64 = (td.time.timestamp() * 1_000_000_000)
            .try_into()
            .expect("failed to make unix timestamp into ns timestamp");

        let end_time_ns = unix_end_time + end_ns;
        let begin_time_ns = end_time_ns - td.call_stats.duration;

        if start.is_none() || begin_time_ns < start.unwrap() {
            start = Some(begin_time_ns);
        }

        if !metamap.contains_key(&td.api.clone()) {
            metamap.insert(String::from(td.api.clone()),
                json!({ "value": num_states }));
            num_states += 1;
        }

        /* Return the TraceData so it can be collected in a Vec. */
        td
    };

    /*
     * Convert the file to a vec while finding some key metadata.
     */
    let t: Vec<TraceData> = Deserializer::from_str(&raw_data)
        .into_iter::<TraceData>()
        .map(parse_data)
        .collect::<Vec<TraceData>>();

    /* 
     * When MinIO doesn't say what it's doing we assume it's not doing anything
     * useful, so we assign the 'waiting' state.
     */
    let waiting_state: u64 = num_states;
    metamap.insert(String::from("waiting"),
        json!({ "value": &waiting_state, "color": "#FFFFFF" }));

    let header = StateHeader {
        start: vec![
            start.unwrap() / 1_000_000_000,
            start.unwrap() % 1_000_000_000
        ],
        title: String::from("minio trace"),
        host: String::from("myhost"),
        //entity_kind: String::from("Host"),
        states: metamap,
    };
    println!("{}", serde_json::to_string(&header)?);

    /*
     * Create all of the states now that we know the beginning timestamp and
     * the necessary metadata.
     */
    for value in t {

        let parsed_data = value;

        let end_ns: u64 = parsed_data.time.timestamp_subsec_nanos().into();
        let unix_end_time: u64 = (parsed_data.time.timestamp() * 1_000_000_000)
            .try_into()
            .expect("failed to make unix timestamp into ns timestamp");

        let end_time_ns = unix_end_time + end_ns;
        let begin_time_ns = end_time_ns - parsed_data.call_stats.duration;

        let offset = begin_time_ns - start.unwrap();
        let statenum = header.states.get(&parsed_data.api).unwrap();
        let statenum = statenum["value"].as_u64().unwrap();

        let state = State {
            time: offset.to_string(),
            entity: parsed_data.host.clone(),
            state: statenum,
        };

        println!("{}", serde_json::to_string(&state)?);

        let offset = end_time_ns - start.unwrap();
        let state = State {
            time: offset.to_string(),
            entity: parsed_data.host,
            state: waiting_state,
        };

        println!("{}", serde_json::to_string(&state)?);
    }

    Ok(())

}

fn usage(opts: Options, msg: &str) {
    let synopsis = "\
        Convert MinIO JSON trace output to statemap input";

    let usg = format!("minio-statemap - {}", synopsis);
    let ex_usg = format!("Example usage:\n \
        ./minio-statemap -i ./my_minio_trace.out | statemap > statemap.svg\n");
    println!("{}", opts.usage(&usg));
    println!("{}", ex_usg);
    println!("{}", msg);
}

fn main() -> std::io::Result<()> {

    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.reqopt("i",
                "input-file",
                "path to minio trace file to be parsed",
                "FILE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            usage(opts, &f.to_string());
            return Ok(())
        },
    };

    let ifile = matches.opt_str("input-file").unwrap();

    print_states(&ifile)
}
