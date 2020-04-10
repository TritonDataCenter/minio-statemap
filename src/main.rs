extern crate serde_json;
extern crate getopts;

use std::env;
use std::fs;
use std::convert::TryInto;
use std::collections::HashMap;

use getopts::Options;

use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::{Deserializer, Map, Value};

use chrono::{DateTime, Utc, NaiveDateTime};

/*
 * TraceData represents the default non-verbose MinIO trace format. If the
 * MinIO trace format changes in the future this will also need to be updated.
 */
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
#[serde(rename_all = "camelCase")]
struct CallStats {
    rx: u32,
    tx: u32,
    duration: u64,
    time_to_first_byte: u32,
}

/*
 * Statemap structures. Ideally we would have a library binding for this type
 * of thing in rust (and any other language of interest) so we could do stuff
 * like statemap::create_state(time, entity, state) and all the boilerplate
 * would be handled for us.
 *
 * Or at the very least we could create a common repository/library that holds
 * the definitions of these structures so they can be shared between
 * minio-statemap, the statemap tool itself, and any other tools that use the
 * statemap format.
 *
 * Since this is just a prototype tool we just copy the structures here.
 */
#[derive(Serialize, Deserialize, Debug)]
struct State {
    time: String, /* nanoseconds since epoch */
    entity: String, /* hostname (e.g. minio0:9000) */
    state: u64,
}

/*
 * Not currently used.
 */
#[derive(Serialize, Deserialize, Debug)]
struct StateMetadata {
    value: usize,
    color: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct StateHeader<'a> {
    start: Vec<u64>,
    title: &'a str, /* optional */
    host: &'a str, /* optional */
    //entity_kind: String,
    states: Map<String, Value>,
}

/*
 * Parse the MinIO trace data file and print statemap-formatted records to
 * stdout.
 */
fn print_states(filename: &str, title: &str, cluster: &str)
    -> std::io::Result<()> {

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

    let parse_data = |mut acc: HashMap<String, Vec<TraceData>>, x: Result<TraceData, serde_json::Error>| {
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

        let statevec = acc
            .entry(td.host.clone())
            .or_insert_with(|| { let v: Vec<TraceData> = Vec::new(); v });

        statevec.push(td);
        acc
    };

    /*
     * Convert the file to a map of hosts -> states.
     *
     * We do this because we need to perform some validation on per-host states.
     * For example, we need to make sure that the hosts weren't performing more
     * than one operation at a time because that would make the statemap
     * misleading.
     */
    let host_states: HashMap<String, Vec<TraceData>> = HashMap::new();
    let host_states = Deserializer::from_str(&raw_data)
        .into_iter::<TraceData>()
        .fold(host_states, parse_data);

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
        title: title,
        host: cluster,
        //entity_kind: String::from("Host"),
        states: metamap,
    };
    println!("{}", serde_json::to_string(&header)?);

    /*
     * Create all of the states now that we know the beginning timestamp and
     * the necessary metadata.
     */
    let mut nerrors = 0;
    let mut nstates = 0;
    for (host, statevec) in host_states.iter() {
        eprintln!("Generating states for host {}", host);

        let mut prev_api = "".to_string();
        let mut prev_ts =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);

        for state in statevec {
            /*
             * Re-compute the start time based on the beginning timestamp.
             */
            let end_ns: u64 = state.time.timestamp_subsec_nanos().into();
            let unix_end_time: u64 = (state.time.timestamp() * 1_000_000_000)
                .try_into()
                .expect("failed to make unix timestamp into ns timestamp");

            let end_time_ns = unix_end_time + end_ns;
            let begin_time_ns = end_time_ns - state.call_stats.duration;

            let offset = begin_time_ns - start.unwrap();
            let statenum = header.states.get(&state.api).unwrap();
            let statenum = statenum["value"].as_u64().unwrap();

            /*
             * Detect out-of-order states.
             *
             * The only reason I know of where this would happen is if a minio
             * instance is processing more than one request at a given time.
             * Although this is reasonable and expected for a minio that's under
             * load this tool does not support this.
             *
             * A statemap generated from this output would be misleading since
             * the statemap only currently shows one state per host per point in
             * time.
             *
             * We perform this validation here because if we don't the statemap
             * tool will barf when it detects this same condition.
             */
            if prev_ts > state.time {
                eprintln!(" \
                    {}: {} at {} and\n \
                    {}: {} at {} are out of order\n",
                    host, &prev_api, &prev_ts,
                    host, &state.api, state.time);

                nerrors += 1;
            }

            prev_api = state.api.clone();
            prev_ts = state.time;

            let sm_state = State {
                time: offset.to_string(),
                entity: state.host.clone(),
                state: statenum,
            };

            println!("{}", serde_json::to_string(&sm_state)?);

            let offset = end_time_ns - start.unwrap();
            let wait_state = State {
                time: offset.to_string(),
                entity: host.to_string(),
                state: waiting_state,
            };

            println!("{}", serde_json::to_string(&wait_state)?);

            /* One 'real' state, one waiting state. */
            nstates += 2;
        }
    }

    if nerrors > 0 {
        eprintln!("\n{} out-of-order timestamps discovered.\n\
            Try again with a serial MinIO workload.", nerrors);
    } else {
        eprintln!("\n{} states discovered.", nstates);
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

    opts.optopt("c",
                "cluster-name",
                "name of the cluster for display in the rendered statemap",
                "NAME");
    opts.optopt("t",
                "title",
                "statemap title",
                "TITLE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            usage(opts, &f.to_string());
            return Ok(())
        },
    };

    let ifile = matches.opt_str("input-file").unwrap();
    let cluster = matches.opt_get_default(
        "cluster-name", "minio cluster".to_string()).unwrap();
    let title = matches.opt_get_default(
        "title", "MinIO".to_string()).unwrap();

    print_states(&ifile, &cluster, &title)
}
